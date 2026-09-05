package customer.internal_monitoring.handlers;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sap.cds.services.ErrorStatuses;
import com.sap.cds.services.ServiceException;
import com.sap.cds.services.cds.CqnService;
import com.sap.cds.services.handler.EventHandler;
import com.sap.cds.services.handler.annotations.On;
import com.sap.cds.services.handler.annotations.ServiceName;

import cds.gen.monitoringservice.ClearCredentialsContext;
import cds.gen.monitoringservice.ImportCredentialsFromCurlContext;
import cds.gen.monitoringservice.IngestionResult;
import cds.gen.monitoringservice.IngestionStatus;
import cds.gen.monitoringservice.IngestionStatus_;
import cds.gen.monitoringservice.MonitoringService_;
import cds.gen.monitoringservice.PurgeExpiredMessagesContext;
import cds.gen.monitoringservice.SetCredentialsContext;
import cds.gen.monitoringservice.SetTopicContext;
import cds.gen.monitoringservice.TriggerIngestionContext;
import customer.internal_monitoring.client.ApiCredentialStore;
import customer.internal_monitoring.client.CurlCommandParser;
import customer.internal_monitoring.client.KafkaApiException;
import customer.internal_monitoring.config.KafkaMonitoringProperties;
import customer.internal_monitoring.ingest.IngestionState;
import customer.internal_monitoring.ingest.MessageIngestionService;

/**
 * Administrative actions and the live ingestion status of the monitoring
 * service.
 */
@Component
@ServiceName(MonitoringService_.CDS_NAME)
public class MonitoringServiceHandler implements EventHandler {

	private static final Logger log = LoggerFactory.getLogger(MonitoringServiceHandler.class);

	/** Fixed key of the singleton status record. */
	private static final String STATUS_ID = "current";

	private final ApiCredentialStore credentials;
	private final MessageIngestionService ingestionService;
	private final KafkaMonitoringProperties properties;
	private final IngestionState state;

	public MonitoringServiceHandler(
			ApiCredentialStore credentials,
			MessageIngestionService ingestionService,
			KafkaMonitoringProperties properties,
			IngestionState state) {
		this.credentials = credentials;
		this.ingestionService = ingestionService;
		this.properties = properties;
		this.state = state;
	}

	@On(event = SetCredentialsContext.CDS_NAME)
	public void setCredentials(SetCredentialsContext context) {
		try {
			credentials.update(context.getCookie(), context.getXsrfToken());
		} catch (IllegalArgumentException e) {
			throw new ServiceException(ErrorStatuses.BAD_REQUEST, e.getMessage());
		}
		state.clearAuthenticationFailure();
		log.info("API credentials replaced");
		context.setResult("Credentials stored in memory." + cookieHint(context.getCookie())
				+ " Polling resumes with the next cycle.");
		context.setCompleted();
	}

	@On(event = ImportCredentialsFromCurlContext.CDS_NAME)
	public void importCredentialsFromCurl(ImportCredentialsFromCurlContext context) {
		CurlCommandParser.ParsedCurl parsed;
		try {
			parsed = CurlCommandParser.parse(context.getCurlCommand());
		} catch (IllegalArgumentException e) {
			throw new ServiceException(ErrorStatuses.BAD_REQUEST, "Could not parse the cURL command: " + e.getMessage());
		}
		if (parsed.cookie() == null) {
			throw new ServiceException(ErrorStatuses.BAD_REQUEST,
					"The cURL command does not contain a 'Cookie' header.");
		}
		credentials.update(parsed.cookie(), parsed.xsrfToken());
		state.clearAuthenticationFailure();
		log.info("API credentials replaced from a cURL command");

		String urlHint = parsed.url() != null && !parsed.url().equals(properties.getEndpoint())
				? " Note: the URL in the command differs from 'monitoring.kafka.endpoint'."
				: "";
		context.setResult("Credentials stored in memory." + cookieHint(parsed.cookie()) + urlHint);
		context.setCompleted();
	}

	/**
	 * Warns about cookies the endpoint needs but the supplied header lacks, so the
	 * operator learns about it here instead of from intermittent 401 responses.
	 */
	private static String cookieHint(String cookie) {
		List<String> missing = ApiCredentialStore.missingExpectedCookies(cookie);
		return missing.isEmpty() ? ""
				: " Warning: the 'Cookie' header is missing " + String.join(" and ", missing)
						+ ", which the endpoint needs; requests are likely to fail with 401.";
	}

	@On(event = ClearCredentialsContext.CDS_NAME)
	public void clearCredentials(ClearCredentialsContext context) {
		credentials.clear();
		log.info("API credentials cleared");
		context.setResult("Credentials cleared. Polling pauses until new credentials are supplied.");
		context.setCompleted();
	}

	@On(event = TriggerIngestionContext.CDS_NAME)
	public void triggerIngestion(TriggerIngestionContext context) {
		IngestionState.IngestionOutcome outcome;
		try {
			outcome = ingestionService.ingest();
		} catch (IllegalStateException e) {
			throw new ServiceException(ErrorStatuses.CONFLICT, e.getMessage());
		} catch (KafkaApiException e) {
			throw new ServiceException(
					e.isAuthenticationFailure() ? ErrorStatuses.UNAUTHORIZED : ErrorStatuses.BAD_GATEWAY,
					e.getMessage());
		}

		IngestionResult result = IngestionResult.create();
		result.setFetched(outcome.fetched());
		result.setStored(outcome.stored());
		result.setDuplicates(outcome.duplicates());
		result.setFailed(outcome.failed());
		result.setDurationMillis(outcome.durationMillis());
		context.setResult(result);
		context.setCompleted();
	}

	@On(event = PurgeExpiredMessagesContext.CDS_NAME)
	public void purgeExpiredMessages(PurgeExpiredMessagesContext context) {
		context.setResult(Math.toIntExact(ingestionService.purgeExpired()));
		context.setCompleted();
	}

	@On(event = SetTopicContext.CDS_NAME)
	public void setTopic(SetTopicContext context) {
		String requested = context.getTopic();
		if (!properties.getSelectableTopics().contains(requested)) {
			throw new ServiceException(ErrorStatuses.BAD_REQUEST,
					"Unknown topic '" + requested + "'. Choose one of: "
							+ String.join(", ", properties.getSelectableTopics()));
		}

		String previous = properties.getTopic();
		if (!requested.equals(previous)) {
			try {
				properties.switchTopic(requested);
			} catch (IllegalArgumentException e) {
				throw new ServiceException(ErrorStatuses.BAD_REQUEST, e.getMessage());
			}
			long purged = ingestionService.purgeTopic(previous);
			state.clearAuthenticationFailure();
			log.info("Switched topic {} -> {}; purged {} messages of the previous topic",
					previous, requested, purged);
		}

		context.setResult("Now monitoring " + requested + ".");
		context.setCompleted();
	}

	/**
	 * Serves the non-persisted {@code IngestionStatus} entity from the running
	 * poller.
	 */
	@On(event = CqnService.EVENT_READ, entity = IngestionStatus_.CDS_NAME)
	public List<IngestionStatus> readIngestionStatus() {
		IngestionStatus status = IngestionStatus.create();
		status.setId(STATUS_ID);
		status.setEnabled(properties.isEnabled());
		status.setCredentialsPresent(credentials.isPresent());
		status.setCredentialsExpired(credentials.isPresent() && state.isCredentialsRejected());
		status.setRunning(ingestionService.isRunning());
		status.setPollIntervalSeconds(Math.toIntExact(properties.getPollInterval().toSeconds()));
		status.setRetention(properties.getRetention().toString());
		status.setEndpoint(properties.getEndpoint());
		status.setActiveTopic(properties.getTopic());
		status.setSelectableTopics(properties.getSelectableTopics());
		status.setLastRunAt(state.getLastRunAt());
		status.setLastSuccessAt(state.getLastSuccessAt());
		status.setLastError(truncate(state.getLastError(), 1024));
		status.setTotalStoredCount(state.getTotalStored());
		status.setSkippedRunCount(state.getSkippedRuns());
		status.setConsecutiveFailures(state.getConsecutiveFailures());

		IngestionState.IngestionOutcome last = state.getLastOutcome();
		if (last != null) {
			status.setLastFetchedCount(last.fetched());
			status.setLastStoredCount(last.stored());
			status.setLastDuplicateCount(last.duplicates());
			status.setLastFailedCount(last.failed());
			status.setLastDurationMillis(last.durationMillis());
		}
		status.setStoredMessageCount(ingestionService.countStoredMessages());
		return List.of(status);
	}

	private static String truncate(String value, int maxLength) {
		if (value == null) {
			return null;
		}
		return value.length() <= maxLength ? value : value.substring(0, maxLength);
	}
}
