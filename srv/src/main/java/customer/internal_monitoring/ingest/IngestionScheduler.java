package customer.internal_monitoring.ingest;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import customer.internal_monitoring.client.ApiCredentialStore;
import customer.internal_monitoring.client.KafkaApiException;
import customer.internal_monitoring.config.KafkaMonitoringProperties;

/**
 * Drives the periodic ingestion and the retention clean-up.
 *
 * <p>Both jobs use {@code fixedDelay}, so the next run starts only after the
 * previous one finished. Failures are logged and recorded in the
 * {@link IngestionState}; they must never terminate the schedule.
 */
@Component
public class IngestionScheduler {

	private static final Logger log = LoggerFactory.getLogger(IngestionScheduler.class);

	/** Log the same authentication problem at most once per this many runs. */
	private static final int AUTH_LOG_INTERVAL = 20;

	private final AtomicLong throttledLogCount = new AtomicLong();

	private final MessageIngestionService ingestionService;
	private final KafkaMonitoringProperties properties;
	private final ApiCredentialStore credentials;
	private final IngestionState state;

	public IngestionScheduler(
			MessageIngestionService ingestionService,
			KafkaMonitoringProperties properties,
			ApiCredentialStore credentials,
			IngestionState state) {
		this.ingestionService = ingestionService;
		this.properties = properties;
		this.credentials = credentials;
		this.state = state;
	}

	@Scheduled(
			fixedDelayString = "${monitoring.kafka.poll-interval:5s}",
			initialDelayString = "${monitoring.kafka.initial-delay:15s}")
	public void poll() {
		if (!properties.isEnabled()) {
			return;
		}
		if (!credentials.isPresent()) {
			String message = "No API credentials configured yet - skipping poll. "
					+ "Supply the browser 'Cookie' header in the dashboard dialog "
					+ "or through the 'setCredentials' action.";
			logThrottled(message);
			state.recordFailure(message);
			return;
		}
		try {
			ingestionService.ingestIfIdle();
			throttledLogCount.set(0);
		} catch (KafkaApiException e) {
			// The failure is recorded by the ingestion service; only report it here.
			if (e.isAuthenticationFailure()) {
				logThrottled("Kafka messages API rejected the credentials (HTTP " + e.getStatusCode()
						+ "). Supply a fresh browser session in the dashboard or via 'setCredentials'.");
			} else {
				log.error("Kafka message ingestion failed: {}", e.getMessage());
			}
		} catch (RuntimeException e) {
			log.error("Kafka message ingestion failed unexpectedly", e);
		}
	}

	@Scheduled(
			fixedDelayString = "${monitoring.kafka.retention-interval:5m}",
			initialDelayString = "${monitoring.kafka.retention-interval:5m}")
	public void purgeExpired() {
		if (!properties.isRetentionEnabled()) {
			return;
		}
		try {
			ingestionService.purgeExpired();
		} catch (RuntimeException e) {
			log.error("Retention run failed", e);
		}
	}

	/**
	 * Keeps a permanently missing or expired session from filling the log at every
	 * poll, while still reporting it regularly. The counter is reset by the first
	 * successful poll.
	 */
	private void logThrottled(String message) {
		if (throttledLogCount.getAndIncrement() % AUTH_LOG_INTERVAL == 0) {
			log.warn("{}", message);
		}
	}
}
