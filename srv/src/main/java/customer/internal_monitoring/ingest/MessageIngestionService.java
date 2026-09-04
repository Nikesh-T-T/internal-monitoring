package customer.internal_monitoring.ingest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.sap.cds.ql.CQL;
import com.sap.cds.ql.Delete;
import com.sap.cds.ql.Insert;
import com.sap.cds.ql.Select;
import com.sap.cds.services.changeset.ChangeSetContext;
import com.sap.cds.services.persistence.PersistenceService;
import com.sap.cds.services.request.RequestContext;
import com.sap.cds.services.runtime.CdsRuntime;

import cds.gen.internal.monitoring.KafkaMessages;
import cds.gen.internal.monitoring.KafkaMessages_;
import customer.internal_monitoring.client.KafkaApiException;
import customer.internal_monitoring.client.KafkaMessagesClient;
import customer.internal_monitoring.config.KafkaMonitoringProperties;
import customer.internal_monitoring.ingest.IngestionState.IngestionOutcome;

/**
 * Retrieves Kafka messages from the API and stores the ones not seen before.
 *
 * <p>Runs are serialised: polling returns overlapping windows of the topic, so
 * two concurrent runs would only compete over the same records. A run that finds
 * another one in progress is skipped rather than queued.
 */
@Service
public class MessageIngestionService {

	private static final Logger log = LoggerFactory.getLogger(MessageIngestionService.class);

	/** Only messages from this service are persisted; the rest are dropped on intake. */
	private static final String PERSISTED_SERVICE_NAME = "x-ai-orchagent-srv";

	private final KafkaMessagesClient client;
	private final KafkaMessageParser parser;
	private final PersistenceService persistence;
	private final CdsRuntime runtime;
	private final KafkaMonitoringProperties properties;
	private final IngestionState state;

	private final ReentrantLock runLock = new ReentrantLock();

	public MessageIngestionService(
			KafkaMessagesClient client,
			KafkaMessageParser parser,
			PersistenceService persistence,
			CdsRuntime runtime,
			KafkaMonitoringProperties properties,
			IngestionState state) {
		this.client = client;
		this.parser = parser;
		this.persistence = persistence;
		this.runtime = runtime;
		this.properties = properties;
		this.state = state;
	}

	/**
	 * Runs one ingestion cycle.
	 *
	 * @throws IllegalStateException if another run is already in progress
	 * @throws KafkaApiException     if the API call fails
	 */
	public IngestionOutcome ingest() {
		return ingestIfIdle().orElseThrow(
				() -> new IllegalStateException("An ingestion run is already in progress"));
	}

	/**
	 * Runs one ingestion cycle unless another one is already in progress.
	 *
	 * @return the outcome, or empty when the run was skipped because of an overlap
	 */
	public Optional<IngestionOutcome> ingestIfIdle() {
		if (!runLock.tryLock()) {
			state.recordSkippedRun();
			log.debug("Skipping ingestion run: the previous run is still in progress");
			return Optional.empty();
		}
		try {
			long startedAt = System.nanoTime();
			Function<RequestContext, IngestionOutcome> run = context -> execute(startedAt);
			IngestionOutcome outcome = runtime.requestContext().systemUser().run(run);
			state.recordSuccess(outcome);
			return Optional.of(outcome);
		} catch (RuntimeException e) {
			if (e instanceof KafkaApiException apiError && apiError.isAuthenticationFailure()) {
				state.recordAuthenticationFailure(describe(e));
			} else {
				state.recordFailure(describe(e));
			}
			throw e;
		} finally {
			runLock.unlock();
		}
	}

	/** True while an ingestion run is in progress. */
	public boolean isRunning() {
		return runLock.isLocked();
	}

	private IngestionOutcome execute(long startedAt) {
		Counters counters = new Counters();
		Set<String> seenInThisRun = new HashSet<>();
		List<ParsedKafkaMessage> buffer = new ArrayList<>(properties.getBatchSize());
		Instant ingestedAt = Instant.now();

		client.fetchMessages(body -> {
			counters.fetched = parser.parse(body, properties.getTopic(), message -> {
				if (message.payload() == null) {
					counters.failed++;
					return;
				}
				if (!PERSISTED_SERVICE_NAME.equals(message.serviceName())) {
					return;
				}
				if (!seenInThisRun.add(message.messageHash())) {
					counters.duplicates++;
					return;
				}
				buffer.add(message);
				if (buffer.size() >= properties.getBatchSize()) {
					flush(buffer, ingestedAt, counters);
				}
			});
			return null;
		});

		flush(buffer, ingestedAt, counters);

		long durationMillis = (System.nanoTime() - startedAt) / 1_000_000L;
		log.info("Ingestion finished: fetched={} stored={} duplicates={} failed={} in {} ms",
				counters.fetched, counters.stored, counters.duplicates, counters.failed, durationMillis);
		return new IngestionOutcome(counters.fetched, counters.stored, counters.duplicates, counters.failed,
				durationMillis);
	}

	/** Writes the buffered messages that are not stored yet and clears the buffer. */
	private void flush(List<ParsedKafkaMessage> buffer, Instant ingestedAt, Counters counters) {
		if (buffer.isEmpty()) {
			return;
		}
		List<ParsedKafkaMessage> batch = List.copyOf(buffer);
		buffer.clear();

		runtime.changeSetContext().run(context -> {
			Set<String> known = findKnownHashes(
					batch.stream().map(ParsedKafkaMessage::messageHash).toList());

			List<KafkaMessages> rows = batch.stream()
					.filter(message -> !known.contains(message.messageHash()))
					.map(message -> toRow(message, ingestedAt))
					.toList();

			counters.duplicates += batch.size() - rows.size();
			if (rows.isEmpty()) {
				return;
			}
			persistence.run(Insert.into(KafkaMessages_.class).entries(rows));
			counters.stored += rows.size();
		});
	}

	private Set<String> findKnownHashes(List<String> hashes) {
		return persistence
				.run(Select.from(KafkaMessages_.class)
						.columns(message -> message.messageHash())
						.where(message -> message.messageHash().in(hashes)))
				.streamOf(KafkaMessages.class)
				.map(KafkaMessages::getMessageHash)
				.collect(Collectors.toSet());
	}

	private KafkaMessages toRow(ParsedKafkaMessage message, Instant ingestedAt) {
		KafkaMessages row = KafkaMessages.create();
		row.setId(UUID.randomUUID().toString());
		row.setServiceName(message.serviceName());
		row.setMessageHash(message.messageHash());
		row.setPayloadHash(message.payloadHash());
		row.setCorrelationId(message.correlationId());
		row.setMessageId(message.messageId());
		row.setSourceId(message.sourceId());
		row.setEventType(message.eventType());
		row.setMessageType(message.messageType());
		row.setTopic(message.topic());
		row.setMessageTimestamp(message.messageTimestamp());
		row.setKafkaPartition(message.kafkaPartition());
		row.setKafkaOffset(message.kafkaOffset());
		row.setPayloadSize(message.payloadSize());
		row.setHeadersSize(message.headersSize());
		row.setTruncated(message.truncated());
		row.setParseStatus(message.parseStatus());
		row.setTenantId(message.tenantId());
		row.setUseCaseName(message.useCaseName());
		row.setServiceType(message.serviceType());
		row.setCalmAction(message.calmAction());
		row.setAgentVersion(message.agentVersion());
		row.setPayload(message.payload());
		row.setProperties(message.properties());
		row.setIngestedAt(ingestedAt);
		return row;
	}

	/**
	 * Deletes messages whose ingestion time is older than the retention period.
	 *
	 * @return the number of deleted rows
	 */
	public long purgeExpired() {
		Instant cutoff = Instant.now().minus(properties.getRetention());
		Function<RequestContext, Long> run = context -> runtime.changeSetContext()
				.run((Function<ChangeSetContext, Long>) changeSet -> (long) persistence
						.run(Delete.from(KafkaMessages_.class)
								.where(message -> message.ingestedAt().lt(cutoff)))
						.rowCount());
		long deleted = runtime.requestContext().systemUser().run(run);
		if (deleted > 0) {
			log.info("Retention removed {} messages ingested before {}", deleted, cutoff);
		}
		return deleted;
	}

	/** Number of messages currently stored. */
	public long countStoredMessages() {
		Function<RequestContext, Long> run = context -> {
			Object count = persistence
					.run(Select.from(KafkaMessages_.class).columns(message -> CQL.count(message.ID()).as("count")))
					.single()
					.get("count");
			return count instanceof Number number ? number.longValue() : 0L;
		};
		return runtime.requestContext().systemUser().run(run);
	}

	private static String describe(Throwable error) {
		if (error instanceof KafkaApiException apiError && apiError.isAuthenticationFailure()) {
			return "Credentials rejected by the API (HTTP " + apiError.getStatusCode()
					+ "). Supply a fresh session with the 'setCredentials' action.";
		}
		String message = error.getMessage();
		return message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
	}

	/** Mutable counters shared with the streaming callback. */
	private static final class Counters {
		private int fetched;
		private int stored;
		private int duplicates;
		private int failed;
	}
}
