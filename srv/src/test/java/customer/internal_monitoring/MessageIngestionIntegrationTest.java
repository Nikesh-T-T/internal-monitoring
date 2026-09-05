package customer.internal_monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.sap.cds.ql.Delete;
import com.sap.cds.ql.Insert;
import com.sap.cds.ql.Select;
import com.sap.cds.services.persistence.PersistenceService;
import com.sap.cds.services.request.RequestContext;
import com.sap.cds.services.runtime.CdsRuntime;
import com.sun.net.httpserver.HttpServer;

import cds.gen.internal.monitoring.KafkaMessages;
import cds.gen.internal.monitoring.KafkaMessages_;
import customer.internal_monitoring.client.ApiCredentialStore;
import customer.internal_monitoring.config.KafkaMonitoringProperties;
import customer.internal_monitoring.ingest.IngestionState.IngestionOutcome;
import customer.internal_monitoring.ingest.MessageIngestionService;

/**
 * Exercises the full ingestion path against a stubbed API: fetch, parse, store,
 * deduplicate and purge.
 */
@SpringBootTest
class MessageIngestionIntegrationTest {

	private static HttpServer server;
	@Autowired
	private MessageIngestionService ingestionService;

	@Autowired
	private ApiCredentialStore credentials;

	@Autowired
	private PersistenceService persistence;

	@Autowired
	private CdsRuntime runtime;

	@Autowired
	private KafkaMonitoringProperties monitoringProperties;

	@BeforeAll
	static void startStubApi() throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/messages", exchange -> serveFixture(exchange, "/sample-kafka-response.json"));
		server.createContext("/rum-messages", exchange -> serveFixture(exchange, "/sample-kafka-response-rum.json"));
		server.createContext("/rum-truncated-messages",
				exchange -> serveFixture(exchange, "/sample-kafka-response-rum-truncated.json"));
		server.start();
	}

	private static void serveFixture(com.sun.net.httpserver.HttpExchange exchange, String resource)
			throws IOException {
		byte[] body;
		try (InputStream fixture = MessageIngestionIntegrationTest.class.getResourceAsStream(resource)) {
			body = fixture.readAllBytes();
		}
		exchange.getResponseHeaders().add("Content-Type", "application/json");
		exchange.sendResponseHeaders(200, body.length);
		try (var out = exchange.getResponseBody()) {
			out.write(body);
		}
		exchange.close();
	}

	@AfterAll
	static void stopStubApi() {
		server.stop(0);
	}

	@DynamicPropertySource
	static void configure(DynamicPropertyRegistry registry) {
		registry.add("monitoring.kafka.endpoint",
				() -> "http://127.0.0.1:" + server.getAddress().getPort() + "/messages");
		registry.add("monitoring.kafka.topic", () -> "ms.demo.internal-monitoring");
		// The scheduler must not interfere with the assertions below.
		registry.add("monitoring.kafka.enabled", () -> "false");
		registry.add("monitoring.kafka.retention-enabled", () -> "false");
		registry.add("monitoring.kafka.batch-size", () -> "2");
	}

	@BeforeEach
	void resetState() {
		credentials.update("JSESSIONID=test; __VCAP_ID__=inst-1", null);
		Consumer<RequestContext> clear = context -> persistence.run(Delete.from(KafkaMessages_.class));
		runtime.requestContext().systemUser().run(clear);
	}

	private List<KafkaMessages> storedMessages() {
		Function<RequestContext, List<KafkaMessages>> select = context -> persistence
				.run(Select.from(KafkaMessages_.class).orderBy(message -> message.kafkaOffset().asc()))
				.listOf(KafkaMessages.class);
		return runtime.requestContext().systemUser().run(select);
	}

	/** Inserts a bare message row for the given topic so purge behaviour can be asserted. */
	private void insertMessage(String topic) {
		KafkaMessages row = KafkaMessages.create();
		row.setId(UUID.randomUUID().toString());
		row.setTopic(topic);
		row.setServiceName("x-ai-orchagent-srv");
		row.setMessageHash(UUID.randomUUID().toString());
		row.setPayloadHash(UUID.randomUUID().toString());
		row.setIngestedAt(Instant.now());
		Consumer<RequestContext> insert = context -> persistence.run(Insert.into(KafkaMessages_.class).entry(row));
		runtime.requestContext().systemUser().run(insert);
	}

	@Test
	void storesOnlyTheOrchagentMessagesWithTheirServiceName() {
		IngestionOutcome outcome = ingestionService.ingest();

		assertThat(outcome.fetched()).isEqualTo(4);
		assertThat(outcome.stored()).isEqualTo(2);
		assertThat(outcome.duplicates()).isZero();
		assertThat(outcome.failed()).isZero();

		List<KafkaMessages> stored = storedMessages();
		// Only x-ai-orchagent-srv is persisted; ordered by Kafka offset.
		assertThat(stored).extracting(KafkaMessages::getServiceName)
				.containsExactly("x-ai-orchagent-srv", "x-ai-orchagent-srv");
		assertThat(stored).allSatisfy(message -> {
			assertThat(message.getId()).isNotBlank();
			assertThat(message.getPayload()).isNotBlank();
			assertThat(message.getIngestedAt()).isNotNull();
			assertThat(message.getTopic()).isEqualTo("ms.demo.internal-monitoring");
		});
	}

	@Test
	void skipsMessagesThatCarryNoConversationId() {
		ingestionService.ingest();

		assertThat(storedMessages()).allSatisfy(
				message -> assertThat(message.getConversationId()).isNotBlank());
	}

	@Test
	void doesNotStoreTheSameMessageTwiceAcrossPolls() {
		ingestionService.ingest();
		IngestionOutcome second = ingestionService.ingest();

		assertThat(second.fetched()).isEqualTo(4);
		assertThat(second.stored()).isZero();
		assertThat(second.duplicates()).isEqualTo(2);
		assertThat(storedMessages()).hasSize(2);
		assertThat(ingestionService.countStoredMessages()).isEqualTo(2);
	}

	@Test
	void keepsRecordsThatShareAPayloadButSitAtDifferentOffsets() {
		ingestionService.ingest();

		List<KafkaMessages> sameService = storedMessages().stream()
				.filter(message -> "x-ai-orchagent-srv".equals(message.getServiceName()))
				.toList();

		assertThat(sameService).hasSize(2);
		assertThat(sameService).extracting(KafkaMessages::getPayloadHash)
				.containsExactly(sameService.get(0).getPayloadHash(), sameService.get(0).getPayloadHash());
		assertThat(sameService).extracting(KafkaMessages::getMessageHash).doesNotHaveDuplicates();
		assertThat(sameService).extracting(KafkaMessages::getKafkaOffset)
				.containsExactly(12740892L, 12740999L);
	}

	@Test
	void storesTheOriginalPayloadAndProperties() {
		ingestionService.ingest();

		KafkaMessages message = storedMessages().get(0);

		assertThat(message.getPayload()).contains("\"sap.service.display_name\"");
		assertThat(message.getProperties()).contains("\"key\":\"tenantId\"");
		assertThat(message.getTenantId()).isEqualTo("00000000-0000-0000-0000-000000000001");
		assertThat(message.getUseCaseName()).isEqualTo("rum");
	}

	@Test
	void purgeRemovesEverythingOlderThanTheRetentionPeriod() {
		ingestionService.ingest();
		assertThat(storedMessages()).hasSize(2);

		// A zero length retention makes every stored message expired.
		long deleted = withRetention(Duration.ZERO, ingestionService::purgeExpired);

		assertThat(deleted).isEqualTo(2);
		assertThat(storedMessages()).isEmpty();
	}

	@Test
	void purgeKeepsMessagesOfRetentionExemptTopics() {
		insertMessage("ms.demo.rum");
		insertMessage("ms.demo.internal-monitoring");

		Set<String> originalExempt = monitoringProperties.getRetentionExemptTopics();
		monitoringProperties.setRetentionExemptTopics(Set.of("ms.demo.rum"));
		try {
			// A zero length retention would otherwise expire every stored message.
			long deleted = withRetention(Duration.ZERO, ingestionService::purgeExpired);

			assertThat(deleted).isEqualTo(1);
			assertThat(storedMessages()).extracting(KafkaMessages::getTopic)
					.containsExactly("ms.demo.rum");
		} finally {
			monitoringProperties.setRetentionExemptTopics(originalExempt);
		}
	}

	@Test
	void purgeTopicRemovesOnlyTheNamedTopic() {
		insertMessage("ms.demo.rum");
		insertMessage("ms.demo.rum");
		insertMessage("ms.demo.internal-monitoring");

		long deleted = ingestionService.purgeTopic("ms.demo.rum");

		assertThat(deleted).isEqualTo(2);
		assertThat(storedMessages()).extracting(KafkaMessages::getTopic)
				.containsExactly("ms.demo.internal-monitoring");
	}

	@Test
	void failsClearlyWhenCredentialsAreMissing() {
		credentials.clear();

		assertThatThrownBy(() -> ingestionService.ingest())
				.hasMessageContaining("No API credentials configured");
	}

	@Test
	void storesEveryServiceButStillRequiresAConversationIdOnFilterExemptTopics() {
		String rumEndpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/rum-messages";
		String rumTopic = "ms.demo.rum";

		String originalEndpoint = monitoringProperties.getEndpoint();
		String originalTopic = monitoringProperties.getTopic();
		Set<String> originalExempt = monitoringProperties.getServiceNameFilterExemptTopics();

		monitoringProperties.setEndpoint(rumEndpoint);
		monitoringProperties.setTopic(rumTopic);
		monitoringProperties.setServiceNameFilterExemptTopics(Set.of(rumTopic));
		try {
			IngestionOutcome outcome = ingestionService.ingest();

			// Two non-orchagent messages are fetched; the service-name filter is lifted,
			// but the message without a conversation id is still skipped.
			assertThat(outcome.fetched()).isEqualTo(2);
			assertThat(outcome.stored()).isEqualTo(1);

			List<KafkaMessages> stored = storedMessages();
			assertThat(stored).extracting(KafkaMessages::getServiceName)
					.containsExactly("x-some-other-srv");
			assertThat(stored).allSatisfy(
					message -> assertThat(message.getConversationId()).isNotBlank());
		} finally {
			monitoringProperties.setServiceNameFilterExemptTopics(originalExempt);
			monitoringProperties.setTopic(originalTopic);
			monitoringProperties.setEndpoint(originalEndpoint);
		}
	}

	@Test
	void skipsTheFullMessageExportForExportExemptTopics() {
		String truncatedEndpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/rum-truncated-messages";
		String rumTopic = "ms.demo.rum";

		String originalEndpoint = monitoringProperties.getEndpoint();
		String originalTopic = monitoringProperties.getTopic();
		Set<String> originalServiceExempt = monitoringProperties.getServiceNameFilterExemptTopics();
		Set<String> originalExportExempt = monitoringProperties.getExportExemptTopics();

		monitoringProperties.setEndpoint(truncatedEndpoint);
		monitoringProperties.setTopic(rumTopic);
		monitoringProperties.setServiceNameFilterExemptTopics(Set.of(rumTopic));
		monitoringProperties.setExportExemptTopics(Set.of(rumTopic));
		try {
			IngestionOutcome outcome = ingestionService.ingest();

			assertThat(outcome.fetched()).isEqualTo(1);
			assertThat(outcome.stored()).isEqualTo(1);

			KafkaMessages stored = storedMessages().get(0);
			// The message is kept with its truncated list payload; no export was attempted.
			assertThat(stored.getTruncated()).isTrue();
			assertThat(stored.getServiceName()).isEqualTo("x-some-other-srv");
			assertThat(stored.getConversationId()).isEqualTo("d4e5f6a7-b8c9-0123-defa-234567890123");
			assertThat(stored.getPayload()).contains("x-some-other-srv");
		} finally {
			monitoringProperties.setExportExemptTopics(originalExportExempt);
			monitoringProperties.setServiceNameFilterExemptTopics(originalServiceExempt);
			monitoringProperties.setTopic(originalTopic);
			monitoringProperties.setEndpoint(originalEndpoint);
		}
	}

	/** Runs an action with a temporarily different retention period. */
	private long withRetention(Duration retention, LongSupplier action) {
		Duration original = monitoringProperties.getRetention();
		monitoringProperties.setRetention(retention);
		try {
			return action.getAsLong();
		} finally {
			monitoringProperties.setRetention(original);
		}
	}
}
