package customer.internal_monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
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
import com.sap.cds.ql.Select;
import com.sap.cds.services.persistence.PersistenceService;
import com.sap.cds.services.request.RequestContext;
import com.sap.cds.services.runtime.CdsRuntime;
import com.sun.net.httpserver.HttpServer;

import cds.gen.internal.monitoring.KafkaMessages;
import cds.gen.internal.monitoring.KafkaMessages_;
import cds.gen.internal.monitoring.ParseStatus;
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
		server.createContext("/messages", exchange -> {
			byte[] body;
			try (InputStream fixture = MessageIngestionIntegrationTest.class
					.getResourceAsStream("/sample-kafka-response.json")) {
				body = fixture.readAllBytes();
			}
			exchange.getResponseHeaders().add("Content-Type", "application/json");
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
			exchange.close();
		});
		server.start();
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

	@Test
	void storesEveryDistinctMessageWithItsServiceName() {
		IngestionOutcome outcome = ingestionService.ingest();

		assertThat(outcome.fetched()).isEqualTo(4);
		assertThat(outcome.stored()).isEqualTo(4);
		assertThat(outcome.duplicates()).isZero();
		assertThat(outcome.failed()).isZero();

		List<KafkaMessages> stored = storedMessages();
		// Ordered by Kafka offset: 12740892, 12740951, 12740999, 12741000.
		assertThat(stored).extracting(KafkaMessages::getServiceName)
				.containsExactly("x-ai-orchagent-srv", "x-landscape-srv-01a06656",
						"x-ai-orchagent-srv", "ops-sum-pushreceiver-srv");
		assertThat(stored).allSatisfy(message -> {
			assertThat(message.getId()).isNotBlank();
			assertThat(message.getPayload()).isNotBlank();
			assertThat(message.getIngestedAt()).isNotNull();
			assertThat(message.getTopic()).isEqualTo("ms.demo.internal-monitoring");
		});
	}

	@Test
	void doesNotStoreTheSameMessageTwiceAcrossPolls() {
		ingestionService.ingest();
		IngestionOutcome second = ingestionService.ingest();

		assertThat(second.fetched()).isEqualTo(4);
		assertThat(second.stored()).isZero();
		assertThat(second.duplicates()).isEqualTo(4);
		assertThat(storedMessages()).hasSize(4);
		assertThat(ingestionService.countStoredMessages()).isEqualTo(4);
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
	void recordsTruncatedPayloadsWithoutLosingTheServiceName() {
		ingestionService.ingest();

		KafkaMessages truncated = storedMessages().stream()
				.filter(message -> Boolean.TRUE.equals(message.getTruncated()))
				.findFirst()
				.orElseThrow();

		assertThat(truncated.getServiceName()).isEqualTo("ops-sum-pushreceiver-srv");
		assertThat(truncated.getParseStatus()).isEqualTo(ParseStatus.TRUNCATED);
		assertThat(truncated.getPayloadSize()).isEqualTo(999999L);
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
		assertThat(storedMessages()).hasSize(4);

		// A zero length retention makes every stored message expired.
		long deleted = withRetention(Duration.ZERO, ingestionService::purgeExpired);

		assertThat(deleted).isEqualTo(4);
		assertThat(storedMessages()).isEmpty();
	}

	@Test
	void failsClearlyWhenCredentialsAreMissing() {
		credentials.clear();

		assertThatThrownBy(() -> ingestionService.ingest())
				.hasMessageContaining("No API credentials configured");
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
