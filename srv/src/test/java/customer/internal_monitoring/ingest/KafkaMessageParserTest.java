package customer.internal_monitoring.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import cds.gen.internal.monitoring.ParseStatus;

class KafkaMessageParserTest {

	private static final String TOPIC = "ms.demo.internal-monitoring";

	private KafkaMessageParser parser;

	@BeforeEach
	void setUp() {
		parser = new KafkaMessageParser();
	}

	private List<ParsedKafkaMessage> parseFixture() throws IOException {
		List<ParsedKafkaMessage> messages = new ArrayList<>();
		try (InputStream fixture = getClass().getResourceAsStream("/sample-kafka-response.json")) {
			assertThat(fixture).as("test fixture must be on the classpath").isNotNull();
			int count = parser.parse(fixture, TOPIC, messages::add);
			assertThat(count).isEqualTo(messages.size());
		}
		return messages;
	}

	@Test
	void readsEveryRecordOfTheResponse() throws IOException {
		assertThat(parseFixture()).hasSize(4);
	}

	@Test
	void mapsRecordLevelFields() throws IOException {
		ParsedKafkaMessage message = parseFixture().get(0);

		assertThat(message.correlationId()).isEqualTo("11111111-1111-1111-1111-111111111111");
		assertThat(message.messageId()).isEqualTo("11111111-1111-1111-1111-111111111111");
		assertThat(message.sourceId()).isEqualTo("x-calm-app2app-router");
		assertThat(message.eventType()).isEqualTo("data");
		assertThat(message.messageType()).isEqualTo("calm-usecase-message");
		assertThat(message.kafkaPartition()).isEqualTo(3);
		assertThat(message.kafkaOffset()).isEqualTo(12740892L);
		assertThat(message.headersSize()).isEqualTo(514);
		assertThat(message.topic()).isEqualTo(TOPIC);
		assertThat(message.messageTimestamp()).isEqualTo(Instant.ofEpochMilli(1788431883228L));
	}

	@Test
	void takesServiceNameFromTheDisplayNameAttribute() throws IOException {
		assertThat(parseFixture().get(0).serviceName()).isEqualTo("x-ai-orchagent-srv");
	}

	@Test
	void fallsBackToServiceNameWhenDisplayNameIsAbsent() throws IOException {
		ParsedKafkaMessage message = parseFixture().get(1);

		assertThat(message.serviceName()).isEqualTo("x-landscape-srv-01a06656");
		assertThat(message.parseStatus()).isEqualTo(ParseStatus.OK);
	}

	@Test
	void recoversTheServiceNameFromATruncatedPayload() throws IOException {
		ParsedKafkaMessage message = parseFixture().get(2);

		assertThat(message.serviceName()).isEqualTo("ops-sum-pushreceiver-srv");
		assertThat(message.truncated()).isTrue();
		assertThat(message.parseStatus()).isEqualTo(ParseStatus.TRUNCATED);
		assertThat(message.payloadSize()).isEqualTo(999999L);
	}

	@Test
	void takesContextFieldsFromTheRecordProperties() throws IOException {
		ParsedKafkaMessage message = parseFixture().get(0);

		assertThat(message.tenantId()).isEqualTo("00000000-0000-0000-0000-000000000001");
		assertThat(message.useCaseName()).isEqualTo("rum");
		assertThat(message.serviceType()).isEqualTo("SAP_CALM");
		assertThat(message.calmAction()).isEqualTo("data");
		assertThat(message.agentVersion()).isEqualTo("1.5.41");
		assertThat(message.properties()).contains("\"key\":\"messageId\"");
	}

	@Test
	void keepsThePayloadUnmodified() throws IOException {
		ParsedKafkaMessage message = parseFixture().get(0);

		assertThat(message.payload()).startsWith("{\"payload\":[{\"resource\":");
		assertThat(message.payload()).contains("x-ai-orchagent-srv");
	}

	@Test
	void distinguishesRecordsThatShareAPayloadButNotAnOffset() throws IOException {
		List<ParsedKafkaMessage> messages = parseFixture();
		ParsedKafkaMessage first = messages.get(0);
		ParsedKafkaMessage sameContentOtherOffset = messages.get(3);

		assertThat(sameContentOtherOffset.payload()).isEqualTo(first.payload());
		assertThat(sameContentOtherOffset.payloadHash()).isEqualTo(first.payloadHash());
		assertThat(sameContentOtherOffset.messageHash()).isNotEqualTo(first.messageHash());
	}

	@Test
	void marksAPayloadWithoutReadableAttributesAsInvalid() throws IOException {
		String response = """
				[{"correlationId":"c1","sourceId":"s","eventType":"data","messageType":"m",
				  "timestamp":1788431883228,"partition":1,"offset":2,
				  "payload":"not json at all","payloadSize":15,"headersSize":1,"properties":[]}]
				""";
		List<ParsedKafkaMessage> messages = new ArrayList<>();
		parser.parse(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)), TOPIC, messages::add);

		assertThat(messages).hasSize(1);
		assertThat(messages.get(0).parseStatus()).isEqualTo(ParseStatus.INVALID);
		assertThat(messages.get(0).serviceName()).isEqualTo(KafkaMessageParser.UNKNOWN_SERVICE);
	}

	@Test
	void rejectsAResponseThatIsNotAnArray() {
		InputStream body = new ByteArrayInputStream("{\"unexpected\":true}".getBytes(StandardCharsets.UTF_8));

		assertThatThrownBy(() -> parser.parse(body, TOPIC, message -> {
		}))
				.isInstanceOf(IOException.class)
				.hasMessageContaining("Expected a JSON array");
	}

	@Test
	void ignoresUnknownRecordFields() throws IOException {
		String response = """
				[{"correlationId":"c1","brandNewField":{"nested":[1,2,3]},"partition":7,"offset":8,
				  "payload":"{\\"payload\\":[{\\"resource\\":{\\"attributes\\":[{\\"key\\":\\"sap.service.display_name\\",\\"value\\":{\\"stringValue\\":\\"svc\\"}}]}}]}",
				  "payloadSize":0,"properties":[]}]
				""";
		List<ParsedKafkaMessage> messages = new ArrayList<>();
		parser.parse(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)), TOPIC, messages::add);

		assertThat(messages).hasSize(1);
		assertThat(messages.get(0).serviceName()).isEqualTo("svc");
		assertThat(messages.get(0).kafkaPartition()).isEqualTo(7);
	}

	@Test
	void countsPayloadLengthInBytesNotCharacters() {
		assertThat(KafkaMessageParser.utf8Length("abc")).isEqualTo(3);
		assertThat(KafkaMessageParser.utf8Length("\u00e4")).isEqualTo(2);
		assertThat(KafkaMessageParser.utf8Length("\u20ac")).isEqualTo(3);
		assertThat(KafkaMessageParser.utf8Length("\uD83D\uDE00")).isEqualTo(4);
	}
}
