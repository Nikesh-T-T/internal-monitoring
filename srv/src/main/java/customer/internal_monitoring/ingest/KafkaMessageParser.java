package customer.internal_monitoring.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;

import cds.gen.internal.monitoring.ParseStatus;

/**
 * Turns the API response into {@link ParsedKafkaMessage} instances.
 *
 * <p>The response is read as a token stream because it routinely exceeds ten
 * megabytes. Two properties of the upstream data drive the design:
 *
 * <ul>
 * <li>Each record carries its message as an <em>escaped JSON string</em> in
 * {@code payload}, which has to be parsed a second time.</li>
 * <li>The API truncates large payloads at roughly 100 kB, so a payload is not
 * necessarily well-formed JSON. The resource attributes sit at the very front of
 * the payload, so the service name can still be recovered; parsing therefore
 * stops as soon as those attributes have been read and tolerates a failure
 * afterwards.</li>
 * </ul>
 */
@Component
public class KafkaMessageParser {

	/** Attribute holding the human readable service name. */
	static final String ATTR_DISPLAY_NAME = "sap.service.display_name";
	/** Fallbacks used when the display name is absent. */
	static final String ATTR_SERVICE_NAME = "service.name";
	static final String ATTR_APP_NAME = "cloudfoundry.app.name";
	static final String UNKNOWN_SERVICE = "UNKNOWN";

	/** Span attribute holding the conversation the message belongs to. */
	static final String ATTR_CONVERSATION_ID = "gen_ai.conversation.id";

	private static final int MAX_SERVICE_NAME = 255;
	private static final int MAX_ID = 64;
	private static final int MAX_SOURCE_ID = 128;
	private static final int MAX_MESSAGE_TYPE = 128;
	private static final int MAX_VERSION = 32;

	private final JsonFactory factory;

	public KafkaMessageParser() {
		this.factory = JsonFactory.builder()
				.streamReadConstraints(StreamReadConstraints.builder()
						.maxStringLength(Integer.MAX_VALUE)
						.maxNestingDepth(2000)
						.build())
				.build();
	}

	/**
	 * Reads the response and hands every record to {@code sink}.
	 *
	 * @return the number of records read
	 * @throws IOException if the response is not a JSON array of records
	 */
	public int parse(InputStream response, String topic, Consumer<ParsedKafkaMessage> sink) throws IOException {
		try (JsonParser parser = factory.createParser(response)) {
			if (parser.nextToken() != JsonToken.START_ARRAY) {
				throw new IOException("Expected a JSON array of Kafka records but found: " + parser.currentToken());
			}
			int count = 0;
			while (parser.nextToken() == JsonToken.START_OBJECT) {
				sink.accept(readRecord(parser, topic));
				count++;
			}
			return count;
		}
	}

	/**
	 * Reads a message exported by the per-message export endpoint.
	 *
	 * <p>The export archive wraps the full, untruncated record in
	 * {@code {"topic": ..., "message": { ...record... }}}. The inner {@code message}
	 * object carries the same fields as a list-endpoint record, so the standard
	 * record parsing is reused on it.
	 *
	 * @throws IOException if the response does not contain a {@code message} object
	 */
	public ParsedKafkaMessage parseExportedMessage(InputStream body, String topic) throws IOException {
		try (JsonParser parser = factory.createParser(body)) {
			if (parser.nextToken() != JsonToken.START_OBJECT) {
				throw new IOException("Expected a JSON object but found: " + parser.currentToken());
			}
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				String field = parser.currentName();
				parser.nextToken();
				if ("message".equals(field) && parser.currentToken() == JsonToken.START_OBJECT) {
					return readRecord(parser, topic);
				}
				parser.skipChildren();
			}
			throw new IOException("Exported response did not contain a 'message' object");
		}
	}

	private ParsedKafkaMessage readRecord(JsonParser parser, String topic) throws IOException {
		String correlationId = null;
		String sourceId = null;
		String eventType = null;
		String messageType = null;
		String payload = null;
		Long timestamp = null;
		Integer partition = null;
		Long offset = null;
		Long payloadSize = null;
		Integer headersSize = null;
		Map<String, String> properties = new LinkedHashMap<>();

		while (parser.nextToken() != JsonToken.END_OBJECT) {
			String field = parser.currentName();
			parser.nextToken();
			switch (field) {
				case "correlationId" -> correlationId = parser.getValueAsString();
				case "sourceId" -> sourceId = parser.getValueAsString();
				case "eventType" -> eventType = parser.getValueAsString();
				case "messageType" -> messageType = parser.getValueAsString();
				case "timestamp" -> timestamp = parser.getValueAsLong();
				case "partition" -> partition = parser.getValueAsInt();
				case "offset" -> offset = parser.getValueAsLong();
				case "payload" -> payload = parser.getValueAsString();
				case "payloadSize" -> payloadSize = parser.getValueAsLong();
				case "headersSize" -> headersSize = parser.getValueAsInt();
				case "properties" -> readProperties(parser, properties);
				default -> parser.skipChildren();
			}
		}

		Map<String, String> attributes = readResourceAttributes(payload);
		String serviceName = resolveServiceName(attributes);
		String conversationId = readConversationId(payload);
		boolean truncated = payloadSize != null && payload != null && utf8Length(payload) < payloadSize;

		String parseStatus;
		if (truncated) {
			parseStatus = ParseStatus.TRUNCATED;
		} else if (attributes.isEmpty()) {
			parseStatus = ParseStatus.INVALID;
		} else {
			parseStatus = ParseStatus.OK;
		}

		return new ParsedKafkaMessage(
				truncate(serviceName, MAX_SERVICE_NAME),
				MessageHashes.messageHash(partition, offset, payload),
				MessageHashes.payloadHash(payload),
				truncate(correlationId, MAX_ID),
				truncate(properties.get("messageId"), MAX_ID),
				truncate(conversationId, MAX_ID),
				truncate(sourceId, MAX_SOURCE_ID),
				truncate(eventType, MAX_ID),
				truncate(messageType, MAX_MESSAGE_TYPE),
				timestamp == null ? null : Instant.ofEpochMilli(timestamp),
				partition,
				offset,
				payloadSize,
				headersSize,
				truncated,
				parseStatus,
				truncate(properties.get("tenantId"), MAX_ID),
				truncate(properties.get("usecaseName"), MAX_ID),
				truncate(properties.get("serviceType"), MAX_ID),
				truncate(properties.get("action"), MAX_ID),
				truncate(properties.get("version"), MAX_VERSION),
				payload,
				serializeProperties(properties),
				topic == null || topic.isBlank() ? null : truncate(topic, MAX_SERVICE_NAME));
	}

	/** Reads the record level {@code properties} array of {@code {key, value}} pairs. */
	private void readProperties(JsonParser parser, Map<String, String> target) throws IOException {
		if (parser.currentToken() != JsonToken.START_ARRAY) {
			parser.skipChildren();
			return;
		}
		while (parser.nextToken() == JsonToken.START_OBJECT) {
			String key = null;
			String value = null;
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				String field = parser.currentName();
				parser.nextToken();
				if ("key".equals(field)) {
					key = parser.getValueAsString();
				} else if ("value".equals(field)) {
					value = parser.getValueAsString();
				} else {
					parser.skipChildren();
				}
			}
			if (key != null) {
				target.put(key, value);
			}
		}
	}

	/**
	 * Extracts the {@code resource.attributes} of the nested payload.
	 *
	 * <p>Attributes found before a parse failure are kept: a payload truncated by
	 * the API still carries a usable attribute block at its start.
	 */
	Map<String, String> readResourceAttributes(String payload) {
		Map<String, String> attributes = new LinkedHashMap<>();
		if (payload == null || payload.isBlank()) {
			return attributes;
		}
		try (JsonParser parser = factory.createParser(payload)) {
			boolean insideResource = false;
			JsonToken token;
			while ((token = parser.nextToken()) != null) {
				if (token != JsonToken.FIELD_NAME) {
					continue;
				}
				String field = parser.currentName();
				if ("resource".equals(field)) {
					insideResource = true;
				} else if (insideResource && "attributes".equals(field)) {
					if (parser.nextToken() == JsonToken.START_ARRAY) {
						readAttributeArray(parser, attributes);
					}
					break;
				}
			}
		} catch (IOException e) {
			// Truncated or malformed payload: keep whatever was read so far.
		}
		return attributes;
	}

	private void readAttributeArray(JsonParser parser, Map<String, String> target) throws IOException {
		while (parser.nextToken() == JsonToken.START_OBJECT) {
			String key = null;
			String value = null;
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				String field = parser.currentName();
				parser.nextToken();
				if ("key".equals(field)) {
					key = parser.getValueAsString();
				} else if ("value".equals(field)) {
					value = readAttributeValue(parser);
				} else {
					parser.skipChildren();
				}
			}
			if (key != null) {
				target.put(key, value);
			}
		}
	}

	/** Reads the first scalar out of an OpenTelemetry {@code AnyValue} wrapper. */
	private String readAttributeValue(JsonParser parser) throws IOException {
		if (parser.currentToken() != JsonToken.START_OBJECT) {
			return parser.getValueAsString();
		}
		String value = null;
		while (parser.nextToken() != JsonToken.END_OBJECT) {
			parser.nextToken();
			if (parser.currentToken().isScalarValue()) {
				if (value == null) {
					value = parser.getValueAsString();
				}
			} else {
				parser.skipChildren();
			}
		}
		return value;
	}

	/**
	 * Extracts the {@code gen_ai.conversation.id} span attribute from the payload.
	 *
	 * <p>The value is identical across every span of a message, so the first span
	 * that carries it wins. Spans live under {@code payload[].scopeSpans[].spans[]}
	 * and sit after the resource block, so a payload truncated by the API may not
	 * reach them; in that case, and when no span carries the attribute, this
	 * returns {@code null}.
	 */
	String readConversationId(String payload) {
		if (payload == null || payload.isBlank()) {
			return null;
		}
		try (JsonParser parser = factory.createParser(payload)) {
			JsonToken token;
			while ((token = parser.nextToken()) != null) {
				if (token == JsonToken.FIELD_NAME && "spans".equals(parser.currentName())
						&& parser.nextToken() == JsonToken.START_ARRAY) {
					String found = readConversationIdFromSpans(parser);
					if (found != null) {
						return found;
					}
				}
			}
		} catch (IOException e) {
			// Truncated or malformed payload: no conversation id recoverable.
		}
		return null;
	}

	/** Scans a {@code spans} array and returns the first conversation id found. */
	private String readConversationIdFromSpans(JsonParser parser) throws IOException {
		while (parser.nextToken() == JsonToken.START_OBJECT) {
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				String field = parser.currentName();
				parser.nextToken();
				if ("attributes".equals(field) && parser.currentToken() == JsonToken.START_ARRAY) {
					String value = readConversationIdFromAttributes(parser);
					if (value != null) {
						return value;
					}
				} else {
					parser.skipChildren();
				}
			}
		}
		return null;
	}

	/** Returns the value of the conversation id key within one span's attributes. */
	private String readConversationIdFromAttributes(JsonParser parser) throws IOException {
		String conversationId = null;
		while (parser.nextToken() == JsonToken.START_OBJECT) {
			String key = null;
			String value = null;
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				String field = parser.currentName();
				parser.nextToken();
				if ("key".equals(field)) {
					key = parser.getValueAsString();
				} else if ("value".equals(field)) {
					value = readAttributeValue(parser);
				} else {
					parser.skipChildren();
				}
			}
			if (conversationId == null && ATTR_CONVERSATION_ID.equals(key)) {
				conversationId = value;
			}
		}
		return conversationId;
	}

	private String resolveServiceName(Map<String, String> attributes) {
		String name = firstNonBlank(
				attributes.get(ATTR_DISPLAY_NAME),
				attributes.get(ATTR_SERVICE_NAME),
				attributes.get(ATTR_APP_NAME));
		return name != null ? name : UNKNOWN_SERVICE;
	}

	private String serializeProperties(Map<String, String> properties) throws IOException {
		if (properties.isEmpty()) {
			return null;
		}
		StringWriter writer = new StringWriter();
		try (JsonGenerator out = factory.createGenerator(writer)) {
			out.writeStartArray();
			for (Map.Entry<String, String> entry : properties.entrySet()) {
				out.writeStartObject();
				out.writeStringField("key", entry.getKey());
				out.writeStringField("value", entry.getValue());
				out.writeEndObject();
			}
			out.writeEndArray();
		}
		return writer.toString();
	}

	private static String firstNonBlank(String... values) {
		for (String value : values) {
			if (value != null && !value.isBlank()) {
				return value;
			}
		}
		return null;
	}

	private static String truncate(String value, int maxLength) {
		if (value == null) {
			return null;
		}
		return value.length() <= maxLength ? value : value.substring(0, maxLength);
	}

	/** UTF-8 byte length without allocating a byte array. */
	static long utf8Length(String value) {
		long length = 0;
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			if (c <= 0x7F) {
				length += 1;
			} else if (c <= 0x7FF) {
				length += 2;
			} else if (Character.isHighSurrogate(c)) {
				length += 4;
				i++;
			} else {
				length += 3;
			}
		}
		return length;
	}
}
