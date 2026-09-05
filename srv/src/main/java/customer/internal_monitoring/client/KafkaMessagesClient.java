package customer.internal_monitoring.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.springframework.stereotype.Component;

import customer.internal_monitoring.config.KafkaMonitoringProperties;

/**
 * Thin HTTP client for the Kafka messages API.
 *
 * <p>The endpoint is protected by a browser session, so every call is made with
 * the credentials currently held by the {@link ApiCredentialStore}. The response
 * is handed to the caller as a stream: it regularly exceeds ten megabytes and
 * must not be buffered into a single string.
 */
@Component
public class KafkaMessagesClient {

	/** Consumes the response body stream. */
	@FunctionalInterface
	public interface BodyReader<T> {
		T read(InputStream body) throws IOException;
	}

	/**
	 * Consumes the properties/metadata entry of an export archive as a stream and
	 * the full payload as a string. The payload is {@code null} for single-entry
	 * archives that carry the full payload inline in the metadata's {@code message}.
	 */
	@FunctionalInterface
	public interface ExportReader<T> {
		T read(InputStream properties, String fullPayload) throws IOException;
	}

	private final KafkaMonitoringProperties properties;
	private final ApiCredentialStore credentials;
	private final HttpClient httpClient;

	public KafkaMessagesClient(KafkaMonitoringProperties properties, ApiCredentialStore credentials) {
		this.properties = properties;
		this.credentials = credentials;
		this.httpClient = HttpClient.newBuilder()
				.connectTimeout(properties.getConnectTimeout())
				.followRedirects(HttpClient.Redirect.NORMAL)
				.build();
	}

	/**
	 * Calls the endpoint and passes the decoded body to {@code reader}.
	 *
	 * @throws KafkaApiException if the endpoint is not configured, no credentials
	 *                           are available, the call fails, or a non-2xx status
	 *                           is returned
	 */
	public <T> T fetchMessages(BodyReader<T> reader) {
		return fetchMessages(properties.getEndpoint(), reader);
	}

	/**
	 * Calls the given endpoint and passes the decoded body to {@code reader}.
	 *
	 * <p>The endpoint is passed explicitly (rather than read from the properties)
	 * so a whole ingestion run stays pinned to one topic even if a concurrent topic
	 * switch repoints {@code monitoring.kafka.endpoint} mid-run.
	 *
	 * @throws KafkaApiException if the endpoint is not configured, no credentials
	 *                           are available, the call fails, or a non-2xx status
	 *                           is returned
	 */
	public <T> T fetchMessages(String endpoint, BodyReader<T> reader) {
		if (endpoint == null || endpoint.isBlank()) {
			throw new KafkaApiException("Property 'monitoring.kafka.endpoint' is not configured", -1);
		}
		ApiCredentialStore.Credentials creds = credentials.get()
				.orElseThrow(() -> new KafkaApiException(
						"No API credentials configured. Call the 'setCredentials' action first.", 401));

		HttpRequest request = buildRequest(endpoint, creds);
		HttpResponse<InputStream> response;
		try {
			response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
		} catch (IOException e) {
			throw new KafkaApiException("Call to Kafka messages API failed: " + e.getMessage(), e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new KafkaApiException("Call to Kafka messages API was interrupted", e);
		}

		try (InputStream body = decode(response)) {
			int status = response.statusCode();
			if (status < 200 || status >= 300) {
				throw new KafkaApiException(
						"Kafka messages API returned HTTP " + status + ": " + readErrorBody(body), status);
			}
			return reader.read(body);
		} catch (IOException e) {
			throw new KafkaApiException("Reading the Kafka messages API response failed: " + e.getMessage(), e);
		}
	}

	/**
	 * Downloads the full, untruncated message at the given Kafka coordinates.
	 *
	 * <p>The list endpoint truncates large payloads; this per-message export endpoint
	 * returns the complete message. The response is a ZIP archive with a single JSON
	 * entry, which is unwrapped and handed to {@code reader}.
	 *
	 * @throws KafkaApiException if the endpoint is not configured, no credentials are
	 *                           available, the call fails, a non-2xx status is
	 *                           returned, or the archive is empty
	 */
	public <T> T exportMessage(int partition, long offset, ExportReader<T> reader) {
		return exportMessage(properties.getEndpoint(), partition, offset, reader);
	}

	/**
	 * Downloads the full, untruncated message at the given Kafka coordinates from the
	 * given endpoint's export sibling.
	 *
	 * <p>The list endpoint truncates large payloads; this per-message export endpoint
	 * returns the complete message. The response is a ZIP archive with a single JSON
	 * entry, which is unwrapped and handed to {@code reader}.
	 *
	 * <p>The endpoint is passed explicitly so the offset/partition (harvested from a
	 * specific topic's list response) are always exported against that same topic's
	 * endpoint, even if a concurrent topic switch repoints the properties mid-run.
	 *
	 * @throws KafkaApiException if the endpoint is not configured, no credentials are
	 *                           available, the call fails, a non-2xx status is
	 *                           returned, or the archive is empty
	 */
	public <T> T exportMessage(String endpoint, int partition, long offset, ExportReader<T> reader) {
		if (endpoint == null || endpoint.isBlank()) {
			throw new KafkaApiException("Property 'monitoring.kafka.endpoint' is not configured", -1);
		}
		ApiCredentialStore.Credentials creds = credentials.get()
				.orElseThrow(() -> new KafkaApiException(
						"No API credentials configured. Call the 'setCredentials' action first.", 401));

		HttpRequest request = buildExportRequest(exportUrl(endpoint, partition, offset), creds);
		HttpResponse<InputStream> response;
		try {
			response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
		} catch (IOException e) {
			throw new KafkaApiException("Call to Kafka export API failed: " + e.getMessage(), e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new KafkaApiException("Call to Kafka export API was interrupted", e);
		}

		try (InputStream body = decode(response)) {
			int status = response.statusCode();
			if (status < 200 || status >= 300) {
				throw new KafkaApiException(
						"Kafka export API returned HTTP " + status + ": " + readErrorBody(body), status);
			}
			return readExportArchive(new ZipInputStream(body), reader, status);
		} catch (IOException e) {
			throw new KafkaApiException("Reading the Kafka export API response failed: " + e.getMessage(), e);
		}
	}

	/**
	 * Splits the export archive into its properties and payload entries and hands
	 * both to {@code reader}.
	 *
	 * <p>Two layouts occur across topics. The two-entry layout holds a
	 * {@code _properties.json} entry (record metadata) and a {@code _payload.json}
	 * entry (the full untruncated payload); both are matched by name so their order
	 * does not matter, and the payload is passed as {@code fullPayload}. The
	 * single-entry layout holds one {@code data-<ts>.json} entry that already
	 * carries the full payload inline in its {@code message} object; that entry is
	 * passed as the properties stream with a {@code null} {@code fullPayload}, so
	 * the reader uses the inline payload.
	 */
	private <T> T readExportArchive(ZipInputStream zip, ExportReader<T> reader, int status) throws IOException {
		byte[] properties = null;
		byte[] combined = null;
		String payload = null;
		int entryCount = 0;
		ZipEntry entry;
		while ((entry = zip.getNextEntry()) != null) {
			entryCount++;
			String name = entry.getName();
			if (name.endsWith("_payload.json")) {
				payload = new String(zip.readAllBytes(), StandardCharsets.UTF_8);
			} else if (name.endsWith("_properties.json")) {
				properties = zip.readAllBytes();
			} else {
				combined = zip.readAllBytes();
			}
		}
		if (properties == null && combined != null && entryCount == 1) {
			return reader.read(new ByteArrayInputStream(combined), null);
		}
		if (properties == null) {
			throw new KafkaApiException("Kafka export archive did not contain a properties entry", status);
		}
		if (payload == null) {
			throw new KafkaApiException("Kafka export archive did not contain a payload entry", status);
		}
		return reader.read(new ByteArrayInputStream(properties), payload);
	}

	/**
	 * Derives the export URL from the configured list endpoint.
	 *
	 * <p>The two endpoints differ only by the last path segment: {@code .../messages}
	 * becomes {@code .../export-message?offset=<offset>&partition=<partition>}.
	 */
	private static String exportUrl(String endpoint, int partition, long offset) {
		if (!endpoint.endsWith("/messages")) {
			throw new KafkaApiException(
					"Cannot derive the export URL: 'monitoring.kafka.endpoint' must end with '/messages'", -1);
		}
		String base = endpoint.substring(0, endpoint.length() - "/messages".length());
		return base + "/export-message?offset=" + offset + "&partition=" + partition;
	}

	private HttpRequest buildRequest(String endpoint, ApiCredentialStore.Credentials creds) {
		return applyHeaders(HttpRequest.newBuilder(URI.create(endpoint)).GET(), creds).build();
	}

	private HttpRequest buildExportRequest(String url, ApiCredentialStore.Credentials creds) {
		return applyHeaders(HttpRequest.newBuilder(URI.create(url)).POST(HttpRequest.BodyPublishers.noBody()), creds)
				.build();
	}

	private HttpRequest.Builder applyHeaders(HttpRequest.Builder builder, ApiCredentialStore.Credentials creds) {
		builder.timeout(properties.getRequestTimeout())
				.header("Accept", properties.getAccept())
				.header("Accept-Encoding", "gzip")
				.header("Cookie", creds.cookie());

		if (creds.xsrfToken() != null) {
			builder.header("X-XSRF-TOKEN", creds.xsrfToken());
		}
		for (Map.Entry<String, String> header : properties.getHeaders().entrySet()) {
			builder.header(header.getKey(), header.getValue());
		}
		return builder;
	}

	private InputStream decode(HttpResponse<InputStream> response) throws IOException {
		InputStream body = response.body();
		String encoding = response.headers().firstValue("Content-Encoding").orElse("");
		return "gzip".equalsIgnoreCase(encoding) ? new GZIPInputStream(body) : body;
	}

	/** Reads a short excerpt of an error body for diagnostics. */
	private String readErrorBody(InputStream body) {
		try {
			byte[] excerpt = body.readNBytes(512);
			return new String(excerpt, StandardCharsets.UTF_8).replaceAll("\\s+", " ").trim();
		} catch (IOException e) {
			return "<response body unavailable: " + e.getMessage() + ">";
		}
	}
}
