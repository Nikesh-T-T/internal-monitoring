package customer.internal_monitoring.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPInputStream;
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
		String endpoint = properties.getEndpoint();
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
	public <T> T exportMessage(int partition, long offset, BodyReader<T> reader) {
		String endpoint = properties.getEndpoint();
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
			ZipInputStream zip = new ZipInputStream(body);
			if (zip.getNextEntry() == null) {
				throw new KafkaApiException("Kafka export API returned an empty archive", status);
			}
			return reader.read(zip);
		} catch (IOException e) {
			throw new KafkaApiException("Reading the Kafka export API response failed: " + e.getMessage(), e);
		}
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
