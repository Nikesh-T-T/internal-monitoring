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

	private HttpRequest buildRequest(String endpoint, ApiCredentialStore.Credentials creds) {
		HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(endpoint))
				.GET()
				.timeout(properties.getRequestTimeout())
				.header("Accept", properties.getAccept())
				.header("Accept-Encoding", "gzip")
				.header("Cookie", creds.cookie());

		if (creds.xsrfToken() != null) {
			builder.header("X-XSRF-TOKEN", creds.xsrfToken());
		}
		for (Map.Entry<String, String> header : properties.getHeaders().entrySet()) {
			builder.header(header.getKey(), header.getValue());
		}
		return builder.build();
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
