package customer.internal_monitoring.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;

import customer.internal_monitoring.config.KafkaMonitoringProperties;

class KafkaMessagesClientTest {

	private HttpServer server;
	private KafkaMonitoringProperties properties;
	private ApiCredentialStore credentials;
	private final AtomicReference<Headers> lastRequestHeaders = new AtomicReference<>();
	private final AtomicReference<String> lastRequestMethod = new AtomicReference<>();
	private final AtomicReference<String> lastRequestUri = new AtomicReference<>();

	/** A complete cookie: the endpoint needs the session and the routing cookie. */
	private static final String COOKIE = "JSESSIONID=abc; __VCAP_ID__=inst-1";

	@BeforeEach
	void setUp() throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.start();

		properties = new KafkaMonitoringProperties();
		properties.setEndpoint("http://127.0.0.1:" + server.getAddress().getPort() + "/messages");
		credentials = new ApiCredentialStore();
	}

	@AfterEach
	void tearDown() {
		server.stop(0);
	}

	private void respond(int status, String body, boolean gzip) {
		server.createContext("/messages", exchange -> {
			lastRequestHeaders.set(exchange.getRequestHeaders());
			lastRequestMethod.set(exchange.getRequestMethod());
			lastRequestUri.set(exchange.getRequestURI().toString());
			byte[] payload = body.getBytes(StandardCharsets.UTF_8);
			if (gzip) {
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				try (GZIPOutputStream out = new GZIPOutputStream(buffer)) {
					out.write(payload);
				}
				payload = buffer.toByteArray();
				exchange.getResponseHeaders().add("Content-Encoding", "gzip");
			}
			exchange.sendResponseHeaders(status, payload.length);
			try (var out = exchange.getResponseBody()) {
				out.write(payload);
			}
			exchange.close();
		});
	}

	/** Serves {@code /export-message} with the given JSON packed into a ZIP archive. */
	private void respondWithZippedExport(int status, String json) {
		server.createContext("/export-message", exchange -> {
			lastRequestHeaders.set(exchange.getRequestHeaders());
			lastRequestMethod.set(exchange.getRequestMethod());
			lastRequestUri.set(exchange.getRequestURI().toString());
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			try (ZipOutputStream zip = new ZipOutputStream(buffer)) {
				zip.putNextEntry(new ZipEntry("data-1.json"));
				zip.write(json.getBytes(StandardCharsets.UTF_8));
				zip.closeEntry();
			}
			byte[] payload = buffer.toByteArray();
			exchange.sendResponseHeaders(status, payload.length);
			try (var out = exchange.getResponseBody()) {
				out.write(payload);
			}
			exchange.close();
		});
	}

	private KafkaMessagesClient client() {
		return new KafkaMessagesClient(properties, credentials);
	}

	private static String read(InputStream body) throws IOException {
		return new String(body.readAllBytes(), StandardCharsets.UTF_8);
	}

	@Test
	void sendsTheConfiguredCredentialsAndHeaders() {
		respond(200, "[]", false);
		credentials.update(COOKIE, "xsrf-1");
		properties.setHeaders(Map.of("X-Custom", "custom-value"));

		String body = client().fetchMessages(KafkaMessagesClientTest::read);

		assertThat(body).isEqualTo("[]");
		Headers sent = lastRequestHeaders.get();
		assertThat(sent.get("Cookie")).isEqualTo(List.of(COOKIE));
		assertThat(sent.get("X-xsrf-token")).isEqualTo(List.of("xsrf-1"));
		assertThat(sent.get("X-custom")).isEqualTo(List.of("custom-value"));
		assertThat(sent.getFirst("Accept")).isEqualTo("application/json;charset=UTF-8");
	}

	@Test
	void omitsTheXsrfHeaderWhenNoTokenIsKnown() {
		respond(200, "[]", false);
		credentials.update(COOKIE, null);

		client().fetchMessages(KafkaMessagesClientTest::read);

		assertThat(lastRequestHeaders.get().get("X-xsrf-token")).isNull();
	}

	@Test
	void decompressesGzippedResponses() {
		String payload = "[{\"correlationId\":\"c1\"}]";
		respond(200, payload, true);
		credentials.update(COOKIE, null);

		assertThat(client().fetchMessages(KafkaMessagesClientTest::read)).isEqualTo(payload);
	}

	@Test
	void failsWithoutCredentials() {
		respond(200, "[]", false);

		assertThatThrownBy(() -> client().fetchMessages(KafkaMessagesClientTest::read))
				.isInstanceOf(KafkaApiException.class)
				.hasMessageContaining("No API credentials configured")
				.extracting(error -> ((KafkaApiException) error).isAuthenticationFailure())
				.isEqualTo(true);
	}

	@Test
	void failsWithoutAnEndpoint() {
		properties.setEndpoint("  ");
		credentials.update(COOKIE, null);

		assertThatThrownBy(() -> client().fetchMessages(KafkaMessagesClientTest::read))
				.isInstanceOf(KafkaApiException.class)
				.hasMessageContaining("monitoring.kafka.endpoint");
	}

	@Test
	void reportsAnExpiredSessionAsAnAuthenticationFailure() {
		respond(401, "{\"status\":401,\"error\":\"Unauthorized\"}", false);
		credentials.update("JSESSIONID=expired; __VCAP_ID__=inst-1", null);

		assertThatThrownBy(() -> client().fetchMessages(KafkaMessagesClientTest::read))
				.isInstanceOf(KafkaApiException.class)
				.hasMessageContaining("HTTP 401")
				.extracting(error -> ((KafkaApiException) error).isAuthenticationFailure())
				.isEqualTo(true);
	}

	@Test
	void reportsOtherErrorStatusesWithoutClaimingAnAuthProblem() {
		respond(503, "service unavailable", false);
		credentials.update(COOKIE, null);

		assertThatThrownBy(() -> client().fetchMessages(KafkaMessagesClientTest::read))
				.isInstanceOf(KafkaApiException.class)
				.hasMessageContaining("HTTP 503")
				.extracting(error -> ((KafkaApiException) error).isAuthenticationFailure())
				.isEqualTo(false);
	}

	@Test
	void surfacesConnectionFailures() {
		properties.setEndpoint("http://127.0.0.1:1/messages");
		credentials.update(COOKIE, null);

		assertThatThrownBy(() -> client().fetchMessages(KafkaMessagesClientTest::read))
				.isInstanceOf(KafkaApiException.class)
				.hasMessageContaining("failed");
	}

	@Test
	void exportsAMessageByUnwrappingTheZipArchive() {
		String json = "{\"topic\":\"t\",\"message\":{\"offset\":42}}";
		respondWithZippedExport(200, json);
		credentials.update(COOKIE, "xsrf-1");

		String body = client().exportMessage(8, 42L, KafkaMessagesClientTest::read);

		assertThat(body).isEqualTo(json);
		assertThat(lastRequestMethod.get()).isEqualTo("POST");
		assertThat(lastRequestUri.get()).isEqualTo("/export-message?offset=42&partition=8");
		assertThat(lastRequestHeaders.get().get("X-xsrf-token")).isEqualTo(List.of("xsrf-1"));
		assertThat(lastRequestHeaders.get().get("Cookie")).isEqualTo(List.of(COOKIE));
	}

	@Test
	void reportsExportErrorStatuses() {
		server.createContext("/export-message", exchange -> {
			byte[] payload = "boom".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(500, payload.length);
			try (var out = exchange.getResponseBody()) {
				out.write(payload);
			}
			exchange.close();
		});
		credentials.update(COOKIE, "xsrf-1");

		assertThatThrownBy(() -> client().exportMessage(8, 42L, KafkaMessagesClientTest::read))
				.isInstanceOf(KafkaApiException.class)
				.hasMessageContaining("HTTP 500");
	}

	@Test
	void failsToExportWhenTheEndpointDoesNotEndWithMessages() {
		properties.setEndpoint("http://127.0.0.1:" + server.getAddress().getPort() + "/other");
		credentials.update(COOKIE, "xsrf-1");

		assertThatThrownBy(() -> client().exportMessage(8, 42L, KafkaMessagesClientTest::read))
				.isInstanceOf(KafkaApiException.class)
				.hasMessageContaining("must end with '/messages'");
	}
}
