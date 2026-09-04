package customer.internal_monitoring.client;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Holds the API credentials in memory.
 *
 * <p>The upstream endpoint is protected by a browser session that expires, so
 * the credentials have to be replaced from time to time through the
 * {@code setCredentials} action. They are never written to the database, to a
 * configuration file or to the log.
 */
@Component
public class ApiCredentialStore {

	/**
	 * Immutable credential snapshot.
	 *
	 * @param cookie    raw value of the {@code Cookie} request header
	 * @param xsrfToken value for the {@code X-XSRF-TOKEN} header, may be {@code null}
	 * @param updatedAt when the credentials were stored
	 */
	public record Credentials(String cookie, String xsrfToken, Instant updatedAt) {
	}

	private final AtomicReference<Credentials> current = new AtomicReference<>();

	private static final Logger log = LoggerFactory.getLogger(ApiCredentialStore.class);

	/**
	 * Cookies the endpoint was observed to need.
	 *
	 * <p>{@code JSESSIONID} carries the session itself. {@code __VCAP_ID__} pins
	 * the request to the Cloud Foundry instance that holds that session: without
	 * it the platform load balancer picks an instance at random and requests fail
	 * with {@code 401} intermittently, which is hard to recognise from the
	 * outside. Both are part of a browser "Copy as cURL" command, so a missing one
	 * means the value was trimmed on the way here.
	 */
	private static final List<String> EXPECTED_COOKIES = List.of("JSESSIONID", "__VCAP_ID__");

	/**
	 * Replaces the stored credentials.
	 *
	 * <p>When no XSRF token is passed explicitly it is taken from the cookie, so
	 * that supplying nothing but the browser {@code Cookie} header is enough.
	 *
	 * @throws IllegalArgumentException if the cookie is blank
	 */
	public void update(String cookie, String xsrfToken) {
		if (cookie == null || cookie.isBlank()) {
			throw new IllegalArgumentException("Cookie must not be empty");
		}
		String token = xsrfToken == null || xsrfToken.isBlank()
				? cookieValue(cookie, "XSRF-TOKEN")
				: xsrfToken.trim();
		current.set(new Credentials(cookie.trim(), token, Instant.now()));
		warnAboutMissingCookies(cookie);
	}

	/** Names the cookies the endpoint needs that the given header does not carry. */
	public static List<String> missingExpectedCookies(String cookie) {
		List<String> missing = new ArrayList<>();
		for (String name : EXPECTED_COOKIES) {
			if (cookieValue(cookie, name) == null) {
				missing.add(name);
			}
		}
		return missing;
	}

	/**
	 * Reads a single cookie out of a {@code Cookie} header value.
	 *
	 * @return the value, or {@code null} if the cookie is not present
	 */
	public static String cookieValue(String cookie, String name) {
		if (cookie == null) {
			return null;
		}
		for (String part : cookie.split(";")) {
			String trimmed = part.trim();
			int eq = trimmed.indexOf('=');
			if (eq > 0 && trimmed.substring(0, eq).trim().equals(name)) {
				String value = trimmed.substring(eq + 1).trim();
				return value.isEmpty() ? null : value;
			}
		}
		return null;
	}

	private static void warnAboutMissingCookies(String cookie) {
		List<String> missing = missingExpectedCookies(cookie);
		if (!missing.isEmpty()) {
			log.warn("Stored credentials do not contain the cookie(s) {}. "
					+ "Requests may be rejected with 401 - '__VCAP_ID__' in particular pins the request to the "
					+ "Cloud Foundry instance holding the session, and without it failures are intermittent. "
					+ "Copy the complete 'Cookie' header from the browser request.", missing);
		}
	}

	public void clear() {
		current.set(null);
	}

	public Optional<Credentials> get() {
		return Optional.ofNullable(current.get());
	}

	public boolean isPresent() {
		return current.get() != null;
	}
}
