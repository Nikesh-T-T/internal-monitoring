package customer.internal_monitoring.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Extracts request headers from a browser "Copy as cURL" command.
 *
 * <p>Refreshing the session means copying a new cURL command out of the browser
 * developer tools; parsing it here spares the operator from picking the header
 * values apart by hand.
 */
public final class CurlCommandParser {

	/**
	 * Result of parsing a cURL command.
	 *
	 * @param url     the request URL, or {@code null} if none was found
	 * @param headers headers keyed by their original name
	 */
	public record ParsedCurl(String url, Map<String, String> headers) {

		public String cookie() {
			return headerIgnoreCase("Cookie");
		}

		public String xsrfToken() {
			String token = headerIgnoreCase("X-XSRF-TOKEN");
			return token != null ? token : cookieValue("XSRF-TOKEN");
		}

		/** Looks up a header irrespective of its capitalisation. */
		public String headerIgnoreCase(String name) {
			for (Map.Entry<String, String> entry : headers.entrySet()) {
				if (entry.getKey().equalsIgnoreCase(name)) {
					return entry.getValue();
				}
			}
			return null;
		}

		/** Reads a single cookie out of the {@code Cookie} header. */
		public String cookieValue(String cookieName) {
			return ApiCredentialStore.cookieValue(cookie(), cookieName);
		}
	}

	private CurlCommandParser() {
	}

	/**
	 * Parses a cURL command into its URL and headers.
	 *
	 * @throws IllegalArgumentException if the command is blank or contains an
	 *                                  unterminated quote
	 */
	public static ParsedCurl parse(String curlCommand) {
		if (curlCommand == null || curlCommand.isBlank()) {
			throw new IllegalArgumentException("cURL command must not be empty");
		}
		List<String> tokens = tokenize(curlCommand);
		String url = null;
		Map<String, String> headers = new LinkedHashMap<>();

		for (int i = 0; i < tokens.size(); i++) {
			String token = tokens.get(i);
			switch (token) {
				case "-H", "--header" -> {
					if (i + 1 < tokens.size()) {
						addHeader(headers, tokens.get(++i));
					}
				}
				case "-b", "--cookie" -> {
					if (i + 1 < tokens.size()) {
						headers.put("Cookie", tokens.get(++i));
					}
				}
				// Flags that carry a value we do not need, but must not be read as the URL.
				case "-X", "--request", "-d", "--data", "--data-raw", "-A", "--user-agent", "-e", "--referer",
						"-u", "--user", "--max-time", "--connect-timeout", "-o", "--output" -> i++;
				default -> {
					if (url == null && !token.startsWith("-") && looksLikeUrl(token)) {
						url = token;
					}
				}
			}
		}
		return new ParsedCurl(url, headers);
	}

	private static void addHeader(Map<String, String> headers, String rawHeader) {
		int colon = rawHeader.indexOf(':');
		if (colon <= 0) {
			return;
		}
		String name = rawHeader.substring(0, colon).trim();
		String value = rawHeader.substring(colon + 1).trim();
		if (!name.isEmpty()) {
			headers.put(name, value);
		}
	}

	private static boolean looksLikeUrl(String token) {
		String lower = token.toLowerCase(Locale.ROOT);
		return lower.startsWith("http://") || lower.startsWith("https://");
	}

	/**
	 * Splits the command into shell-like tokens, honouring single quotes, double
	 * quotes, backslash escapes and line continuations.
	 */
	private static List<String> tokenize(String command) {
		List<String> tokens = new ArrayList<>();
		StringBuilder current = new StringBuilder();
		boolean inToken = false;
		char quote = 0;

		for (int i = 0; i < command.length(); i++) {
			char c = command.charAt(i);

			if (quote != 0) {
				if (c == quote) {
					quote = 0;
				} else if (c == '\\' && quote == '"' && i + 1 < command.length()) {
					current.append(command.charAt(++i));
				} else {
					current.append(c);
				}
				continue;
			}

			if (c == '\'' || c == '"') {
				quote = c;
				inToken = true;
			} else if (c == '\\' && i + 1 < command.length() && command.charAt(i + 1) == '\n') {
				i++;
			} else if (c == '\\' && i + 1 < command.length()) {
				current.append(command.charAt(++i));
				inToken = true;
			} else if (Character.isWhitespace(c)) {
				if (inToken) {
					tokens.add(current.toString());
					current.setLength(0);
					inToken = false;
				}
			} else {
				current.append(c);
				inToken = true;
			}
		}

		if (quote != 0) {
			throw new IllegalArgumentException("cURL command contains an unterminated quote");
		}
		if (inToken) {
			tokens.add(current.toString());
		}
		return tokens;
	}
}
