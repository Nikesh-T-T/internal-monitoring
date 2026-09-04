package customer.internal_monitoring.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import customer.internal_monitoring.client.CurlCommandParser.ParsedCurl;

class CurlCommandParserTest {

	/** Shape of a "Copy as cURL" command taken from browser developer tools. */
	private static final String BROWSER_CURL = """
			curl 'https://example.test/v2/usecase/space/kafka/topic/messages' \\
			  --compressed \\
			  -H 'Accept: application/json;charset=UTF-8' \\
			  -H 'Cookie: JSESSIONID=ABC123; JTENANTSESSIONID_a060375d6=tok%3D; XSRF-TOKEN=xsrf-42'
			""";

	@Test
	void extractsUrlAndHeaders() {
		ParsedCurl parsed = CurlCommandParser.parse(BROWSER_CURL);

		assertThat(parsed.url()).isEqualTo("https://example.test/v2/usecase/space/kafka/topic/messages");
		assertThat(parsed.headerIgnoreCase("accept")).isEqualTo("application/json;charset=UTF-8");
		assertThat(parsed.cookie()).contains("JSESSIONID=ABC123");
	}

	@Test
	void derivesTheXsrfTokenFromTheCookieWhenNoHeaderIsPresent() {
		assertThat(CurlCommandParser.parse(BROWSER_CURL).xsrfToken()).isEqualTo("xsrf-42");
	}

	@Test
	void prefersAnExplicitXsrfHeader() {
		String curl = "curl 'https://example.test/m' -H 'Cookie: XSRF-TOKEN=from-cookie' "
				+ "-H 'X-XSRF-TOKEN: from-header'";

		assertThat(CurlCommandParser.parse(curl).xsrfToken()).isEqualTo("from-header");
	}

	@Test
	void supportsTheCookieFlag() {
		ParsedCurl parsed = CurlCommandParser.parse("curl https://example.test/m -b 'JSESSIONID=XYZ'");

		assertThat(parsed.cookie()).isEqualTo("JSESSIONID=XYZ");
	}

	@Test
	void doesNotMistakeAFlagValueForTheUrl() {
		ParsedCurl parsed = CurlCommandParser.parse(
				"curl -X GET --data-raw 'https://not-the-url.test' 'https://example.test/m' -H 'Cookie: a=b'");

		assertThat(parsed.url()).isEqualTo("https://example.test/m");
	}

	@Test
	void handlesDoubleQuotesAndEscapes() {
		ParsedCurl parsed = CurlCommandParser.parse(
				"curl \"https://example.test/m\" -H \"Cookie: name=\\\"quoted\\\"\"");

		assertThat(parsed.url()).isEqualTo("https://example.test/m");
		assertThat(parsed.cookie()).isEqualTo("name=\"quoted\"");
	}

	@Test
	void returnsNullWhenNoCookieIsPresent() {
		ParsedCurl parsed = CurlCommandParser.parse("curl https://example.test/m -H 'Accept: application/json'");

		assertThat(parsed.cookie()).isNull();
		assertThat(parsed.xsrfToken()).isNull();
	}

	@Test
	void rejectsBlankInput() {
		assertThatThrownBy(() -> CurlCommandParser.parse("  "))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("must not be empty");
	}

	@Test
	void rejectsAnUnterminatedQuote() {
		assertThatThrownBy(() -> CurlCommandParser.parse("curl 'https://example.test/m"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("unterminated quote");
	}
}
