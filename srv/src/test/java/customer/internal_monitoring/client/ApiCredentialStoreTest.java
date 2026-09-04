package customer.internal_monitoring.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ApiCredentialStoreTest {

	@Test
	void startsWithoutCredentials() {
		ApiCredentialStore store = new ApiCredentialStore();

		assertThat(store.isPresent()).isFalse();
		assertThat(store.get()).isEmpty();
	}

	@Test
	void storesAndReplacesCredentials() {
		ApiCredentialStore store = new ApiCredentialStore();

		store.update("JSESSIONID=one", "token-1");
		assertThat(store.get()).get().extracting(ApiCredentialStore.Credentials::cookie).isEqualTo("JSESSIONID=one");

		store.update("JSESSIONID=two", "token-2");
		assertThat(store.get()).get().extracting(ApiCredentialStore.Credentials::xsrfToken).isEqualTo("token-2");
	}

	@Test
	void trimsInputAndTreatsBlankTokenAsAbsent() {
		ApiCredentialStore store = new ApiCredentialStore();

		store.update("  JSESSIONID=one  ", "   ");

		ApiCredentialStore.Credentials credentials = store.get().orElseThrow();
		assertThat(credentials.cookie()).isEqualTo("JSESSIONID=one");
		assertThat(credentials.xsrfToken()).isNull();
		assertThat(credentials.updatedAt()).isNotNull();
	}

	@Test
	void rejectsABlankCookie() {
		ApiCredentialStore store = new ApiCredentialStore();

		assertThatThrownBy(() -> store.update("   ", "token"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Cookie");
		assertThatThrownBy(() -> store.update(null, "token"))
				.isInstanceOf(IllegalArgumentException.class);
		assertThat(store.isPresent()).isFalse();
	}

	@Test
	void clearsCredentials() {
		ApiCredentialStore store = new ApiCredentialStore();
		store.update("JSESSIONID=one", null);

		store.clear();

		assertThat(store.isPresent()).isFalse();
	}

	@Test
	void derivesTheXsrfTokenFromTheCookieWhenNoneIsPassed() {
		// The dashboard dialog asks for nothing but the Cookie header, so the
		// token has to be picked out of it.
		ApiCredentialStore store = new ApiCredentialStore();
		store.update("JSESSIONID=abc; __VCAP_ID__=def; XSRF-TOKEN=token-from-cookie", null);

		assertThat(store.get()).get().extracting(ApiCredentialStore.Credentials::xsrfToken)
				.isEqualTo("token-from-cookie");
	}

	@Test
	void prefersAnExplicitlyPassedXsrfToken() {
		ApiCredentialStore store = new ApiCredentialStore();
		store.update("JSESSIONID=abc; XSRF-TOKEN=from-cookie", "explicit");

		assertThat(store.get()).get().extracting(ApiCredentialStore.Credentials::xsrfToken)
				.isEqualTo("explicit");
	}

	@Test
	void reportsCookiesTheEndpointNeedsButTheHeaderLacks() {
		// Both present: the endpoint answers reliably.
		assertThat(ApiCredentialStore.missingExpectedCookies(
				"AMCV_x=y; JSESSIONID=abc; __VCAP_ID__=def; XSRF-TOKEN=ghi")).isEmpty();

		// Without __VCAP_ID__ the load balancer may route to an instance that does
		// not hold the session, which shows up as intermittent 401 responses.
		assertThat(ApiCredentialStore.missingExpectedCookies("JSESSIONID=abc"))
				.containsExactly("__VCAP_ID__");
		assertThat(ApiCredentialStore.missingExpectedCookies("__VCAP_ID__=def"))
				.containsExactly("JSESSIONID");
		assertThat(ApiCredentialStore.missingExpectedCookies("AMCV_x=y"))
				.containsExactly("JSESSIONID", "__VCAP_ID__");
	}

	@Test
	void doesNotMistakeACookieWhoseNameMerelyEndsWithAnExpectedOne() {
		assertThat(ApiCredentialStore.missingExpectedCookies("NOT_JSESSIONID=abc; __VCAP_ID_META__=secure"))
				.containsExactly("JSESSIONID", "__VCAP_ID__");
	}
}
