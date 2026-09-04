package customer.internal_monitoring.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import customer.internal_monitoring.ingest.IngestionState.IngestionOutcome;

/**
 * The rejection flag drives the dashboard dialog that asks for a fresh browser
 * session, so it has to be set and cleared at exactly the right moments.
 */
class IngestionStateTest {

	private static final IngestionOutcome OUTCOME = new IngestionOutcome(10, 4, 6, 0, 25L);

	@Test
	void startsWithoutARejection() {
		assertThat(new IngestionState().isCredentialsRejected()).isFalse();
	}

	@Test
	void ordinaryFailuresDoNotAskForNewCredentials() {
		IngestionState state = new IngestionState();

		state.recordFailure("connection reset");

		assertThat(state.isCredentialsRejected()).isFalse();
		assertThat(state.getConsecutiveFailures()).isEqualTo(1);
		assertThat(state.getLastError()).isEqualTo("connection reset");
	}

	@Test
	void recordsARejectionAsAFailureAsWell() {
		IngestionState state = new IngestionState();

		state.recordAuthenticationFailure("HTTP 401");

		assertThat(state.isCredentialsRejected()).isTrue();
		assertThat(state.getConsecutiveFailures()).isEqualTo(1);
		assertThat(state.getLastError()).isEqualTo("HTTP 401");
	}

	@Test
	void aSuccessfulRunClearsTheRejection() {
		IngestionState state = new IngestionState();
		state.recordAuthenticationFailure("HTTP 401");

		state.recordSuccess(OUTCOME);

		assertThat(state.isCredentialsRejected()).isFalse();
		assertThat(state.getConsecutiveFailures()).isZero();
		assertThat(state.getLastError()).isNull();
	}

	@Test
	void freshCredentialsClearTheRejectionBeforeTheNextRun() {
		// Otherwise the dialog would reappear between saving and the next poll.
		IngestionState state = new IngestionState();
		state.recordAuthenticationFailure("HTTP 401");

		state.clearAuthenticationFailure();

		assertThat(state.isCredentialsRejected()).isFalse();
	}
}
