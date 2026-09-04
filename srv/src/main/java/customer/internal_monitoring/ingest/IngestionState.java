package customer.internal_monitoring.ingest;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.stereotype.Component;

/**
 * Live view on the ingestion loop, surfaced through the {@code IngestionStatus}
 * entity so the dashboard can show whether data is still flowing in.
 */
@Component
public class IngestionState {

	/** Counters of a single ingestion run. */
	public record IngestionOutcome(int fetched, int stored, int duplicates, int failed, long durationMillis) {
	}

	private final AtomicReference<Instant> lastRunAt = new AtomicReference<>();
	private final AtomicReference<Instant> lastSuccessAt = new AtomicReference<>();
	private final AtomicReference<String> lastError = new AtomicReference<>();
	private final AtomicReference<IngestionOutcome> lastOutcome = new AtomicReference<>();
	private final AtomicLong totalStored = new AtomicLong();
	private final AtomicLong skippedRuns = new AtomicLong();
	private final AtomicInteger consecutiveFailures = new AtomicInteger();
	private final AtomicBoolean credentialsRejected = new AtomicBoolean();

	public void recordSuccess(IngestionOutcome outcome) {
		Instant now = Instant.now();
		lastRunAt.set(now);
		lastSuccessAt.set(now);
		lastOutcome.set(outcome);
		lastError.set(null);
		totalStored.addAndGet(outcome.stored());
		consecutiveFailures.set(0);
		credentialsRejected.set(false);
	}

	public void recordFailure(String message) {
		lastRunAt.set(Instant.now());
		lastError.set(message);
		consecutiveFailures.incrementAndGet();
	}

	/**
	 * Records that the API refused the stored credentials.
	 *
	 * <p>Kept apart from an ordinary failure because it is the one condition an
	 * operator can fix, and the dashboard uses it to ask for a fresh session.
	 */
	public void recordAuthenticationFailure(String message) {
		recordFailure(message);
		credentialsRejected.set(true);
	}

	/** Resets the rejection flag when fresh credentials arrive. */
	public void clearAuthenticationFailure() {
		credentialsRejected.set(false);
	}

	/** True when the last attempt failed because the session was not accepted. */
	public boolean isCredentialsRejected() {
		return credentialsRejected.get();
	}

	public void recordSkippedRun() {
		skippedRuns.incrementAndGet();
	}

	public Instant getLastRunAt() {
		return lastRunAt.get();
	}

	public Instant getLastSuccessAt() {
		return lastSuccessAt.get();
	}

	public String getLastError() {
		return lastError.get();
	}

	public IngestionOutcome getLastOutcome() {
		return lastOutcome.get();
	}

	public long getTotalStored() {
		return totalStored.get();
	}

	public long getSkippedRuns() {
		return skippedRuns.get();
	}

	public int getConsecutiveFailures() {
		return consecutiveFailures.get();
	}
}
