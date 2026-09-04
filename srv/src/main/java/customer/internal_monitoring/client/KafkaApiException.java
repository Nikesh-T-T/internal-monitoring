package customer.internal_monitoring.client;

/**
 * Raised when the Kafka messages API cannot be queried successfully.
 */
public class KafkaApiException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private final int statusCode;

	public KafkaApiException(String message, int statusCode) {
		super(message);
		this.statusCode = statusCode;
	}

	public KafkaApiException(String message, Throwable cause) {
		super(message, cause);
		this.statusCode = -1;
	}

	/** HTTP status code, or {@code -1} when the call failed before a response. */
	public int getStatusCode() {
		return statusCode;
	}

	/** True when the API rejected the supplied credentials. */
	public boolean isAuthenticationFailure() {
		return statusCode == 401 || statusCode == 403;
	}
}
