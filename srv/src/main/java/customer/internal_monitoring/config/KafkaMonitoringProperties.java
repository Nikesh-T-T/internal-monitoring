package customer.internal_monitoring.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration of the Kafka message monitoring ingestion.
 *
 * <p>Credentials are deliberately <em>not</em> part of this configuration. The
 * upstream API is protected by a browser session that expires regularly, so
 * credentials are supplied at runtime through the {@code setCredentials} action
 * and are kept in memory only.
 */
@ConfigurationProperties(prefix = "monitoring.kafka")
public class KafkaMonitoringProperties {

	/** Absolute URL of the Kafka messages endpoint. */
	private String endpoint;

	/** Logical topic name stored with every message. */
	private String topic;

	/** Enables the scheduled poller. */
	private boolean enabled = true;

	/** Delay between the end of one poll and the start of the next one. */
	private Duration pollInterval = Duration.ofSeconds(5);

	/** Delay before the first poll, giving the application time to start up. */
	private Duration initialDelay = Duration.ofSeconds(15);

	/** Timeout for a single API call. */
	private Duration requestTimeout = Duration.ofSeconds(120);

	/** Timeout for establishing the connection to the API. */
	private Duration connectTimeout = Duration.ofSeconds(15);

	/** Messages older than this, measured by ingestion time, are deleted. */
	private Duration retention = Duration.ofHours(1);

	/** Interval between retention runs. */
	private Duration retentionInterval = Duration.ofMinutes(5);

	/** Enables the scheduled retention job. */
	private boolean retentionEnabled = true;

	/** Number of rows written per database round trip. */
	private int batchSize = 200;

	/** Value of the {@code Accept} header sent to the API. */
	private String accept = "application/json;charset=UTF-8";

	/** Additional static request headers, for example a proxy or API key header. */
	private Map<String, String> headers = new LinkedHashMap<>();

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public Duration getPollInterval() {
		return pollInterval;
	}

	public void setPollInterval(Duration pollInterval) {
		this.pollInterval = pollInterval;
	}

	public Duration getInitialDelay() {
		return initialDelay;
	}

	public void setInitialDelay(Duration initialDelay) {
		this.initialDelay = initialDelay;
	}

	public Duration getRequestTimeout() {
		return requestTimeout;
	}

	public void setRequestTimeout(Duration requestTimeout) {
		this.requestTimeout = requestTimeout;
	}

	public Duration getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(Duration connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public Duration getRetention() {
		return retention;
	}

	public void setRetention(Duration retention) {
		this.retention = retention;
	}

	public Duration getRetentionInterval() {
		return retentionInterval;
	}

	public void setRetentionInterval(Duration retentionInterval) {
		this.retentionInterval = retentionInterval;
	}

	public boolean isRetentionEnabled() {
		return retentionEnabled;
	}

	public void setRetentionEnabled(boolean retentionEnabled) {
		this.retentionEnabled = retentionEnabled;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public String getAccept() {
		return accept;
	}

	public void setAccept(String accept) {
		this.accept = accept;
	}

	public Map<String, String> getHeaders() {
		return headers;
	}

	public void setHeaders(Map<String, String> headers) {
		this.headers = headers == null ? new LinkedHashMap<>() : headers;
	}
}
