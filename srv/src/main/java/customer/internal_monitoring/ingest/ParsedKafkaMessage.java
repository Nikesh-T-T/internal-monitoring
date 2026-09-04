package customer.internal_monitoring.ingest;

import java.time.Instant;

/**
 * One Kafka record after parsing, ready to be persisted.
 */
public record ParsedKafkaMessage(
		String serviceName,
		String messageHash,
		String payloadHash,
		String correlationId,
		String messageId,
		String conversationId,
		String sourceId,
		String eventType,
		String messageType,
		Instant messageTimestamp,
		Integer kafkaPartition,
		Long kafkaOffset,
		Long payloadSize,
		Integer headersSize,
		boolean truncated,
		String parseStatus,
		String tenantId,
		String useCaseName,
		String serviceType,
		String calmAction,
		String agentVersion,
		String payload,
		String properties,
		String topic) {
}
