package customer.internal_monitoring.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MessageHashesTest {

	@Test
	void isStableForIdenticalInput() {
		assertThat(MessageHashes.messageHash(1, 2L, "payload"))
				.isEqualTo(MessageHashes.messageHash(1, 2L, "payload"));
	}

	@Test
	void changesWithThePayload() {
		assertThat(MessageHashes.messageHash(1, 2L, "a"))
				.isNotEqualTo(MessageHashes.messageHash(1, 2L, "b"));
	}

	@Test
	void separatesRecordsWithIdenticalPayloadsAtDifferentOffsets() {
		assertThat(MessageHashes.messageHash(1, 2L, "same"))
				.isNotEqualTo(MessageHashes.messageHash(1, 3L, "same"));
		assertThat(MessageHashes.messageHash(1, 2L, "same"))
				.isNotEqualTo(MessageHashes.messageHash(2, 2L, "same"));
	}

	@Test
	void payloadHashIgnoresKafkaCoordinates() {
		assertThat(MessageHashes.payloadHash("same")).isEqualTo(MessageHashes.payloadHash("same"));
		assertThat(MessageHashes.payloadHash("a")).isNotEqualTo(MessageHashes.payloadHash("b"));
	}

	@Test
	void producesHexDigestsThatFitTheColumn() {
		assertThat(MessageHashes.messageHash(1, 2L, "payload")).hasSize(64).matches("[0-9a-f]{64}");
		assertThat(MessageHashes.payloadHash(null)).hasSize(64);
	}

	@Test
	void doesNotConfuseFieldBoundaries() {
		// "1|23" and "12|3" must not collide once concatenated.
		assertThat(MessageHashes.messageHash(1, 23L, "x"))
				.isNotEqualTo(MessageHashes.messageHash(12, 3L, "x"));
	}
}
