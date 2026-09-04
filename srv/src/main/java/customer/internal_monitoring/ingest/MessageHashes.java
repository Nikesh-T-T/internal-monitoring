package customer.internal_monitoring.ingest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SHA-256 helpers used to recognise messages that were already ingested.
 */
public final class MessageHashes {

	private MessageHashes() {
	}

	/**
	 * Hash over the payload only. Two records with the same value carry
	 * structurally identical content, even if they sit at different offsets.
	 */
	public static String payloadHash(String payload) {
		return sha256(payload == null ? "" : payload);
	}

	/**
	 * Deduplication key.
	 *
	 * <p>The Kafka coordinates are folded into the hash on purpose. Polling
	 * returns overlapping windows of the topic, so the same record is seen over
	 * and over and must be stored once. Two <em>different</em> records may still
	 * carry byte-identical payloads, and hashing the payload alone would silently
	 * discard them.
	 */
	public static String messageHash(Integer partition, Long offset, String payload) {
		return sha256(partition + "|" + offset + "|" + (payload == null ? "" : payload));
	}

	private static String sha256(String value) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is not available in this JVM", e);
		}
	}
}
