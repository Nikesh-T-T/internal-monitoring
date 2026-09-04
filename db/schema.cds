namespace internal.monitoring;

using {cuid} from '@sap/cds/common';

/**
 * Outcome of parsing the (escaped) JSON payload of a Kafka record.
 */
type ParseStatus : String(16) enum {
    OK;
    TRUNCATED;
    INVALID;
}

/**
 * One Kafka record retrieved from the monitoring API.
 *
 * `payload` keeps the original message JSON exactly as delivered so that no
 * information is lost, while the remaining elements duplicate the parts that
 * the dashboard filters, sorts and aggregates on.
 */
entity KafkaMessages : cuid {
    /**
     * Value of the `sap.service.display_name` resource attribute.
     * Falls back to `service.name` / `cloudfoundry.app.name` and finally to
     * `UNKNOWN` when the attribute is absent.
     */
    serviceName      : String(255) not null;

    /** Deduplication key: SHA-256 over partition, offset and payload. */
    messageHash      : String(64) not null;

    /** SHA-256 over the payload only, to spot structurally identical messages. */
    payloadHash      : String(64) not null;

    correlationId    : String(64);
    messageId        : String(64);

    /** `gen_ai.conversation.id` attribute of the first span that carries it. */
    conversationId   : String(64);

    sourceId         : String(128);
    eventType        : String(64);
    messageType      : String(128);
    topic            : String(255);

    /** Record timestamp reported by the API, converted from epoch millis. */
    messageTimestamp : Timestamp;
    kafkaPartition   : Integer;
    kafkaOffset      : Int64;

    /** Size the API reported for the original payload, in bytes. */
    payloadSize      : Int64;
    headersSize      : Integer;

    /** True when the API delivered less payload than `payloadSize` announces. */
    truncated        : Boolean default false;
    parseStatus      : ParseStatus;

    tenantId         : String(64);
    useCaseName      : String(64);
    serviceType      : String(64);

    /** `sap.calm.action` / `action` property of the record. */
    calmAction       : String(64);

    /** `sap.calm.actionVersion` / `version` property of the record. */
    agentVersion     : String(32);

    /** Original message payload, unmodified. */
    payload          : LargeString;

    /** Original `properties` array of the record, serialised as JSON. */
    properties       : LargeString;

    ingestedAt       : Timestamp not null;
}

/**
 * Per-service roll-up used by the dashboard overview.
 */
view ServiceOverview as
    select from KafkaMessages {
        key serviceName,
            count(ID)               as messageCount    : Integer,
            min(messageTimestamp)   as firstMessageAt  : Timestamp,
            max(messageTimestamp)   as lastMessageAt   : Timestamp,
            sum(payloadSize)        as totalPayloadSize : Int64,
    }
    group by
        serviceName;
