-- Indexes for the Kafka message table.
--
-- The unique index on MESSAGEHASH is the safety net behind the application
-- level deduplication: even if two ingestion runs were to overlap, the same
-- Kafka record can never be stored twice. The remaining indexes back the
-- dashboard filters and the retention job.
--
-- The CDS compiler emits the table definition into schema-h2.sql; index
-- definitions are kept here because @sql.append cannot express table
-- constraints for H2.

CREATE UNIQUE INDEX IF NOT EXISTS IDX_KAFKAMESSAGES_MESSAGEHASH
    ON internal_monitoring_KafkaMessages (messageHash);

CREATE INDEX IF NOT EXISTS IDX_KAFKAMESSAGES_SERVICENAME
    ON internal_monitoring_KafkaMessages (serviceName);

CREATE INDEX IF NOT EXISTS IDX_KAFKAMESSAGES_INGESTEDAT
    ON internal_monitoring_KafkaMessages (ingestedAt);

CREATE INDEX IF NOT EXISTS IDX_KAFKAMESSAGES_MESSAGETIMESTAMP
    ON internal_monitoring_KafkaMessages (messageTimestamp);

CREATE INDEX IF NOT EXISTS IDX_KAFKAMESSAGES_PAYLOADHASH
    ON internal_monitoring_KafkaMessages (payloadHash);
