using {internal.monitoring as db} from '../db/schema';

/**
 * Read-only monitoring service for Kafka messages plus the administrative
 * actions needed to keep ingestion running.
 */
service MonitoringService {

    @readonly
    entity Messages        as projection on db.KafkaMessages;

    @readonly
    entity ServiceOverview as projection on db.ServiceOverview;

    /**
     * Live ingestion state. Not persisted - served from the running poller.
     */
    @readonly
    @cds.persistence.skip
    entity IngestionStatus {
        key ID                  : String(16);
            /** Whether the scheduled poller is enabled. */
            enabled             : Boolean;
            /** Whether API credentials are currently configured. */
            credentialsPresent  : Boolean;
            /**
             * Whether the API rejected the configured credentials, i.e. the
             * browser session expired. The dashboard asks for a new one.
             */
            credentialsExpired  : Boolean;
            /** Whether a poll is running right now. */
            running             : Boolean;
            pollIntervalSeconds : Integer;
            retention           : String(32);
            endpoint            : String(1024);
            lastRunAt           : Timestamp;
            lastSuccessAt       : Timestamp;
            lastDurationMillis  : Int64;
            lastFetchedCount    : Integer;
            lastStoredCount     : Integer;
            lastDuplicateCount  : Integer;
            lastFailedCount     : Integer;
            totalStoredCount    : Int64;
            skippedRunCount     : Int64;
            consecutiveFailures : Integer;
            lastError           : String(1024);
            storedMessageCount  : Int64;
    }

    type IngestionResult {
        fetched    : Integer;
        stored     : Integer;
        duplicates : Integer;
        failed     : Integer;
        durationMillis : Int64;
    }

    /**
     * Replaces the API credentials held in memory. Nothing is persisted.
     * `cookie` is the raw value of the `Cookie` request header.
     */
    action setCredentials(cookie : String  @mandatory, xsrfToken : String) returns String;

    /**
     * Convenience variant of `setCredentials`: accepts a browser
     * "Copy as cURL" command and extracts the authentication headers from it.
     */
    action importCredentialsFromCurl(curlCommand : LargeString @mandatory) returns String;

    /** Clears the in-memory credentials and stops further polling attempts. */
    action clearCredentials()                                              returns String;

    /** Runs one ingestion cycle immediately and reports what it did. */
    action triggerIngestion()                                              returns IngestionResult;

    /** Deletes messages older than the configured retention period. */
    action purgeExpiredMessages()                                          returns Integer;
}
