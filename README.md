# Internal Monitoring - Kafka Message Dashboard

An SAP CAP Java application that polls the internal monitoring Kafka messages
API, stores every message it has not seen before in H2, and exposes the result
as an OData V4 service with a Fiori elements dashboard.

Each stored row keeps the **original message payload** together with the
**service name** taken from the payload, plus the metadata the dashboard filters
and aggregates on.

## Running it

```bash
mvn clean install
java -jar srv/target/internal-monitoring-exec.jar
```

Then open:

| What | URL |
| --- | --- |
| Dashboard | <http://localhost:8080/monitoring/index.html> |
| Service overview page | <http://localhost:8080/monitoring/index.html#/ServiceOverview> |
| OData service | <http://localhost:8080/odata/v4/MonitoringService/> |
| Ingestion status | <http://localhost:8080/odata/v4/MonitoringService/IngestionStatus> |

The dashboard loads UI5 from the public CDN; point the bootstrap `src` in
`srv/src/main/resources/static/monitoring/index.html` at a local UI5
distribution if the machine has no internet access.

## Supplying credentials

The upstream API is protected by a **browser session that expires**, so no
credential is stored in the repository or in a configuration file. Polling stays
idle until credentials are handed to the running application, and they live in
memory only.

### From the dashboard

The dashboard asks for them by itself. A dialog opens when the application has
no session yet, and again as soon as the API rejects the stored one, so an
expired session shows up as a prompt rather than as an empty list. Paste the
complete value of the `Cookie` request header - copy it from the Kafka messages
request in the browser developer tools (*Network* tab) - and choose **Save**.
Polling resumes with the next cycle.

The **Set Session Cookie** button in the message list opens the same dialog at
any time, for instance to replace a session before it runs out. Closing the
dialog without saving keeps it away for two minutes so it cannot nag.

### From the command line

The quickest way is to copy the request out of the browser developer tools
(*Network* > right click the request > *Copy* > *Copy as cURL*) and paste it:

```bash
curl -X POST http://localhost:8080/odata/v4/MonitoringService/importCredentialsFromCurl \
  -H 'Content-Type: application/json' \
  -d '{"curlCommand": "curl \"https://...\" -H \"Cookie: JSESSIONID=...\""}'
```

The `Cookie` header is taken from the command, and `X-XSRF-TOKEN` is used when
the command carries one (either as a header or as an `XSRF-TOKEN` cookie).

The equivalent explicit call:

```bash
curl -X POST http://localhost:8080/odata/v4/MonitoringService/setCredentials \
  -H 'Content-Type: application/json' \
  -d '{"cookie": "JSESSIONID=...; JTENANTSESSIONID_...=...", "xsrfToken": "..."}'
```

When the session expires the API answers `401`. The poller keeps running, records
the problem in `IngestionStatus.lastError`, raises `IngestionStatus.credentialsExpired`
- which is what makes the dashboard dialog reappear - and reports it in the log
at most once every 20 attempts. Ingestion resumes as soon as fresh credentials
arrive, no restart needed.

`xsrfToken` is optional in both actions: when it is omitted the `XSRF-TOKEN`
cookie is used, so the plain `Cookie` header the dialog collects is enough.

> **Copy the whole `Cookie` header.** The endpoint needs two cookies, verified by
> removing them one at a time against a live session:
>
> - `JSESSIONID` - the session itself.
> - `__VCAP_ID__` - pins the request to the Cloud Foundry instance that holds
>   that session. Without it the platform load balancer picks an instance at
>   random, so roughly half the requests fail with `401` while the rest succeed.
>   An intermittent failure like that is easy to misread as an expired session.
>
> `JTENANTSESSIONID_*`, `XSRF-TOKEN`, the Adobe analytics cookies and even the
> `Accept` header turned out **not** to be required, but there is no reason to
> strip them. The dialog and both actions warn - in the log and on screen - when
> a required cookie is absent.

## Actions

| Action | Purpose |
| --- | --- |
| `setCredentials(cookie, xsrfToken)` | Replace the in-memory credentials |
| `importCredentialsFromCurl(curlCommand)` | Same, from a "Copy as cURL" command |
| `clearCredentials()` | Forget the credentials and pause polling |
| `triggerIngestion()` | Run one cycle now and return the counters |
| `purgeExpiredMessages()` | Apply the retention period immediately |

## Configuration

All settings live under `monitoring.kafka` in
`srv/src/main/resources/application.yaml`:

| Property | Default | Meaning |
| --- | --- | --- |
| `endpoint` | the CALM dev topic | Absolute URL of the messages API |
| `topic` | `ms.calm-dev-eu10-004.internal-monitoring` | Stored with every message |
| `enabled` | `true` | Enables the scheduled poller |
| `poll-interval` | `5s` | Delay between the end of one poll and the next |
| `initial-delay` | `15s` | Delay before the first poll |
| `request-timeout` | `120s` | Timeout of one API call |
| `connect-timeout` | `15s` | Connection timeout |
| `retention` | `1h` | Messages older than this are deleted |
| `retention-interval` | `5m` | How often retention runs |
| `retention-enabled` | `true` | Enables the retention job |
| `batch-size` | `200` | Rows per database round trip |
| `accept` | `application/json;charset=UTF-8` | `Accept` header |
| `headers` | empty | Extra static request headers |

### Volume and memory

A single poll of the development topic returns about **1500 records, roughly
16 MB of JSON** (near 1 MB on the wire, since the API serves gzip). Polling every
five seconds keeps re-reading an overlapping window of the topic: in a live run
each poll returned the same 1488 records and only a few dozen were new. Only
messages that were not seen before are written, so the database grows with the
rate at which the topic produces new messages, not with the polling frequency.

The default database is **in-memory H2**, so retention is what keeps the heap
bounded - raise `retention` only together with the JVM heap. To keep the history
across restarts, use the bundled profile, which stores the data in a file:

```bash
java -jar srv/target/internal-monitoring-exec.jar --spring.profiles.active=persistent
```

## Data model

`internal.monitoring.KafkaMessages` (`db/schema.cds`):

| Element | Notes |
| --- | --- |
| `ID` | Generated UUID, the unique identifier |
| `serviceName` | From the payload, see below |
| `payload` | The original message JSON, unmodified |
| `properties` | The record's `properties` array as JSON |
| `messageHash` | Deduplication key, unique |
| `payloadHash` | Hash of the payload alone |
| `messageTimestamp`, `kafkaPartition`, `kafkaOffset`, `payloadSize`, `headersSize` | Kafka record metadata |
| `correlationId`, `messageId`, `sourceId`, `eventType`, `messageType`, `topic` | Message metadata |
| `tenantId`, `useCaseName`, `serviceType`, `calmAction`, `agentVersion` | From the record properties |
| `truncated`, `parseStatus` | Payload health, see below |
| `ingestedAt` | Drives retention |

`ServiceOverview` aggregates the table per service (message count, first and last
message, total payload size).

### How the service name is determined

Each record carries its message as an *escaped JSON string* in `payload`. That
string is parsed a second time and the first `resource.attributes` block is read:

1. `sap.service.display_name` - the intended value.
2. `service.name` - fallback; 70 of 1488 sample records had no display name.
3. `cloudfoundry.app.name` - second fallback.
4. `UNKNOWN` - only if none of the above is readable.

### Truncated payloads

The API truncates large payloads at about 100 kB (31 of 1488 sample records),
which leaves the payload as **invalid JSON**. Those messages are still stored:
the attribute block sits at the front of the payload, so parsing stops as soon as
the attributes have been read and tolerates the failure that follows. Such rows
get `truncated = true` and `parseStatus = TRUNCATED`; `payloadSize` shows the
size the API reported, which is larger than what was delivered.

`parseStatus` values: `OK`, `TRUNCATED`, `INVALID` (no attributes readable).

### Deduplication

`messageHash` is `SHA-256(partition | offset | payload)` and carries a unique
index.

The Kafka coordinates are part of the hash deliberately. Polling returns
overlapping windows, so the same record is fetched over and over and must be
stored once - but two *different* records can carry byte-identical payloads, and
hashing the payload alone would silently drop them. `payloadHash` is kept as a
separate, non-unique column to find structurally identical messages.

Each run first checks which hashes are already stored and inserts only the rest.
Runs are serialised: a scheduled poll that finds another run in progress is
skipped and counted in `IngestionStatus.skippedRunCount`.

## Tests

```bash
mvn test
```

Covers payload parsing (including truncated payloads and the service-name
fallbacks), hashing, the cURL parser, the credential store, the HTTP client
(headers, gzip, `401` and error handling) and an end-to-end ingestion test that
stores, deduplicates and purges against a stubbed API.

The application has also been run against the live endpoint: 1488 records per
poll, 2.6 s for a cold ingestion, deduplication reducing a follow-up poll to the
few dozen genuinely new messages, 90 distinct services, no record left without a
service name, and the truncated payloads stored with their attributes intact.

The dashboard was driven in a real (headless) browser to confirm that the
credential dialog opens on its own when no session is configured, that the
toolbar button reopens it, that an empty value is rejected without a request,
that saving stores the session and surfaces the missing-cookie warning, and that
an expired session brings the dialog back with the matching explanation.
