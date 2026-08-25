# Changelog

## v0.9.9

- Replaced the telemetry realtime/fallback split with one SQLite-first
  `telemetry_outbox`: every telemetry report that passes collection filtering
  is committed before MQTT and deleted only after a successful send.
- Removed the process-memory `AsyncValuesChannel`; drivers now use synchronous
  `ReportAsyncValues`, whose successful return means SQLite commit completed.
- Added independent `telemetryOutbox` configuration and the default
  `./data/telemetry-outbox.db`; the removed `reliableQueue` configuration and
  legacy `reliable_queue` rows are intentionally not accepted or migrated.
- Network recovery now drains the recovery backlog in `time, id` order before
  post-recovery telemetry. Telemetry payloads preserve collection `time` and
  publish `send_at` plus `is_replayed`; pending startup/failure rows are replayed.
- Added configurable 7-day retention, batch size, send-rate limit, and bounded
  retry backoff. The default telemetry MQTT QoS is now 1.
- EVENT payload uses the existing `data.event_code` and `data.type` fields.
- Added required lifecycle fields: `raise` is `phase=start, status=active`,
  `clear` is `phase=end, status=resolved`, and `pulse` is
  `phase=record, status=recorded`.
- EVENT metadata keeps the rule-definition field as `meta.rule_type`, while
  `data.type` carries the lifecycle action.
- Changed EVENT delivery to a write-ahead SQLite `event_outbox`: every event
  is persisted before MQTT delivery and removed only after a successful send.
  The legacy event queue is not migrated.
- Added signal-driven graceful shutdown for driver, EVENT state/outbox,
  telemetry outbox, and MQTT resources.

## v0.9.1

- Added fixed-width `String` property write normalization: short values are
  zero-padded and values over `maxLength` are truncated by byte length.
- Renamed the unreleased optional JetStream configuration from `bus` to
  `natsBus`; its store now defaults to `./data/natsbus`.
- Replaced the top-level Process enable list with `device.processDir` and
  per-device `processNames` bindings.
- Simplified Process YAML so subject lists and telemetry format allowlists are
  unnecessary. Each Process now consumes all fixed SDK subjects for its bound
  devices, always skips its own outputs, accepts other Process outputs, and
  retains timeout, concurrency, and hop-limit safeguards.
- Added the protocol-independent `event` engine with validated EVENT YAML profiles, direct telemetry/property references, standard connect events, exclusive OEE states, aggregate alarm/fault events, pulse/rise-clear actions, hold/recover debounce, and summary windows.
- Added `device.eventDir`, `device.eventProfile`, and the optional global `eventReport` MQTT topic while preserving old configurations that omit them.
- Added an independent SQLite-backed event outbox with replay metadata, event-time preservation, state-file recovery, and safe shutdown draining.
- Added `runtime/event` integration and an optional timestamp-aware driver status reporter so protocol services can provide collection/processing time without changing the existing reporter interface.

## v0.8.8

- Added **multi-MQTT broker support** with group-based architecture: top-level parallel groups with per-group failover broker chains (`transport/mqtt/multi.go`). Each group supports per-broker `clientId`, `username`, `password` config, per-group `dataFormat` overrides, and per-group `heartbeatInterval`.
- Added `MQTTGroupConfig` to `types.go` with `Name`, `TelemetryFormat`, `StatusReportFormat`, `HeartbeatInterval`, `Brokers` fields.
- Added `MultiGroupPublisher` interface with `GroupPublishers()` and `GroupStatusTopic(i)` methods.
- Added `NewPublisher()` factory that auto-detects groups vs single-broker mode for full backward compatibility.
- Added `PublishJSON` to `Publisher` interface for MQTT query transport.
- Added `StartHeartbeatOnly()` to `deviceStatusPublisher` for per-group independent heartbeat goroutines (`runtime/app/status_report.go`).
- Added **runtime ops services**: `configsvc.ConfigService` (dynamic config read/write, diff, backup, restore, hot-reload callbacks), `logsvc.LogSearcher` (paginated log browsing with filtering), `ops.Restarter` (service restart lifecycle).
- Added **service port auto-assign**: `service.port: 0` now auto-binds to an OS-picked free port instead of disabling the server.
- Added **service port range search**: `service.portEnd` enables trying ports in `[port, portEnd]` range, using the first available.
- Changed `Server.Run()` to use `net.Listen` + `Serve` for actual port capture; runtime status API now reports the actual bound port.
- Added `bitMerge bool` to `TelemetryConfig` and `PropertyConfig`, propagated through `mergeDeviceWithProfile`.
- Enhanced `convertToCompactFormat` to include `trace_id`, `time`, `send_at` fields.
- Added ClientId randomization: `sdk-{10-digit}` when unset, `{custom}-{6-digit}` when configured in `lifecycle.go`.
- Added `FileClient` to `CommandContext` interface for presigned URL file transfer.

- Added `SchemaField` type to `command` package for structured input/output schema definitions.
- Added `Enable bool`, `InputSchema []SchemaField`, `OutputSchema []SchemaField` fields to `CommandDescriptor`.
- Updated `normalizeDescriptor` and `cloneDescriptor` to handle new `SchemaField` fields.
- Added `validateInputSchema` in `runtime/command` service for required field validation alongside `validateInputParams`.

## v0.7.5

- Added `kind: struct` support for standalone structs (no index) with multi-field nested structures.
- Added recursive `PropertyStructField` with `Kind`, `Fields`, `MaxItems`, `IndexStride` for nesting struct/array sub-types (max 2 array levels).
- Added `IsScalar()`, `IsStruct()`, `IsArray()` helper methods on `PropertyStructField`.
- Unified `struct_array` and `array` (multi-field) into a single code path; `struct_array` retains full backward compatibility.
- Added cumulative offset address calculation via `structFieldNodeNameWithOffset` for nested struct/array fields.
- Added `flattenStructFields` with depth limit (2 array levels) for config-driven field expansion.
- Rewrote `buildStructWriteRequests` as input-driven recursive traversal — only fields present in the input generate PLC write operations.
- Added `buildStructWriteFields`, `buildNestedArrayWrite` for recursive write handling.
- Rewrote `buildStructReadRequests` to support `kind: struct` and nested fields.
- Added `buildStructReadFields`, `buildNestedArrayRead` for recursive read handling.
- Added `buildStructFieldSelection` for recursive selection map generation.
- Updated `BuildAutoPropertyReadRequests` to support `kind: struct`.
- Added `Structs []PropertyStruct` to `TelemetryConfig` and `TelemetryGroup` for telemetry struct support.
- Added `BuildTelemetryStructReadRequests` for auto-report telemetry struct read request generation.
- Integrated telemetry struct reads into `runMergedTelemetryWorker` — struct values assembled via `BuildPropertyResponse` and emitted as Object `CommandValue`.
- Updated `normalizeFields`, `cloneFields`, `cloneGroups`, `cloneStructs` for recursive field handling.
- Updated `mergeDeviceWithProfile` to merge `Telemetry.Structs`.

## v0.6.7

- Added `name[index]` support for property get: `BuildPropertyReadSelectionFromNames` now accepts `properties: ["wheels[1]", "wheels[1,3,5]", "wheels[1-10]"]` to read specific struct array indices instead of all items.
- Added `name[index]` support for property set: `BuildPropertyWriteRequests` accepts `wheels[2]` payload format for single-index struct writes.
- Added `name[index]` support for property readback: `BuildPropertyReadRequests` resolves `name[index]` keys from selection maps.
- Added `parseStructNameWithIndices`, `parseIndexList`, `deduplicateAndSort`, and `buildStructSelectionForIndices` helper functions.

## v0.6.5

- Added shared `command` and `control` packages for command descriptors, control requests/results, and common control error semantics.
- Added `runtime/command` and `runtime/control` for command execution, async command resume, SQLite-backed control job persistence, diagnostics, and export.
- Added HTTP model/query APIs and control tracing APIs, including device model query, job list/export/diagnostics, and trace-based property/command result lookup.
- Added MQTT query handling in `runtime/app` so model and control-state views can be queried through MQTT request/reply topics.
- Enhanced property runtime progress/failure reporting and helper utilities used by property operations and runtime commands.

## v0.6.4

- Changed property HTTP requests and responses to use `device_code` only; removed `product_code` from property payloads.
- Changed property HTTP/MQTT request correlation field from `request_id` to `trace_id`.
- Changed HTTP `property/get` and `property/set` to stop reusing the old MQTT property result alias and align on the current `property/result` result flow.
- Changed MQTT property write follow-up handling to align on the current `property/result` result topic instead of the legacy result alias.
- Added property auto-report configuration with telemetry-style `interval`, `onChange`, `watchedFields`, and `heartbeatInterval`.
- Added `property.structs[].autoReport` so large struct arrays can stay request-driven and skip automatic reporting.
- Removed `product_code` from HTTP `/api/v1/runtime/status` device entries.

## v0.6.3

- Changed HTTP `/api/v1/runtime/status` response keys to `snake_case`.
- Aligned runtime device-state keys with MQTT `statusReport`, including `device_code`, `connection_state`, and `last_*` fields.

## v0.6.2

- Changed MQTT `telemetry.rule` payload keys to `trace_id`, `send_at`, and `is_replayed`.
- Changed MQTT `statusReport` payload keys to `device_code`, `data.connection_state`, and `data.last_seen_at`.

## v0.6.1

- Added `statusReport.heartbeatInterval` with a default of `30s`.
- Changed MQTT `statusReport` payloads from device snapshot batches to per-device status messages.
- Added incremental + heartbeat status publishing with per-device heartbeat scheduling.
- Standardized status payload fields to `deviceCode`, `time`, `data.online`, `data.connectionState`, `data.lastSeenAt`, and `data.error`.

## v0.6.0

- Moved runtime bootstrap assembly out of `config` into `runtime/app`.
- Added `runtime/config` as the runtime-facing configuration access layer.
- Added `runtime/property` for property request execution and MQTT property topic integration.
- Reduced `config` to configuration loading, profile merge, and normalization compatibility helpers.
- Generalized runtime naming to avoid S7-specific defaults in shared packages.
- Confirmed `edge-service-fanuc` and `edge-service-s7` both run against the updated SDK layout.

## v0.5.0

- Initial public extraction of the shared edge runtime from `edge-service-s7`.
- Added unified runtime packages for config, auth, ops HTTP, status, telemetry, MQTT transport, reliable queueing, dependency checks, and scheduler logic.
- Added `runtime/app` bootstrap facade for protocol-specific services.
