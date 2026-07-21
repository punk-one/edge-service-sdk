# Changelog

## v0.8.5

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
