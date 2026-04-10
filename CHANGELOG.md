# Changelog

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
