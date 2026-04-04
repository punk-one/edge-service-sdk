# Changelog

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
