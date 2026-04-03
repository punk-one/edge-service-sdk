# edge-service-sdk

`edge-service-sdk` is the shared runtime foundation for the edge-service family.

It centralizes the common edge runtime outside protocol-specific drivers, including:

- config loading and profile merge
- auth bootstrap, token issuance, and protected request verification
- ops HTTP runtime endpoints
- device status tracking
- telemetry event normalization
- MQTT transport
- reliable telemetry replay via SQLite
- dependency checks and worker scheduling

Protocol projects such as S7, Fanuc, Modbus, and Mitsubishi should keep only protocol-specific driver implementations, address codecs, and vendor library bindings.

## Package Layout

- `config`
  Unified runtime configuration, device/profile loading, and bootstrap wiring.
- `driver`
  Shared driver contracts, device models, command request/value types, and value type constants.
- `property`
  Shared property request and response models.
- `auth`
  Credential bootstrap, token issuance, and request authorization.
- `ops/http`
  Runtime health, readiness, auth, and property HTTP endpoints.
- `ops/status`
  Device status tracking and snapshots.
- `runtime/app`
  SDK entry point for service bootstrap.
- `runtime/dependency`
  Runtime dependency checks.
- `runtime/scheduler`
  Worker supervision and restart logic.
- `telemetry`
  Unified telemetry event model and trace identifiers.
- `telemetry/reliable`
  Durable queueing, replay, and queue statistics.
- `transport/mqtt`
  MQTT client lifecycle, publishing, subscriptions, and health checks.
- `logging`
  Shared logging interface and default implementation.

## Current Consumers

- `edge-service-s7` already boots from `runtime/app` and uses SDK driver/logging contracts.
- `edge-service-fanuc` still needs migration for config/runtime/property/auth/http/status/reliable integration.

## Version

This repository is being published as `v0.5.0`.
