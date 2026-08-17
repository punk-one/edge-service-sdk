# edge-service-sdk

`edge-service-sdk` is the shared runtime foundation for the edge-service family.

It extracts the common runtime, control, and transport capabilities out of protocol-specific projects so that S7, Fanuc, Modbus, Mitsubishi, and other device services can focus on protocol adapters and device-domain logic only.

## Core Capabilities

- unified config loading, profile merge, and runtime normalization
- auth bootstrap, token issuance, credential query, and protected request verification
- ops HTTP runtime endpoints for health, readiness, runtime status, model query, and control tracing
- device status tracking and optional MQTT status reporting
- telemetry event normalization and reliable replay via SQLite
- built-in EVENT engine for connection, OEE, alarm, fault, pulse and rise-clear rules
- event profile loading from `device.eventDir`, device-level `eventProfile` binding, state persistence, summary windows, and a separate durable event outbox
- MQTT lifecycle management with explicit reconnect, subscription recovery, and health checks
- property request execution over HTTP and MQTT, including auto-report and progress/failure context
- command registry, synchronous/asynchronous command execution, and persisted command result tracking
- control job persistence, result history, diagnostics, export, and MQTT query handling
- dependency checks, worker supervision, and shared logging contracts

## What's New In v0.8.8

- **Multi-MQTT broker support** — group-based architecture: top-level parallel groups, each group with its own failover broker chain. Per-group `dataFormat` overrides, per-group `heartbeatInterval`, and per-broker `clientId`/`username`/`password`. `NewPublisher()` factory auto-detects groups vs single-broker mode.
- **Per-group device status** — `deviceStatusPublisher` creates independent heartbeat goroutines per MQTT group via `StartHeartbeatOnly()`.
- **Runtime ops services** — `configsvc.ConfigService` for dynamic config read/write, diff, backup, restore, and hot-reload callback support. `logsvc.LogSearcher` for log file browsing with pagination and filtering. `ops.Restarter` for service restarts.
- **Service port auto-assign & range search** — `service.port: 0` now auto-assigns a free port (OS-picked). `service.portEnd` enables range search: try each port in `[port, portEnd]`, use the first available. Previously `port <= 0` disabled the server.
- **Telemetry compact format** — now includes `trace_id`, `time`, and `send_at` alongside device-name-keyed data.
- **bitMerge config** — `bitMerge` field on `TelemetryConfig` and `PropertyConfig` for bit-level data merging.
- **ClientId randomization** — MQTT clientId always gets a random suffix: `sdk-{10-digit}` when unset, `{custom}-{6-digit}` when configured.

## EVENT Configuration

EVENT is an SDK capability shared by protocol services. It is enabled only when
`device.eventDir`, `device.eventProfile`, and (for cloud delivery)
`eventReport.topic` are configured. Existing configurations without these keys
continue to run their status, telemetry, property, and command paths unchanged.

```yaml
device:
  profilesDir: "./configs/profiles"
  devicesDir: "./configs/devices"
  eventDir: "./configs/events"

eventReport:
  topic: "v1/gateway/{productCode}/event/report"
  qos: 1
  retain: false
```

Each device selects one named EVENT profile with `eventProfile`. The SDK
evaluates telemetry snapshots and standard connection observations, keeps
state separately from the durable event outbox, preserves the original event
`time` during replay, and updates only transport metadata such as `send_at` and
`is_replayed`. EVENT profiles do not contain middleware pipelines or MQTT
connection settings.

## What's New In v0.7.5

- **`kind: struct`** — standalone struct type (no index), supports multi-field nested structures: `PropertyStructField` now has recursive `Kind`, `Fields`, `MaxItems`, `IndexStride` for nesting
- **Nested arrays** — `kind: array` fields support multi-field struct elements and nested `array`/`struct` sub-types (max 2 array levels)
- **Unified array path** — `struct_array` internally maps to `array` (multi-field), full backward compatibility with existing configs
- **Recursive address calculation** — cumulative offset accumulation through nested struct/array levels for correct PLC address generation
- **Telemetry struct support** — `TelemetryConfig.Structs` and `TelemetryGroup.Structs` reuse `PropertyStruct`, auto-reported structs emit nested JSON objects
- **Input-driven writes** — property write path processes only fields present in the input, supporting partial updates on nested structures
- Added `flattenStructFields`, `buildStructWriteFields`, `buildStructReadFields`, `buildNestedArrayRead`, `buildNestedArrayWrite`, `buildStructFieldSelection`, `BuildTelemetryStructReadRequests`
- Added `IsScalar()`, `IsStruct()`, `IsArray()` helper methods on `PropertyStructField`

## What's New In v0.6.7

- property get/set now supports `name[index]` format for struct arrays: `BuildPropertyReadSelectionFromNames` accepts `wheels[1]`, `wheels[1,3,5]`, `wheels[1-10]` to read specific indices
- `BuildPropertyWriteRequests` accepts `wheels[2]` single-index writes
- `BuildPropertyReadRequests` resolves `name[index]` keys for readback support
- added `parseStructNameWithIndices`, `parseIndexList`, `deduplicateAndSort`, `buildStructSelectionForIndices` helpers

## What's New In v0.6.5

- added shared `command` and `control` packages for command descriptors, execution contracts, and control request/result models
- added `runtime/command` and `runtime/control` for async command execution, SQLite-backed control state, and resume-on-restart behavior
- expanded `ops/http` with device-model query APIs, control job list/export/diagnostics APIs, and trace-based property/command result lookup
- added MQTT query handling in `runtime/app` so device model and control state can be queried over MQTT request/reply topics
- refined property runtime progress, failure context, and helper utilities for telemetry/property reads inside runtime commands

## Quick Start

A protocol-specific service typically keeps only the protocol driver and command registration code locally, then boots through the SDK:

```go
package main

import (
    cmdapi "github.com/punk-one/edge-service-sdk/command"
    "github.com/punk-one/edge-service-sdk/runtime/app"
)

func main() {
    registry := cmdapi.NewRegistry()
    // registry.MustRegister(yourCommand)

    app.Bootstrap("edge-service-yourproto", "v0.6.7", newDriver(), registry)
}
```

The bootstrap flow loads config, initializes auth/MQTT/reliable queue/control store, wires property + command + query handlers, starts runtime HTTP APIs, and then supervises protocol workers.

## Integration Checklist

1. Keep protocol-specific address parsing, connections, and driver calls in your service repository.
2. Put shared runtime config into `configs/config.yaml`, `devices/*.yaml`, and `profiles/*.yaml`.
3. Register command implementations through `command.Registry` when the service exposes callable commands.
4. Route telemetry/property/status/command topics through the SDK MQTT publisher instead of re-implementing transport logic.
5. Reuse the SDK HTTP endpoints and control store so HTTP, MQTT, async execution, and result queries stay aligned.

## Optional embedded JetStream bus

The MQTT, SQLite, collection, property, and command paths remain authoritative.
The embedded JetStream bus is disabled by default and mirrors MQTT traffic only
when `bus.enabled` is true. A bus startup or publish failure is reported as a
degraded optional dependency and does not stop the existing service path.

The embedded server binds `127.0.0.1` on a random operating-system-selected
port. Subjects and the port are SDK conventions and are not application
configuration.

### Fixed subjects

| Logical type | JetStream subject | Direction |
| --- | --- | --- |
| `telemetry.report` | `edge.v1.telemetry.report` | SDK/process to monitoring or MQTT |
| `property.report` | `edge.v1.property.report` | SDK/process to monitoring or MQTT |
| `property.result` | `edge.v1.property.result` | SDK/process to monitoring or MQTT |
| `command.result` | `edge.v1.command.result` | SDK/process to monitoring or MQTT |
| `property.set` | `edge.v1.property.set` | MQTT/process/NATS to property service |
| `property.get` | `edge.v1.property.get` | MQTT/process/NATS to property service |
| `command.call` | `edge.v1.command.call.{identifier}` | MQTT/process/NATS to command service |
| `event.report` | `edge.v1.event.report` | SDK/process to monitoring or MQTT |
| `status.report` | `edge.v1.status.report` | SDK/process to monitoring or MQTT |

JetStream data is the same payload produced for the corresponding MQTT route.
Telemetry therefore follows the effective `telemetryReport.dataFormat`; it is
not forced to `rule`. The SDK adds routing metadata only as NATS headers:
`Edge-Origin`, `Edge-Process-Name`, `Edge-Message-Type`, `Edge-Data-Format`,
`Edge-Trace-Id`, `Edge-Product-Code`, `Edge-Device-Code`, and `Edge-Hop`.

Minimal optional configuration:

```yaml
bus:
  enabled: true

process:
  # Defaults to ./configs/processes when enabled is non-empty.
  configDir: "./configs/processes"
  enabled:
    - telemetry-alarm
```

Omitting either section preserves the previous behavior. See
[`docs/process-development-spec.md`](docs/process-development-spec.md) for the
application process API and lifecycle contract.

## Package Layout

- `config`
  Compatibility-facing configuration model, device/profile loading, normalization helpers, and property/command lookup helpers.
- `driver`
  Shared driver contracts, device models, command request/value types, and value type constants.
- `property`
  Shared property request/response models.
- `command`
  Shared command registry, descriptors, request/response aliases, progress payloads, and command context contracts.
- `control`
  Shared control request, metadata, result, and result-code contracts.
- `auth`
  Credential bootstrap, token issuance, and request authorization.
- `ops/http`
  Runtime health, readiness, auth, model-query, property, command, and control-job HTTP endpoints.
- `ops/status`
  Device status tracking and runtime snapshots.
- `event`
  Protocol-independent event model, YAML validation, expression evaluation, connection/OEE/alarm state machines, payload selection, and summary windows.
- `runtime/event`
  EVENT runtime lifecycle, state-file persistence, event dispatch, and integration with the MQTT event publisher.
- `bus` / `runtime/bus`
  Fixed message contracts plus the optional embedded JetStream server, mirror,
  durable consumers, and random-port lifecycle.
- `process` / `runtime/process`
  Application handler registry, YAML definitions, independent durable
  consumers, self-loop prevention, timeout handling, and output publication.
- `runtime/app`
  SDK bootstrap facade, runtime assembly, status publishing, and MQTT query wiring.
- `runtime/config`
  Runtime-facing config access layer used by bootstrap modules.
- `runtime/property`
  Property request execution, MQTT property topic integration, auto-report, and result persistence.
- `runtime/command`
  Command execution, async command resume, MQTT command topic integration, and result publishing.
- `runtime/control`
  SQLite-backed control job/result store, diagnostics, and export helpers.
- `runtime/dependency`
  Runtime dependency checks.
- `runtime/scheduler`
  Worker supervision and restart logic.
- `telemetry`
  Unified telemetry event model and trace identifiers.
- `telemetry/reliable`
  Durable telemetry queueing and replay plus the independent event outbox namespace.
- `transport/mqtt`
  MQTT client lifecycle, publishing, subscriptions, and health checks.
- `logging`
  Shared logging interface and default implementation.

## Runtime APIs And Topics

The SDK now keeps HTTP and MQTT control flows aligned around the same `trace_id` and control-store model.

HTTP runtime APIs include:

- `/api/v1/health`
- `/api/v1/ready`
- `/api/v1/runtime/status`
- `/api/v1/device/model/properties`
- `/api/v1/device/model/telemetry`
- `/api/v1/device/model/commands`
- `/api/v1/device/control/property/get`
- `/api/v1/device/control/property/set`
- `/api/v1/device/control/command/call/:identifier`
- `/api/v1/device/control/jobs`
- `/api/v1/device/control/jobs/export`
- `/api/v1/device/control/jobs/diagnostics`
- `/api/v1/device/control/property/result/:trace_id`
- `/api/v1/device/control/command/result/:trace_id`

MQTT runtime capabilities include telemetry/property/status publishing, property/command request handling, and optional MQTT query request/reply handling for model and control-state lookup.

## Current Consumers

- `edge-service-s7` boots from `runtime/app` and reuses the shared runtime, telemetry, MQTT, reliable queue, property, command, and control capabilities.
- `edge-service-fanuc` reuses SDK runtime/config/property/auth/http/status/reliable modules and keeps only protocol-specific driver logic locally.

## Documentation

- `docs/系统实现参考架构.md`
  Shared runtime layering, package boundaries, startup flow, and integration guidance for new edge-service projects.
- `CHANGELOG.md`
  Release notes by SDK version.

## Version

This repository is being published as `v0.8.8`.
