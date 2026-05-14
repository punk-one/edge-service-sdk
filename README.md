# edge-service-sdk

`edge-service-sdk` is the shared runtime foundation for the edge-service family.

It extracts the common runtime, control, and transport capabilities out of protocol-specific projects so that S7, Fanuc, Modbus, Mitsubishi, and other device services can focus on protocol adapters and device-domain logic only.

## Core Capabilities

- unified config loading, profile merge, and runtime normalization
- auth bootstrap, token issuance, credential query, and protected request verification
- ops HTTP runtime endpoints for health, readiness, runtime status, model query, and control tracing
- device status tracking and optional MQTT status reporting
- telemetry event normalization and reliable replay via SQLite
- MQTT lifecycle management with explicit reconnect, subscription recovery, and health checks
- property request execution over HTTP and MQTT, including auto-report and progress/failure context
- command registry, synchronous/asynchronous command execution, and persisted command result tracking
- control job persistence, result history, diagnostics, export, and MQTT query handling
- dependency checks, worker supervision, and shared logging contracts

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
  Durable queueing, replay, retention, and queue statistics.
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

This repository is being published as `v0.6.7`.
