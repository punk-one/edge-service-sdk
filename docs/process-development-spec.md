# Process development specification

## 1. Scope

An SDK Process is application-owned Go code that observes fixed JetStream
routes and may publish new messages to those routes. It runs in the same binary
as the edge service. It is not a sidecar, a Go plugin, or a replacement for the
existing collection, MQTT, SQLite, property, or command paths.

The optional bus is fail-open. If it cannot start, all Processes are disabled
and the original service continues. A Process panic, timeout, invalid YAML, or
missing handler must not stop the application.

## 2. Application layout

```text
edge-service-*/
├── configs/
│   ├── config.yaml
│   ├── devices/
│   ├── profiles/
│   └── processes/
│       ├── telemetry-alarm.yaml
│       └── external-query.yaml
└── internal/
    └── processes/
        ├── registry.go
        ├── telemetry_alarm.go
        └── external_query.go
```

`process.configDir` is optional. When `process.enabled` is non-empty, its
default is `./configs/processes`. When the entire `process` section or the
enabled list is absent, the SDK does not scan the directory.

Go handlers are compiled into the application and require a rebuild. YAML is
loaded at startup; hot reload and runtime Go-code loading are out of scope.

## 3. Configuration

Main configuration selects enabled Process names:

```yaml
bus:
  enabled: true
  # Optional. Defaults to ./data/nats/{serviceName}.
  storeDir: "./data/nats/device-s7"
  maxAge: "72h"
  maxBytes: 1073741824

process:
  configDir: "./configs/processes"
  enabled:
    - telemetry-alarm
    - external-query
```

The enabled list is authoritative. A YAML file or registered handler that is
not named in the list is not started. An enabled name with a missing YAML file
or handler is reported and skipped without stopping other Processes.

One Process definition:

```yaml
name: telemetry-alarm
handler: telemetry-alarm

subscribe:
  - telemetry.report
  - property.result
  - command.result

publish:
  - property.set
  - command.call

dataFormats:
  - compact

concurrency: 1
timeout: 10s
acceptProcessMessages: false
maxHop: 4
```

YAML uses logical message types, never raw NATS subjects. The fixed mapping is
defined in the SDK README and `bus` package.

## 4. Handler and registry

```go
type Handler interface {
    Handle(context.Context, bus.Message) ([]bus.Message, error)
}
```

Handlers register under the name referenced by YAML:

```go
registry := process.NewRegistry()
registry.MustRegister("telemetry-alarm", telemetryAlarm{})
```

An application with Processes starts through `app.BootstrapWithOptions`:

```go
app.BootstrapWithOptions(serviceName, version, driver, app.BootstrapOptions{
    CommandRegistry: commands.NewRegistry(),
    ProcessRegistry: processes.NewRegistry(),
})
```

The original `app.Bootstrap` remains supported and starts with no Process
registry.

## 5. Message contract

`bus.Message.Data` is the MQTT payload for the same logical route. Telemetry
uses the active MQTT data format (`compact`, `rule`, `raw`, `telemetry`, or
`influx`). A handler must use `Message.DataFormat` rather than assuming Rule
JSON. Property and command request/result data retains the existing MQTT JSON
contract.

Application handlers set output metadata explicitly when it cannot be inherited
from the input:

```go
return []bus.Message{{
    Type:        bus.PropertySet,
    ProductCode: "product-01",
    DeviceCode:  "device-01",
    TraceID:     "trace-01",
    DataFormat:  "json",
    Data:        payload,
}}, nil
```

For `command.call`, `Identifier` is required. The SDK maps it to
`edge.v1.command.call.{identifier}`.

## 6. Origins and loop prevention

The runtime owns the following headers:

| Header | Meaning |
| --- | --- |
| `Edge-Origin` | `sdk`, `mqtt`, `process`, or `nats` |
| `Edge-Process-Name` | publishing Process name |
| `Edge-Message-Type` | fixed logical type |
| `Edge-Data-Format` | MQTT payload format |
| `Edge-Trace-Id` | business trace identifier |
| `Edge-Causation-Id` | input trace that caused the output |
| `Edge-Hop` | Process chain depth |

Rules:

1. SDK-originated outbound messages are already sent to MQTT; the JetStream
   MQTT egress acknowledges them without sending them again.
2. MQTT-originated control requests are already executed; the JetStream control
   ingress acknowledges them without executing them again.
3. Process- and NATS-originated outbound messages use direct MQTT publication,
   which bypasses the mirror hook.
4. Process- and NATS-originated property/command requests reuse the existing
   property and command services.
5. A Process always ignores its own messages.
6. Other Process-originated messages are ignored unless
   `acceptProcessMessages` is true.
7. Messages at `maxHop` are acknowledged and skipped.

## 7. Delivery and error behavior

- Each Process/message-type pair has an independent durable pull consumer.
- A handler input is acknowledged after every declared output receives a
  JetStream publish acknowledgement.
- Handler errors and timeouts cause delayed redelivery.
- Undeclared output types are rejected.
- Panics are recovered and treated as handler errors.
- Property and command requests require stable `trace_id` values and rely on
  the existing control store for idempotency.
- JetStream delivery is at least once; handlers must tolerate duplicate input.

## 8. MQTT format and multi-group behavior

For a single MQTT publisher, the JetStream mirror is byte-for-byte the actual
MQTT payload. In multi-group mode the first configured group is the stable
mirrored representation, so one logical telemetry message is not duplicated on
the bus. Process-generated telemetry should only be enabled when all target
groups accept its declared format.

## 9. Required tests for an application Process

Each Process must cover:

- all declared input data formats;
- invalid and incomplete payloads;
- duplicate `trace_id` handling for side effects;
- context timeout/cancellation;
- self-origin and max-hop behavior;
- every declared output type;
- HTTP failure/timeout behavior when external data is used;
- property/command payload compatibility with the current MQTT contract.
