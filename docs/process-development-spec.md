# Process development specification

## 1. Scope

An SDK Process is application-owned Go code that observes the fixed JetStream
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
│   └── process/
│       ├── telemetry-alarm.yaml
│       └── external-query.yaml
└── internal/
    └── processes/
        ├── registry.go
        ├── telemetry_alarm.go
        └── external_query.go
```

`device.processDir` defaults to `./configs/process`. Go handlers are compiled
into the application and require a rebuild. YAML is loaded at startup; hot
reload and runtime Go-code loading are out of scope.

## 3. Enabling the runtime and binding devices

The embedded bus is enabled in `configs/config.yaml`:

```yaml
natsBus:
  enabled: true

device:
  profilesDir: "./configs/profiles"
  devicesDir: "./configs/devices"
  processDir: "./configs/process"
```

`storeDir`, `maxAge`, and `maxBytes` are optional. Their defaults are
`./data/natsbus`, `72h`, and 1 GiB.

A Process is enabled by binding its name to one or more devices:

```yaml
deviceList:
  - name: device-01
    profileName: profile-01
    productCode: product-01
    processNames:
      - external-query
```

The SDK starts each distinct referenced Process once. Before invoking it, the
runtime requires the message device code to match one of its bound devices.
Messages without a resolvable device code are skipped by device-bound
Processes.

## 4. Process YAML

Only `name` is required:

```yaml
name: external-query

externalQuery:
  baseURL: "https://example.invalid"
  timeoutMs: 2000
```

`handler` defaults to `name`. Runtime controls are optional:

```yaml
name: external-query
handler: external-query
concurrency: 1
timeout: 30s
maxHop: 4
```

| Field | Default | Purpose |
| --- | --- | --- |
| `concurrency` | `1` | Concurrent handler calls. Keep 1 for ordered or stateful processing. |
| `timeout` | `30s` | Maximum duration of one `Handle` call before redelivery. |
| `maxHop` | `4` | Maximum Process-chain depth. Values above the SDK maximum of 16 are rejected. |

Business-specific keys may coexist in the same YAML. The SDK reads the common
definition fields; the application handler may load its own section. A Process
does not receive a Profile object and must not use Process YAML to duplicate
PLC node addresses, Profile point names, polling intervals, or point lengths.
Those are SDK collection/control concerns. If a Process needs a business field,
it owns that field name in code and reads it from the MQTT-compatible payload.

There are no `subscribe`, `publish`, `dataFormats`, or
`acceptProcessMessages` fields. Every enabled Process consumes `edge.v1.>` and
may output any fixed SDK `bus.MessageType`.

## 5. Handler and registry

```go
type Handler interface {
    Handle(context.Context, bus.Message) ([]bus.Message, error)
}
```

Handlers register under the name referenced by YAML:

```go
registry := process.NewRegistry()
registry.MustRegister("external-query", externalQuery{})
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

## 6. Input and output contract

Every Process receives all fixed logical message types for its bound devices:

- `telemetry.report`
- `property.report`
- `property.result`
- `command.result`
- `property.set`
- `property.get`
- `command.call`
- `event.report`
- `status.report`

Use `Message.Type` to dispatch inputs. `Message.Data` is the MQTT payload for
the same logical route. Telemetry uses the effective MQTT data format and
`Message.DataFormat` reports the actual format. A handler must inspect that
field instead of assuming Rule JSON. In multi-group MQTT mode, each emitted
group payload is delivered independently; a Process can observe every emitted
field over the message stream, but one payload is not required to contain the
whole Profile.

Property and command request/result data retains the existing MQTT JSON
contract. Outputs set the logical type and any metadata that cannot be
inherited:

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
`edge.v1.command.call.{identifier}`. Outputs targeting a device not bound to
the current Process are rejected.

## 7. Origins and loop prevention

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

1. A Process always ignores every message it produced itself, including its
   own property and command requests.
2. Messages produced by another Process are accepted, enabling Process chains.
3. Every Process output increments `Edge-Hop`; messages at `maxHop` are
   acknowledged and skipped.
4. SDK-originated outbound messages are already sent to MQTT, so MQTT egress
   acknowledges them without sending them again.
5. MQTT-originated control requests are already executed, so control ingress
   acknowledges them without executing them again.
6. Process- and NATS-originated property/command requests reuse the existing
   property and command services.

## 8. Delivery and error behavior

- Each Process has one independent durable `edge.v1.>` pull consumer.
- A handler input is acknowledged after every output receives a JetStream
  publish acknowledgement.
- Handler errors and timeouts cause delayed redelivery.
- Invalid output message types and unbound target devices are rejected.
- Panics are recovered and treated as handler errors.
- Property and command requests require stable trace IDs and rely on the
  existing control store for idempotency.
- JetStream delivery is at least once; handlers must tolerate duplicate input.

## 9. Required tests for an application Process

Each Process must cover:

- every relevant `Message.Type` and telemetry `Message.DataFormat`;
- invalid, stale, and incomplete payloads;
- messages belonging to unbound devices;
- duplicate trace IDs for side effects;
- context timeout and cancellation;
- self-origin and max-hop behavior;
- every emitted output type;
- HTTP failure and timeout behavior when external data is used;
- property/command payload compatibility with the current MQTT contract.
