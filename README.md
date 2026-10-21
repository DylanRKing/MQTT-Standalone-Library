# MQTT Standalone Library

A self-contained .NET 10 MQTT 3.1.1 client for AWS IoT Core (QoS 0/1, TLS mutual auth), built with the BCL only and no NuGet dependencies.

## Layout

| Path | Contents |
| --- | --- |
| `src/Mqtt` | Library. Public API at the root; `Internal/Codec`, `Internal/Transport`, `Internal/Core` are internal. |
| `tests/Mqtt.Tests` | xUnit tests: codec golden vectors, frame reader, transport and client end to end against a scripted fake TLS broker. |

## Status

Following the build order in the design plan:

- [x] 1. Codec: CONNECT/CONNACK, PUBLISH/PUBACK, SUBSCRIBE/SUBACK, PINGREQ/PINGRESP, DISCONNECT
- [x] 2. Transport: TLS connect, read and write loops
- [x] 3. Client core: connect, publish, subscribe, keep-alive, inbound dispatch
- [x] 4. Resilience: reconnect, session modes, offline queue, duplicate filter
- [ ] 5. Scale: connection throttle, rate limiter, soak test
- [ ] 6. Production certificate provider

## Usage

```csharp
await using var device = new MqttClient(new MqttClientOptions
{
    Host = "xxxxxxxx-ats.iot.eu-west-2.amazonaws.com",
    ClientId = "device-042",
    CertificateProvider = myProvider,
});
device.MessageReceived += msg => HandleAsync(msg);
await device.ConnectAsync();
await device.SubscribeAsync([new("sim/device-042/cmd", MqttQos.AtLeastOnce)]);
await device.PublishAsync("sim/device-042/telemetry", payload, MqttQos.AtLeastOnce);
await device.DisconnectAsync();
```

A lost connection is recovered automatically with jittered backoff (1 s → 60 s). On recovery the
client restores subscriptions, resends unacknowledged QoS 1 publishes, then drains the offline queue.
While connecting or reconnecting, QoS 1 publishes wait in a bounded in-memory queue
(`OfflineQueueCapacity`, default 1,000) and QoS 0 publishes return `DroppedOffline`. CONNACK
refusals other than "server unavailable" and TLS authentication failures are not retried: the
client goes to `Disconnected` and pending publishes fail with `MqttConnectionLostException`.

Inbound QoS 1 redeliveries flagged DUP are filtered best-effort; retained replays arrive with
`MqttMessage.Retain = true`. Publishing retained messages needs `iot:RetainPublish` in the
device's IoT policy as well as `iot:Publish`.

## Build and test

```
dotnet test
```
