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
- [ ] 4. Resilience: reconnect, session modes, offline queue, duplicate filter
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

Until step 4 lands, a lost connection is not recovered: the client goes to `Disconnected`,
pending QoS 1 publishes and subscribes fail with `MqttConnectionLostException`, and QoS 1
publishes while disconnected throw `InvalidOperationException`.

## Build and test

```
dotnet test
```
