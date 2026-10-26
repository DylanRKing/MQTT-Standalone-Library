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
- [x] 5. Scale: connection throttle, rate limiter, soak test
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

## Running many devices

Each `MqttClient` is independent: no shared state, about 3 async loops and one timer each, and
nothing blocks a thread. For many devices in one process:

```csharp
var throttle = new ConnectionThrottle(10);   // at most 10 TLS handshakes at once, shared
var devices = Enumerable.Range(0, 100).Select(i => new MqttClient(baseOptions with
{
    ClientId = $"device-{i:000}",
    ConnectionThrottle = throttle,           // also paces mass reconnects after a network drop
    MaxPublishRate = 50,                     // optional: wait rather than exceed AWS's 100/s per connection
})).ToList();
await Task.WhenAll(devices.Select(d => d.ConnectAsync()));
```

## Build and test

```
dotnet test
```

The soak test (100 devices, every connection dropped periodically, checks no QoS 1 message is
lost and memory stays flat) is skipped unless asked for:

```
MQTT_SOAK_MINUTES=240 MQTT_SOAK_DROP_SECONDS=60 dotnet test --filter SoakTests --logger "console;verbosity=detailed"
```

## Simple client

`src/Mqtt.Simple` is a deliberately minimal alternative to the full library: MQTT 3.1.1 over
TLS with a client certificate, QoS 0/1, and only connect, publish, subscribe and disconnect. It sends keep-alive pings but has no reconnect,
offline queue, rate limiting or topic validation. If the connection drops, `Disconnected` fires
and you create a new client.

| File | Role |
| --- | --- |
| `SimpleMqttClient.cs` | Public API: connect, publish, subscribe, disconnect; read and ping loops; waiting for acks. |
| `Transport/MqttTransport.cs` | TLS connection to AWS; reads and writes whole packets. |
| `Codec/MqttCodec.cs` | Builds outgoing packets and reads fields from incoming ones. |

```csharp
using var cert = SimpleMqttClient.LoadCertificate("device.pem.crt", "private.pem.key");
await using var client = await SimpleMqttClient.ConnectAsync("abc123-ats.iot.eu-west-1.amazonaws.com", "my-thing", cert);
client.MessageReceived += (topic, payload) => Console.WriteLine($"{topic}: {Encoding.UTF8.GetString(payload)}");
await client.SubscribeAsync("demo/my-thing", qos: 1);
await client.PublishAsync("demo/my-thing", "hello", qos: 1);
await client.DisconnectAsync();
```

To try it against AWS, run the console demo. It subscribes to a topic, publishes each line you
type, and prints whatever arrives. You can also send to it from the AWS console's MQTT test client.

```
dotnet run --project samples/Mqtt.Simple.Demo -- <endpoint> <clientId> <certificate.pem.crt> <private.pem.key> [topic]
```

The thing's IoT policy must allow `iot:Connect` for the client ID, plus `iot:Publish`,
`iot:Subscribe` and `iot:Receive` for the topic (default `demo/<clientId>`).
