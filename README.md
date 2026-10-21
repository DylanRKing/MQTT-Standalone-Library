# MQTT Standalone Library

A self-contained .NET 10 MQTT 3.1.1 client for AWS IoT Core (QoS 0/1, TLS mutual auth), built with the BCL only and no NuGet dependencies.

## Layout

| Path | Contents |
| --- | --- |
| `src/Mqtt` | Library. Public API at the root; `Internal/Codec`, `Internal/Transport`, `Internal/Core` are internal. |
| `tests/Mqtt.Tests` | xUnit tests: codec golden vectors, frame reader, transport against a loopback TLS server. |

## Status

Following the build order in the design plan:

- [x] 1. Codec: CONNECT/CONNACK, PUBLISH/PUBACK, SUBSCRIBE/SUBACK, PINGREQ/PINGRESP, DISCONNECT
- [x] 2. Transport: TLS connect, read and write loops
- [ ] 3. Client core: connect, publish, subscribe, keep-alive, inbound dispatch
- [ ] 4. Resilience: reconnect, session modes, offline queue, duplicate filter
- [ ] 5. Scale: connection throttle, rate limiter, soak test
- [ ] 6. Production certificate provider

## Build and test

```
dotnet test
```
