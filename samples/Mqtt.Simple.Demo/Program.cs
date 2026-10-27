using System.Security.Cryptography.X509Certificates;
using System.Text;
using Mqtt.Simple;

// Connects to AWS IoT Core, subscribes to a topic, and publishes each line you type to it.
// Messages arriving on the topic (yours echoed back, or ones sent from the AWS console's
// MQTT test client) are printed. An empty line disconnects.
//
// Usage: dotnet run -- <endpoint> <clientId> <certificate.pem.crt> <private.pem.key> [AmazonRootCA1.pem] [topic]

if (args.Length < 4)
{
    Console.WriteLine("Usage: dotnet run -- <endpoint> <clientId> <certificate.pem.crt> <private.pem.key> [AmazonRootCA1.pem] [topic]");
    return 1;
}

string endpoint = args[0];
string clientId = args[1];
string topic = args.Length > 5 ? args[5] : $"demo/{clientId}";

using var certificate = SimpleMqttClient.LoadCertificate(args[2], args[3]);
using X509Certificate2? rootCa = args.Length > 4 ? SimpleMqttClient.LoadRootCa(args[4]) : null;

Console.WriteLine($"Connecting to {endpoint} as {clientId}...");
await using SimpleMqttClient client = await SimpleMqttClient.ConnectAsync(endpoint, clientId, certificate, rootCa);
Console.WriteLine("Connected.");

client.MessageReceived += (t, payload) => Console.WriteLine($"<- [{t}] {Encoding.UTF8.GetString(payload)}");
client.Disconnected += ex => Console.WriteLine($"Connection lost: {ex.Message}");

await client.SubscribeAsync(topic, qos: 1);
Console.WriteLine($"Subscribed to {topic}. Type a message and press Enter; an empty line quits.");

while (Console.ReadLine() is { Length: > 0 } line)
{
    await client.PublishAsync(topic, line, qos: 1);
    Console.WriteLine($"-> [{topic}] {line}");
}

await client.DisconnectAsync();
Console.WriteLine("Disconnected.");
return 0;
