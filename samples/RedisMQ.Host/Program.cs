using RedisMQ.Broker.Diagnostics;
using RedisMQ.Broker.Server;
using RedisMQ.Hosting;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Services.AddRedisMQBroker(builder.Configuration);

WebApplication app = builder.Build();

app.MapGet("/", (IConfiguration configuration) =>
    Results.Text(
        $"RedisMQ broker — AMQP {configuration["RedisMQ:Broker:Port"] ?? "5672"}, " +
        $"HTTP {configuration["ASPNETCORE_URLS"] ?? "http://localhost:5000"}"));

app.MapGet("/livez", (AmqpBrokerServer server) =>
{
    BrokerHealth health = server.GetHealth();
    return health.Live ? Results.Ok() : Results.StatusCode(503);
});

app.MapGet("/healthz", (AmqpBrokerServer server) =>
{
    BrokerHealth health = server.GetHealth();
    return Results.Json(health, statusCode: health.Ready ? 200 : 503);
});

await app.RunAsync();
