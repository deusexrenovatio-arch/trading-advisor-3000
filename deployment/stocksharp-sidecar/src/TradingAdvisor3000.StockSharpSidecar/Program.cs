using System.Text.Json.Serialization;
using TradingAdvisor3000.StockSharpSidecar.Runtime;

var builder = WebApplication.CreateBuilder(args);
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = null;
    options.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
});

var route = builder.Configuration["TA3000_GATEWAY_ROUTE"];
if (string.IsNullOrWhiteSpace(route))
{
    route = "stocksharp->quik->finam";
}
var connectorMode = builder.Configuration["TA3000_CONNECTOR_MODE"];
if (string.IsNullOrWhiteSpace(connectorMode))
{
    connectorMode = "staging-real";
}
var connectorBackend = builder.Configuration["TA3000_CONNECTOR_BACKEND"];
if (string.IsNullOrWhiteSpace(connectorBackend))
{
    connectorBackend = "stocksharp-quik-finam";
}
var connectorBaseUrl = builder.Configuration["TA3000_BROKER_CONNECTOR_BASE_URL"];
var connectorApiPrefix = builder.Configuration["TA3000_BROKER_CONNECTOR_API_PREFIX"];
if (string.IsNullOrWhiteSpace(connectorApiPrefix))
{
    connectorApiPrefix = "v1";
}
var connectorAuthHeader = builder.Configuration["TA3000_BROKER_CONNECTOR_AUTH_HEADER"];
if (string.IsNullOrWhiteSpace(connectorAuthHeader))
{
    connectorAuthHeader = "X-Broker-Api-Key";
}
var connectorAuthToken = builder.Configuration["TA3000_BROKER_CONNECTOR_AUTH_TOKEN"];
if (string.IsNullOrWhiteSpace(connectorAuthToken))
{
    connectorAuthToken = builder.Configuration["TA3000_STOCKSHARP_API_KEY"];
}
var connectorTimeoutSeconds = ParseDouble(builder.Configuration["TA3000_BROKER_CONNECTOR_TIMEOUT_SECONDS"], defaultValue: 3.0);

var connectorTransport = new BrokerConnectorHttpTransport(
    connectorBaseUrl: connectorBaseUrl,
    apiPrefix: connectorApiPrefix,
    authHeaderName: connectorAuthHeader,
    authToken: connectorAuthToken,
    timeoutSeconds: connectorTimeoutSeconds,
    expectedConnectorMode: connectorMode,
    expectedConnectorBackend: connectorBackend,
    route: route
);

builder.Services.AddSingleton(
    new SidecarGatewayState(
        brokerRoute: route,
        killSwitchActive: ParseBool(builder.Configuration["TA3000_GATEWAY_KILL_SWITCH"], defaultValue: false),
        connectorTransport: connectorTransport
    )
);

var app = builder.Build();

app.MapGet("/health", (SidecarGatewayState state) => TypedResults.Ok(state.Health()));

app.MapGet("/ready", (SidecarGatewayState state) =>
{
    var payload = state.Ready();
    if (payload.Ready)
    {
        return Results.Json(payload, statusCode: StatusCodes.Status200OK);
    }

    return Results.Json(payload, statusCode: StatusCodes.Status503ServiceUnavailable);
});

app.MapGet("/metrics", (SidecarGatewayState state) =>
{
    return Results.Text(state.RenderMetrics(), "text/plain; charset=utf-8");
});

app.MapGet("/v1/stream/updates", (string? cursor, int? limit, SidecarGatewayState state) =>
{
    return ToResult(state.StreamUpdates(cursor, limit), value => value);
});

app.MapGet("/v1/stream/fills", (string? cursor, int? limit, SidecarGatewayState state) =>
{
    return ToResult(state.StreamFills(cursor, limit), value => value);
});

app.MapPost("/v1/intents/submit", (HttpContext context, SubmitIntentRequest request, SidecarGatewayState state) =>
{
    var idempotencyKey = context.Request.Headers["X-Idempotency-Key"].FirstOrDefault();
    var result = state.Submit(request, idempotencyKey);
    return ToResult(result, value => new SubmitAckEnvelope(value));
});

app.MapPost("/v1/intents/{intentId}/cancel", (HttpContext context, string intentId, CancelIntentRequest request, SidecarGatewayState state) =>
{
    var idempotencyKey = context.Request.Headers["X-Idempotency-Key"].FirstOrDefault();
    var result = state.Cancel(intentId, request, idempotencyKey);
    return ToResult(result, value => new CancelAckEnvelope(value));
});

app.MapPost("/v1/intents/{intentId}/replace", (HttpContext context, string intentId, ReplaceIntentRequest request, SidecarGatewayState state) =>
{
    var idempotencyKey = context.Request.Headers["X-Idempotency-Key"].FirstOrDefault();
    var result = state.Replace(intentId, request, idempotencyKey);
    return ToResult(result, value => new ReplaceAckEnvelope(value));
});

app.MapPost("/v1/admin/kill-switch", (KillSwitchRequest request, SidecarGatewayState state) =>
{
    var active = request.Active.GetValueOrDefault(true);
    return Results.Json(state.SetKillSwitch(active), statusCode: StatusCodes.Status200OK);
});

app.Run();

static bool ParseBool(string? value, bool defaultValue)
{
    if (string.IsNullOrWhiteSpace(value))
    {
        return defaultValue;
    }

    var token = value.Trim().ToLowerInvariant();
    return token is "1" or "true" or "yes" or "on";
}

static double ParseDouble(string? value, double defaultValue)
{
    if (string.IsNullOrWhiteSpace(value))
    {
        return defaultValue;
    }

    if (!double.TryParse(
        value.Trim(),
        System.Globalization.NumberStyles.Float,
        System.Globalization.CultureInfo.InvariantCulture,
        out var parsed
    ))
    {
        return defaultValue;
    }

    return parsed > 0 ? parsed : defaultValue;
}

static IResult ToResult<T>(GatewayResult<T> result, Func<T, object> successPayload)
{
    if (!result.IsSuccess || result.Error is not null || result.Value is null)
    {
        return Results.Json(result.Error, statusCode: result.StatusCode);
    }

    return Results.Json(successPayload(result.Value), statusCode: result.StatusCode);
}
