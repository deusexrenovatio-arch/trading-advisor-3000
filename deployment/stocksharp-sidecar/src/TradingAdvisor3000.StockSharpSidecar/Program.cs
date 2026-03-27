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

builder.Services.AddSingleton(
    new SidecarGatewayState(
        brokerRoute: route,
        killSwitchActive: ParseBool(builder.Configuration["TA3000_GATEWAY_KILL_SWITCH"])
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
    return Results.Json(state.StreamUpdates(cursor, limit), statusCode: StatusCodes.Status200OK);
});

app.MapGet("/v1/stream/fills", (string? cursor, int? limit, SidecarGatewayState state) =>
{
    return Results.Json(state.StreamFills(cursor, limit), statusCode: StatusCodes.Status200OK);
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

static bool ParseBool(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
    {
        return false;
    }

    var token = value.Trim().ToLowerInvariant();
    return token is "1" or "true" or "yes" or "on";
}

static IResult ToResult<T>(GatewayResult<T> result, Func<T, object> successPayload)
{
    if (!result.IsSuccess || result.Error is not null || result.Value is null)
    {
        return Results.Json(result.Error, statusCode: result.StatusCode);
    }

    return Results.Json(successPayload(result.Value), statusCode: result.StatusCode);
}