using System.Text.Json;
using System.Text.Json.Serialization;

namespace TradingAdvisor3000.StockSharpSidecar.Runtime;

public sealed record ErrorPayload(
    [property: JsonPropertyName("error_code")] string ErrorCode,
    [property: JsonPropertyName("message")] string Message
);

public sealed record SubmitIntentRequest
{
    [JsonPropertyName("intent_id")]
    public string? IntentId { get; init; }

    [JsonPropertyName("intent")]
    public JsonElement Intent { get; init; }
}

public sealed record CancelIntentRequest
{
    [JsonPropertyName("intent_id")]
    public string? IntentId { get; init; }

    [JsonPropertyName("canceled_at")]
    public string? CanceledAt { get; init; }
}

public sealed record ReplaceIntentRequest
{
    [JsonPropertyName("intent_id")]
    public string? IntentId { get; init; }

    [JsonPropertyName("new_qty")]
    public int? NewQty { get; init; }

    [JsonPropertyName("new_price")]
    public double? NewPrice { get; init; }

    [JsonPropertyName("replaced_at")]
    public string? ReplacedAt { get; init; }
}

public sealed record SubmitAck(
    [property: JsonPropertyName("intent_id")] string IntentId,
    [property: JsonPropertyName("external_order_id")] string ExternalOrderId,
    [property: JsonPropertyName("accepted")] bool Accepted,
    [property: JsonPropertyName("broker_adapter")] string BrokerAdapter,
    [property: JsonPropertyName("state")] string State
);

public sealed record CancelAck(
    [property: JsonPropertyName("intent_id")] string IntentId,
    [property: JsonPropertyName("external_order_id")] string ExternalOrderId,
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("canceled_at")] string CanceledAt
);

public sealed record ReplaceAck(
    [property: JsonPropertyName("intent_id")] string IntentId,
    [property: JsonPropertyName("external_order_id")] string ExternalOrderId,
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("new_qty")] int NewQty,
    [property: JsonPropertyName("new_price")] double NewPrice,
    [property: JsonPropertyName("replaced_at")] string ReplacedAt
);

public sealed record BrokerUpdate(
    [property: JsonPropertyName("external_order_id")] string ExternalOrderId,
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("event_ts")] string EventTs,
    [property: JsonPropertyName("payload")] IReadOnlyDictionary<string, object?> Payload
);

public sealed record BrokerFill(
    [property: JsonPropertyName("external_order_id")] string ExternalOrderId,
    [property: JsonPropertyName("fill_id")] string FillId,
    [property: JsonPropertyName("qty")] int Qty,
    [property: JsonPropertyName("price")] double Price,
    [property: JsonPropertyName("fee")] double Fee,
    [property: JsonPropertyName("fill_ts")] string FillTs
);

public sealed record SubmitAckEnvelope(
    [property: JsonPropertyName("ack")] SubmitAck Ack
);

public sealed record CancelAckEnvelope(
    [property: JsonPropertyName("ack")] CancelAck Ack
);

public sealed record ReplaceAckEnvelope(
    [property: JsonPropertyName("ack")] ReplaceAck Ack
);

public sealed record UpdatesEnvelope(
    [property: JsonPropertyName("updates")] IReadOnlyList<BrokerUpdate> Updates,
    [property: JsonPropertyName("next_cursor")] string NextCursor
);

public sealed record FillsEnvelope(
    [property: JsonPropertyName("fills")] IReadOnlyList<BrokerFill> Fills,
    [property: JsonPropertyName("next_cursor")] string NextCursor
);

public sealed record HealthPayload(
    [property: JsonPropertyName("service")] string Service,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("route")] string Route,
    [property: JsonPropertyName("kill_switch")] bool KillSwitch,
    [property: JsonPropertyName("queued_intents")] int QueuedIntents
);

public sealed record ReadyPayload(
    [property: JsonPropertyName("ready")] bool Ready,
    [property: JsonPropertyName("reason")] string Reason,
    [property: JsonPropertyName("route")] string Route
);

public sealed record KillSwitchRequest(
    [property: JsonPropertyName("active")] bool? Active
);

public sealed record KillSwitchPayload(
    [property: JsonPropertyName("ok")] bool Ok,
    [property: JsonPropertyName("kill_switch_active")] bool KillSwitchActive
);

public sealed record GatewayResult<T>(
    T? Value,
    ErrorPayload? Error,
    int StatusCode
)
{
    public bool IsSuccess => Error is null;

    public static GatewayResult<T> Ok(T value, int statusCode = StatusCodes.Status200OK)
    {
        return new GatewayResult<T>(value, null, statusCode);
    }

    public static GatewayResult<T> Fail(int statusCode, string errorCode, string message)
    {
        return new GatewayResult<T>(default, new ErrorPayload(errorCode, message), statusCode);
    }
}