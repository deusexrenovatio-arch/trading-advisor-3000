using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace TradingAdvisor3000.StockSharpSidecar.Runtime;

public sealed class SidecarGatewayState
{
    private sealed record IntentRecord(string ExternalOrderId, string BrokerAdapter, string State);

    private readonly object _gate = new();
    private readonly string _brokerRoute;
    private bool _killSwitchActive;

    private readonly Dictionary<string, IntentRecord> _intents = new(StringComparer.Ordinal);
    private readonly Dictionary<string, SubmitAck> _submitCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, CancelAck> _cancelCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ReplaceAck> _replaceCache = new(StringComparer.Ordinal);
    private readonly List<BrokerUpdate> _updates = new();
    private readonly List<BrokerFill> _fills = new();

    public SidecarGatewayState(string brokerRoute, bool killSwitchActive)
    {
        _brokerRoute = string.IsNullOrWhiteSpace(brokerRoute)
            ? "stocksharp->quik->finam"
            : brokerRoute.Trim();
        _killSwitchActive = killSwitchActive;
    }

    public HealthPayload Health()
    {
        lock (_gate)
        {
            return new HealthPayload(
                Service: "stocksharp-sidecar-gateway",
                Status: "ok",
                Route: _brokerRoute,
                KillSwitch: _killSwitchActive,
                QueuedIntents: _intents.Count
            );
        }
    }

    public ReadyPayload Ready()
    {
        lock (_gate)
        {
            return new ReadyPayload(
                Ready: !_killSwitchActive,
                Reason: _killSwitchActive ? "kill_switch_active" : "ok",
                Route: _brokerRoute
            );
        }
    }

    public KillSwitchPayload SetKillSwitch(bool active)
    {
        lock (_gate)
        {
            _killSwitchActive = active;
            return new KillSwitchPayload(Ok: true, KillSwitchActive: _killSwitchActive);
        }
    }

    public string RenderMetrics()
    {
        lock (_gate)
        {
            var lines = new[]
            {
                "# HELP ta3000_sidecar_gateway_up Sidecar gateway process health.",
                "# TYPE ta3000_sidecar_gateway_up gauge",
                "ta3000_sidecar_gateway_up 1",
                "# HELP ta3000_sidecar_gateway_queued_intents Total queued intents.",
                "# TYPE ta3000_sidecar_gateway_queued_intents gauge",
                $"ta3000_sidecar_gateway_queued_intents {_intents.Count.ToString(CultureInfo.InvariantCulture)}",
                "# HELP ta3000_sidecar_gateway_kill_switch Kill switch state.",
                "# TYPE ta3000_sidecar_gateway_kill_switch gauge",
                $"ta3000_sidecar_gateway_kill_switch {(_killSwitchActive ? "1" : "0")}",
            };
            return string.Join('\n', lines) + "\n";
        }
    }

    public GatewayResult<SubmitAck> Submit(SubmitIntentRequest request, string? idempotencyKey)
    {
        var intentId = (request.IntentId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(intentId) || request.Intent.ValueKind != JsonValueKind.Object)
        {
            return GatewayResult<SubmitAck>.Fail(
                StatusCodes.Status400BadRequest,
                "invalid_submit_payload",
                "intent_id and intent object are required"
            );
        }

        lock (_gate)
        {
            if (_killSwitchActive)
            {
                return GatewayResult<SubmitAck>.Fail(
                    StatusCodes.Status503ServiceUnavailable,
                    "kill_switch_active",
                    "gateway kill-switch is active"
                );
            }

            var cacheKey = NormalizeCacheKey(idempotencyKey, fallback: intentId);
            if (_submitCache.TryGetValue(cacheKey, out var cached))
            {
                return GatewayResult<SubmitAck>.Ok(cached);
            }

            var brokerAdapter = ReadString(request.Intent, "broker_adapter", "stocksharp-sidecar");
            var createdAt = ReadString(request.Intent, "created_at", UtcNow());
            var externalOrderId = HashExternalOrderId(intentId);

            _intents[intentId] = new IntentRecord(externalOrderId, brokerAdapter, "submitted");
            var ack = new SubmitAck(
                IntentId: intentId,
                ExternalOrderId: externalOrderId,
                Accepted: true,
                BrokerAdapter: brokerAdapter,
                State: "submitted"
            );
            _submitCache[cacheKey] = ack;
            _updates.Add(
                new BrokerUpdate(
                    ExternalOrderId: externalOrderId,
                    State: "submitted",
                    EventTs: createdAt,
                    Payload: new Dictionary<string, object?>
                    {
                        ["intent_id"] = intentId,
                    }
                )
            );
            return GatewayResult<SubmitAck>.Ok(ack);
        }
    }

    public GatewayResult<CancelAck> Cancel(string intentId, CancelIntentRequest request, string? idempotencyKey)
    {
        var normalizedIntentId = (intentId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedIntentId))
        {
            return GatewayResult<CancelAck>.Fail(
                StatusCodes.Status400BadRequest,
                "invalid_cancel_payload",
                "intent_id path parameter is required"
            );
        }

        var canceledAt = string.IsNullOrWhiteSpace(request.CanceledAt) ? UtcNow() : request.CanceledAt.Trim();

        lock (_gate)
        {
            var cacheKey = NormalizeCacheKey(idempotencyKey, fallback: $"cancel:{normalizedIntentId}:{canceledAt}");
            if (_cancelCache.TryGetValue(cacheKey, out var cached))
            {
                return GatewayResult<CancelAck>.Ok(cached);
            }

            if (!_intents.TryGetValue(normalizedIntentId, out var record))
            {
                return GatewayResult<CancelAck>.Fail(
                    StatusCodes.Status404NotFound,
                    "unknown_intent_id",
                    $"unknown intent_id: {normalizedIntentId}"
                );
            }

            _intents[normalizedIntentId] = record with { State = "canceled" };
            var ack = new CancelAck(
                IntentId: normalizedIntentId,
                ExternalOrderId: record.ExternalOrderId,
                State: "canceled",
                CanceledAt: canceledAt
            );
            _cancelCache[cacheKey] = ack;
            _updates.Add(
                new BrokerUpdate(
                    ExternalOrderId: record.ExternalOrderId,
                    State: "canceled",
                    EventTs: canceledAt,
                    Payload: new Dictionary<string, object?>
                    {
                        ["intent_id"] = normalizedIntentId,
                    }
                )
            );
            return GatewayResult<CancelAck>.Ok(ack);
        }
    }

    public GatewayResult<ReplaceAck> Replace(string intentId, ReplaceIntentRequest request, string? idempotencyKey)
    {
        var normalizedIntentId = (intentId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedIntentId))
        {
            return GatewayResult<ReplaceAck>.Fail(
                StatusCodes.Status400BadRequest,
                "invalid_replace_payload",
                "intent_id path parameter is required"
            );
        }

        if (!request.NewQty.HasValue || !request.NewPrice.HasValue)
        {
            return GatewayResult<ReplaceAck>.Fail(
                StatusCodes.Status400BadRequest,
                "invalid_replace_payload",
                "new_qty and new_price must be numeric"
            );
        }

        if (request.NewQty.Value <= 0 || request.NewPrice.Value <= 0)
        {
            return GatewayResult<ReplaceAck>.Fail(
                StatusCodes.Status400BadRequest,
                "invalid_replace_payload",
                "new_qty/new_price must be positive"
            );
        }

        var replacedAt = string.IsNullOrWhiteSpace(request.ReplacedAt) ? UtcNow() : request.ReplacedAt.Trim();

        lock (_gate)
        {
            var cacheKey = NormalizeCacheKey(idempotencyKey, fallback: $"replace:{normalizedIntentId}:{replacedAt}");
            if (_replaceCache.TryGetValue(cacheKey, out var cached))
            {
                return GatewayResult<ReplaceAck>.Ok(cached);
            }

            if (!_intents.TryGetValue(normalizedIntentId, out var record))
            {
                return GatewayResult<ReplaceAck>.Fail(
                    StatusCodes.Status404NotFound,
                    "unknown_intent_id",
                    $"unknown intent_id: {normalizedIntentId}"
                );
            }

            _intents[normalizedIntentId] = record with { State = "replaced" };
            var ack = new ReplaceAck(
                IntentId: normalizedIntentId,
                ExternalOrderId: record.ExternalOrderId,
                State: "replaced",
                NewQty: request.NewQty.Value,
                NewPrice: request.NewPrice.Value,
                ReplacedAt: replacedAt
            );
            _replaceCache[cacheKey] = ack;
            _updates.Add(
                new BrokerUpdate(
                    ExternalOrderId: record.ExternalOrderId,
                    State: "replaced",
                    EventTs: replacedAt,
                    Payload: new Dictionary<string, object?>
                    {
                        ["intent_id"] = normalizedIntentId,
                        ["new_qty"] = request.NewQty.Value,
                        ["new_price"] = request.NewPrice.Value,
                    }
                )
            );
            return GatewayResult<ReplaceAck>.Ok(ack);
        }
    }

    public UpdatesEnvelope StreamUpdates(string? cursor, int? limit)
    {
        lock (_gate)
        {
            var (start, safeLimit) = ParseStreamWindow(cursor, limit, _updates.Count);
            var rows = _updates
                .Skip(start)
                .Take(safeLimit)
                .ToList();
            var nextCursor = (start + rows.Count).ToString(CultureInfo.InvariantCulture);
            return new UpdatesEnvelope(rows, nextCursor);
        }
    }

    public FillsEnvelope StreamFills(string? cursor, int? limit)
    {
        lock (_gate)
        {
            var (start, safeLimit) = ParseStreamWindow(cursor, limit, _fills.Count);
            var rows = _fills
                .Skip(start)
                .Take(safeLimit)
                .ToList();
            var nextCursor = (start + rows.Count).ToString(CultureInfo.InvariantCulture);
            return new FillsEnvelope(rows, nextCursor);
        }
    }

    private static (int Start, int Limit) ParseStreamWindow(string? cursor, int? limit, int count)
    {
        var parsedCursor = 0;
        if (!string.IsNullOrWhiteSpace(cursor) && int.TryParse(cursor, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
        {
            parsedCursor = value;
        }
        if (parsedCursor < 0)
        {
            parsedCursor = 0;
        }
        if (parsedCursor > count)
        {
            parsedCursor = count;
        }

        var safeLimit = limit.GetValueOrDefault(500);
        if (safeLimit <= 0)
        {
            safeLimit = 500;
        }
        if (safeLimit > 5000)
        {
            safeLimit = 5000;
        }

        return (parsedCursor, safeLimit);
    }

    private static string NormalizeCacheKey(string? raw, string fallback)
    {
        return string.IsNullOrWhiteSpace(raw) ? fallback : raw.Trim();
    }

    private static string HashExternalOrderId(string intentId)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(intentId));
        var hash = Convert.ToHexString(bytes).ToLowerInvariant();
        return $"gw-{hash[..16]}";
    }

    private static string ReadString(JsonElement payload, string name, string fallback)
    {
        if (payload.ValueKind == JsonValueKind.Object && payload.TryGetProperty(name, out var value))
        {
            if (value.ValueKind == JsonValueKind.String)
            {
                var text = value.GetString();
                if (!string.IsNullOrWhiteSpace(text))
                {
                    return text.Trim();
                }
            }
        }
        return fallback;
    }

    private static string UtcNow()
    {
        return DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
    }
}