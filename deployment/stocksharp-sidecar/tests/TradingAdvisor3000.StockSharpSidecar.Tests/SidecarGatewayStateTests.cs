using System.Text.Json;
using System.Reflection;
using Microsoft.AspNetCore.Http;
using TradingAdvisor3000.StockSharpSidecar.Runtime;
using Xunit;

namespace TradingAdvisor3000.StockSharpSidecar.Tests;

public sealed class SidecarGatewayStateTests
{
    [Fact]
    public void Submit_UsesConnectorTransportAndCachesByIdempotencyKey()
    {
        var connector = new FakeConnectorTransport();
        var state = BuildState(connector);
        var request = new SubmitIntentRequest
        {
            IntentId = "INT-100",
            Intent = ParseObject("{\"broker_adapter\":\"stocksharp-sidecar\",\"created_at\":\"2026-03-27T06:00:00Z\"}"),
        };

        var first = state.Submit(request, "submit:INT-100");
        var second = state.Submit(request, "submit:INT-100");
        var updates = state.StreamUpdates(cursor: "0", limit: 100);

        Assert.True(first.IsSuccess);
        Assert.True(second.IsSuccess);
        Assert.True(updates.IsSuccess);
        Assert.NotNull(first.Value);
        Assert.NotNull(second.Value);
        Assert.NotNull(updates.Value);
        Assert.Equal(first.Value!.ExternalOrderId, second.Value!.ExternalOrderId);
        Assert.Single(updates.Value!.Updates);
    }

    [Fact]
    public void Cancel_ReturnsNotFoundForUnknownIntentId()
    {
        var state = BuildState(new FakeConnectorTransport());

        var outcome = state.Cancel(
            intentId: "INT-404",
            request: new CancelIntentRequest { IntentId = "INT-404", CanceledAt = "2026-03-27T06:01:00Z" },
            idempotencyKey: null
        );

        Assert.False(outcome.IsSuccess);
        Assert.NotNull(outcome.Error);
        Assert.Equal(404, outcome.StatusCode);
        Assert.Equal("unknown_intent_id", outcome.Error!.ErrorCode);
    }

    [Fact]
    public void Replace_RejectsNonPositiveValues()
    {
        var connector = new FakeConnectorTransport();
        var state = BuildState(connector);
        SeedIntent(state, "INT-REPLACE-1");

        var outcome = state.Replace(
            intentId: "INT-REPLACE-1",
            request: new ReplaceIntentRequest
            {
                IntentId = "INT-REPLACE-1",
                NewQty = 0,
                NewPrice = 82.5,
                ReplacedAt = "2026-03-27T06:02:00Z",
            },
            idempotencyKey: null
        );

        Assert.False(outcome.IsSuccess);
        Assert.NotNull(outcome.Error);
        Assert.Equal(400, outcome.StatusCode);
        Assert.Equal("invalid_replace_payload", outcome.Error!.ErrorCode);
    }

    [Fact]
    public void Health_ExposesConnectorSessionFieldsAndReadyReflectsConnectorState()
    {
        var connector = new FakeConnectorTransport(
            snapshot: new ConnectorHealthSnapshot(
                Route: "stocksharp->quik->finam",
                ConnectorReady: false,
                ConnectorMode: "staging-real",
                ConnectorBackend: "stocksharp-quik-finam",
                QueuedIntents: 0,
                ConnectorSessionId: string.Empty,
                ConnectorBindingSource: string.Empty,
                ConnectorLastHeartbeat: string.Empty,
                ErrorCode: "connector_not_ready"
            )
        );
        var state = BuildState(connector);

        var health = state.Health();
        var ready = state.Ready();

        Assert.Equal("degraded", health.Status);
        Assert.Equal("staging-real", health.ConnectorMode);
        Assert.Equal("stocksharp-quik-finam", health.ConnectorBackend);
        Assert.False(health.ConnectorReady);
        Assert.Equal("connector_not_ready", health.ConnectorError);
        Assert.False(ready.Ready);
        Assert.Equal("connector_not_ready", ready.Reason);
    }

    [Fact]
    public void Submit_FailsClosedWhenConnectorIsNotReady()
    {
        var connector = new FakeConnectorTransport(
            snapshot: new ConnectorHealthSnapshot(
                Route: "stocksharp->quik->finam",
                ConnectorReady: false,
                ConnectorMode: "staging-real",
                ConnectorBackend: "stocksharp-quik-finam",
                QueuedIntents: 0,
                ConnectorSessionId: string.Empty,
                ConnectorBindingSource: string.Empty,
                ConnectorLastHeartbeat: string.Empty,
                ErrorCode: "connector_unreachable"
            )
        );
        var state = BuildState(connector);

        var outcome = state.Submit(
            new SubmitIntentRequest
            {
                IntentId = "INT-UNAVAILABLE",
                Intent = ParseObject("{\"broker_adapter\":\"stocksharp-sidecar\"}"),
            },
            idempotencyKey: null
        );

        Assert.False(outcome.IsSuccess);
        Assert.NotNull(outcome.Error);
        Assert.Equal(503, outcome.StatusCode);
        Assert.Equal("connector_unreachable", outcome.Error!.ErrorCode);
    }

    [Fact]
    public void FinamSessionMode_DoesNotMasqueradeLifecycleAsRealTransport()
    {
        var transport = new BrokerConnectorHttpTransport(
            connectorBaseUrl: "https://api.finam.ru",
            apiPrefix: "v1",
            authHeaderName: "Authorization",
            authToken: "integration-token",
            timeoutSeconds: 1.0,
            expectedConnectorMode: "staging-real",
            expectedConnectorBackend: "stocksharp-quik-finam",
            route: "stocksharp->quik->finam"
        );

        SeedFinamReadySnapshot(
            transport,
            new ConnectorHealthSnapshot(
                Route: "stocksharp->quik->finam",
                ConnectorReady: true,
                ConnectorMode: "staging-real",
                ConnectorBackend: "stocksharp-quik-finam",
                QueuedIntents: 0,
                ConnectorSessionId: "SESSION-REAL-001",
                ConnectorBindingSource: "finam-account:1899011",
                ConnectorLastHeartbeat: "2026-03-31T12:00:00Z",
                ErrorCode: null
            )
        );

        var outcome = transport.Submit(
            new SubmitIntentRequest
            {
                IntentId = "INT-FINAM-1",
                Intent = ParseObject("{\"broker_adapter\":\"stocksharp-sidecar\",\"created_at\":\"2026-03-31T12:00:00Z\"}"),
            },
            idempotencyKey: "submit:INT-FINAM-1"
        );

        Assert.False(outcome.IsSuccess);
        Assert.NotNull(outcome.Error);
        Assert.Equal(503, outcome.StatusCode);
        Assert.Equal("finam_lifecycle_not_bound", outcome.Error!.ErrorCode);
    }

    private static SidecarGatewayState BuildState(FakeConnectorTransport connector)
    {
        return new SidecarGatewayState(
            brokerRoute: "stocksharp->quik->finam",
            killSwitchActive: false,
            connectorTransport: connector
        );
    }

    private static JsonElement ParseObject(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }

    private static void SeedFinamReadySnapshot(BrokerConnectorHttpTransport transport, ConnectorHealthSnapshot snapshot)
    {
        var snapshotField = typeof(BrokerConnectorHttpTransport).GetField(
            "_cachedFinamSnapshot",
            BindingFlags.Instance | BindingFlags.NonPublic
        );
        var snapshotAtField = typeof(BrokerConnectorHttpTransport).GetField(
            "_cachedFinamSnapshotAt",
            BindingFlags.Instance | BindingFlags.NonPublic
        );

        Assert.NotNull(snapshotField);
        Assert.NotNull(snapshotAtField);

        snapshotField!.SetValue(transport, snapshot);
        snapshotAtField!.SetValue(transport, DateTimeOffset.UtcNow);
    }

    private static void SeedIntent(SidecarGatewayState state, string intentId)
    {
        var outcome = state.Submit(
            new SubmitIntentRequest
            {
                IntentId = intentId,
                Intent = ParseObject("{\"broker_adapter\":\"stocksharp-sidecar\",\"created_at\":\"2026-03-27T06:00:00Z\"}"),
            },
            idempotencyKey: null
        );
        Assert.True(outcome.IsSuccess);
    }

    private sealed class FakeConnectorTransport : IBrokerConnectorTransport
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, SubmitAck> _submitCache = new(StringComparer.Ordinal);
        private readonly Dictionary<string, CancelAck> _cancelCache = new(StringComparer.Ordinal);
        private readonly Dictionary<string, ReplaceAck> _replaceCache = new(StringComparer.Ordinal);
        private readonly Dictionary<string, string> _externalOrderIdByIntent = new(StringComparer.Ordinal);
        private readonly List<BrokerUpdate> _updates = new();
        private readonly List<BrokerFill> _fills = new();
        private ConnectorHealthSnapshot _snapshot;

        public FakeConnectorTransport(ConnectorHealthSnapshot? snapshot = null)
        {
            _snapshot = snapshot ?? new ConnectorHealthSnapshot(
                Route: "stocksharp->quik->finam",
                ConnectorReady: true,
                ConnectorMode: "staging-real",
                ConnectorBackend: "stocksharp-quik-finam",
                QueuedIntents: 0,
                ConnectorSessionId: "SESSION-TEST-001",
                ConnectorBindingSource: "test-harness",
                ConnectorLastHeartbeat: "2026-03-31T12:00:00Z",
                ErrorCode: null
            );
        }

        public ConnectorHealthSnapshot ProbeHealth()
        {
            lock (_gate)
            {
                return _snapshot with
                {
                    QueuedIntents = _externalOrderIdByIntent.Count,
                };
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
                var cacheKey = string.IsNullOrWhiteSpace(idempotencyKey) ? intentId : idempotencyKey.Trim();
                if (_submitCache.TryGetValue(cacheKey, out var cached))
                {
                    return GatewayResult<SubmitAck>.Ok(cached);
                }

                var externalOrderId = $"gw-{intentId}";
                _externalOrderIdByIntent[intentId] = externalOrderId;
                var ack = new SubmitAck(
                    IntentId: intentId,
                    ExternalOrderId: externalOrderId,
                    Accepted: true,
                    BrokerAdapter: "stocksharp-sidecar",
                    State: "submitted"
                );
                _submitCache[cacheKey] = ack;
                _updates.Add(
                    new BrokerUpdate(
                        ExternalOrderId: externalOrderId,
                        State: "submitted",
                        EventTs: "2026-03-27T06:00:00Z",
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

            lock (_gate)
            {
                var canceledAt = string.IsNullOrWhiteSpace(request.CanceledAt) ? "2026-03-27T06:01:00Z" : request.CanceledAt.Trim();
                var cacheKey = string.IsNullOrWhiteSpace(idempotencyKey)
                    ? $"cancel:{normalizedIntentId}:{canceledAt}"
                    : idempotencyKey.Trim();
                if (_cancelCache.TryGetValue(cacheKey, out var cached))
                {
                    return GatewayResult<CancelAck>.Ok(cached);
                }

                if (!_externalOrderIdByIntent.TryGetValue(normalizedIntentId, out var externalOrderId))
                {
                    return GatewayResult<CancelAck>.Fail(
                        StatusCodes.Status404NotFound,
                        "unknown_intent_id",
                        $"unknown intent_id: {normalizedIntentId}"
                    );
                }

                var ack = new CancelAck(
                    IntentId: normalizedIntentId,
                    ExternalOrderId: externalOrderId,
                    State: "canceled",
                    CanceledAt: canceledAt
                );
                _cancelCache[cacheKey] = ack;
                _updates.Add(
                    new BrokerUpdate(
                        ExternalOrderId: externalOrderId,
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

            lock (_gate)
            {
                var replacedAt = string.IsNullOrWhiteSpace(request.ReplacedAt) ? "2026-03-27T06:02:00Z" : request.ReplacedAt.Trim();
                var cacheKey = string.IsNullOrWhiteSpace(idempotencyKey)
                    ? $"replace:{normalizedIntentId}:{replacedAt}"
                    : idempotencyKey.Trim();
                if (_replaceCache.TryGetValue(cacheKey, out var cached))
                {
                    return GatewayResult<ReplaceAck>.Ok(cached);
                }

                if (!_externalOrderIdByIntent.TryGetValue(normalizedIntentId, out var externalOrderId))
                {
                    return GatewayResult<ReplaceAck>.Fail(
                        StatusCodes.Status404NotFound,
                        "unknown_intent_id",
                        $"unknown intent_id: {normalizedIntentId}"
                    );
                }

                var ack = new ReplaceAck(
                    IntentId: normalizedIntentId,
                    ExternalOrderId: externalOrderId,
                    State: "replaced",
                    NewQty: request.NewQty.Value,
                    NewPrice: request.NewPrice.Value,
                    ReplacedAt: replacedAt
                );
                _replaceCache[cacheKey] = ack;
                _updates.Add(
                    new BrokerUpdate(
                        ExternalOrderId: externalOrderId,
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

        public GatewayResult<UpdatesEnvelope> StreamUpdates(string? cursor, int? limit)
        {
            lock (_gate)
            {
                var start = 0;
                if (!string.IsNullOrWhiteSpace(cursor) &&
                    int.TryParse(cursor, out var parsed) &&
                    parsed >= 0)
                {
                    start = parsed;
                }
                var safeLimit = limit.GetValueOrDefault(500);
                if (safeLimit <= 0)
                {
                    safeLimit = 500;
                }

                var rows = _updates.Skip(start).Take(safeLimit).ToList();
                return GatewayResult<UpdatesEnvelope>.Ok(
                    new UpdatesEnvelope(
                        Updates: rows,
                        NextCursor: (start + rows.Count).ToString()
                    )
                );
            }
        }

        public GatewayResult<FillsEnvelope> StreamFills(string? cursor, int? limit)
        {
            lock (_gate)
            {
                var start = 0;
                if (!string.IsNullOrWhiteSpace(cursor) &&
                    int.TryParse(cursor, out var parsed) &&
                    parsed >= 0)
                {
                    start = parsed;
                }
                var safeLimit = limit.GetValueOrDefault(500);
                if (safeLimit <= 0)
                {
                    safeLimit = 500;
                }

                var rows = _fills.Skip(start).Take(safeLimit).ToList();
                return GatewayResult<FillsEnvelope>.Ok(
                    new FillsEnvelope(
                        Fills: rows,
                        NextCursor: (start + rows.Count).ToString()
                    )
                );
            }
        }
    }
}
