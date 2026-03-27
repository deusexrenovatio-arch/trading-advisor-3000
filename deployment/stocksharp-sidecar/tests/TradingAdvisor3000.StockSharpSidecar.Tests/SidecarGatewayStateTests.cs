using System.Text.Json;
using TradingAdvisor3000.StockSharpSidecar.Runtime;
using Xunit;

namespace TradingAdvisor3000.StockSharpSidecar.Tests;

public sealed class SidecarGatewayStateTests
{
    [Fact]
    public void Submit_UsesIdempotencyCacheAndProducesSingleUpdateForRepeatedKey()
    {
        var state = new SidecarGatewayState("stocksharp->quik->finam", killSwitchActive: false);
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
        Assert.NotNull(first.Value);
        Assert.NotNull(second.Value);
        Assert.Equal(first.Value!.ExternalOrderId, second.Value!.ExternalOrderId);
        Assert.Single(updates.Updates);
    }

    [Fact]
    public void Cancel_ReturnsNotFoundForUnknownIntentId()
    {
        var state = new SidecarGatewayState("stocksharp->quik->finam", killSwitchActive: false);

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
        var state = new SidecarGatewayState("stocksharp->quik->finam", killSwitchActive: false);
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
    public void StreamUpdates_AdvancesCursorAndReturnsDeterministicWindow()
    {
        var state = new SidecarGatewayState("stocksharp->quik->finam", killSwitchActive: false);
        SeedIntent(state, "INT-CURSOR-1");
        SeedIntent(state, "INT-CURSOR-2");
        SeedIntent(state, "INT-CURSOR-3");

        var first = state.StreamUpdates(cursor: "0", limit: 2);
        var second = state.StreamUpdates(cursor: first.NextCursor, limit: 2);

        Assert.Equal(2, first.Updates.Count);
        Assert.Equal("2", first.NextCursor);
        Assert.Single(second.Updates);
        Assert.Equal("3", second.NextCursor);
    }

    private static JsonElement ParseObject(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
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
}
