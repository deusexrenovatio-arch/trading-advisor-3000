using System.Globalization;

namespace TradingAdvisor3000.StockSharpSidecar.Runtime;

public sealed class SidecarGatewayState
{
    private readonly object _gate = new();
    private readonly string _brokerRoute;
    private readonly IBrokerConnectorTransport _connectorTransport;
    private bool _killSwitchActive;

    public SidecarGatewayState(
        string brokerRoute,
        bool killSwitchActive,
        IBrokerConnectorTransport connectorTransport
    )
    {
        _brokerRoute = string.IsNullOrWhiteSpace(brokerRoute)
            ? "stocksharp->quik->finam"
            : brokerRoute.Trim();
        _killSwitchActive = killSwitchActive;
        _connectorTransport = connectorTransport ?? throw new ArgumentNullException(nameof(connectorTransport));
    }

    public HealthPayload Health()
    {
        lock (_gate)
        {
            return BuildHealthPayload(_connectorTransport.ProbeHealth());
        }
    }

    public ReadyPayload Ready()
    {
        lock (_gate)
        {
            var connector = _connectorTransport.ProbeHealth();
            var ready = !_killSwitchActive && connector.ConnectorReady;
            var reason = _killSwitchActive
                ? "kill_switch_active"
                : connector.ConnectorReady
                    ? "ok"
                    : NormalizeConnectorError(connector.ErrorCode);
            return new ReadyPayload(
                Ready: ready,
                Reason: reason,
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
            var connector = _connectorTransport.ProbeHealth();
            var lines = new[]
            {
                "# HELP ta3000_sidecar_gateway_up Sidecar gateway process health.",
                "# TYPE ta3000_sidecar_gateway_up gauge",
                "ta3000_sidecar_gateway_up 1",
                "# HELP ta3000_sidecar_gateway_queued_intents Total queued intents.",
                "# TYPE ta3000_sidecar_gateway_queued_intents gauge",
                $"ta3000_sidecar_gateway_queued_intents {connector.QueuedIntents.ToString(CultureInfo.InvariantCulture)}",
                "# HELP ta3000_sidecar_gateway_kill_switch Kill switch state.",
                "# TYPE ta3000_sidecar_gateway_kill_switch gauge",
                $"ta3000_sidecar_gateway_kill_switch {(_killSwitchActive ? "1" : "0")}",
                "# HELP ta3000_sidecar_connector_up External broker connector health.",
                "# TYPE ta3000_sidecar_connector_up gauge",
                $"ta3000_sidecar_connector_up {(connector.ConnectorReady ? "1" : "0")}",
            };
            return string.Join('\n', lines) + "\n";
        }
    }

    public GatewayResult<SubmitAck> Submit(SubmitIntentRequest request, string? idempotencyKey)
    {
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

            var gate = GateConnectorHealth<SubmitAck>();
            if (gate is not null)
            {
                return gate;
            }

            return _connectorTransport.Submit(request, idempotencyKey);
        }
    }

    public GatewayResult<CancelAck> Cancel(string intentId, CancelIntentRequest request, string? idempotencyKey)
    {
        lock (_gate)
        {
            var gate = GateConnectorHealth<CancelAck>();
            if (gate is not null)
            {
                return gate;
            }

            return _connectorTransport.Cancel(intentId, request, idempotencyKey);
        }
    }

    public GatewayResult<ReplaceAck> Replace(string intentId, ReplaceIntentRequest request, string? idempotencyKey)
    {
        lock (_gate)
        {
            var gate = GateConnectorHealth<ReplaceAck>();
            if (gate is not null)
            {
                return gate;
            }

            return _connectorTransport.Replace(intentId, request, idempotencyKey);
        }
    }

    public GatewayResult<UpdatesEnvelope> StreamUpdates(string? cursor, int? limit)
    {
        lock (_gate)
        {
            var gate = GateConnectorHealth<UpdatesEnvelope>();
            if (gate is not null)
            {
                return gate;
            }

            return _connectorTransport.StreamUpdates(cursor, limit);
        }
    }

    public GatewayResult<FillsEnvelope> StreamFills(string? cursor, int? limit)
    {
        lock (_gate)
        {
            var gate = GateConnectorHealth<FillsEnvelope>();
            if (gate is not null)
            {
                return gate;
            }

            return _connectorTransport.StreamFills(cursor, limit);
        }
    }

    private GatewayResult<T>? GateConnectorHealth<T>()
    {
        var connector = _connectorTransport.ProbeHealth();
        if (connector.ConnectorReady)
        {
            return null;
        }

        var errorCode = NormalizeConnectorError(connector.ErrorCode);
        return GatewayResult<T>.Fail(
            StatusCodes.Status503ServiceUnavailable,
            errorCode,
            $"broker connector is not ready: {errorCode}"
        );
    }

    private HealthPayload BuildHealthPayload(ConnectorHealthSnapshot connector)
    {
        var status = connector.ConnectorReady ? "ok" : "degraded";
        return new HealthPayload(
            Service: "stocksharp-sidecar-gateway",
            Status: status,
            Route: _brokerRoute,
            KillSwitch: _killSwitchActive,
            QueuedIntents: connector.QueuedIntents,
            ConnectorMode: connector.ConnectorMode,
            ConnectorBackend: connector.ConnectorBackend,
            ConnectorReady: connector.ConnectorReady,
            ConnectorSessionId: connector.ConnectorSessionId,
            ConnectorBindingSource: connector.ConnectorBindingSource,
            ConnectorLastHeartbeat: connector.ConnectorLastHeartbeat,
            ConnectorError: connector.ErrorCode
        );
    }

    private static string NormalizeConnectorError(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? "connector_not_ready" : value.Trim();
    }
}
