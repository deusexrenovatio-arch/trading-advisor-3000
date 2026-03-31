using System.Globalization;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace TradingAdvisor3000.StockSharpSidecar.Runtime;

public sealed record ConnectorHealthSnapshot(
    string Route,
    bool ConnectorReady,
    string ConnectorMode,
    string ConnectorBackend,
    int QueuedIntents,
    string ConnectorSessionId,
    string ConnectorBindingSource,
    string ConnectorLastHeartbeat,
    string? ErrorCode
);

public interface IBrokerConnectorTransport
{
    ConnectorHealthSnapshot ProbeHealth();
    GatewayResult<SubmitAck> Submit(SubmitIntentRequest request, string? idempotencyKey);
    GatewayResult<CancelAck> Cancel(string intentId, CancelIntentRequest request, string? idempotencyKey);
    GatewayResult<ReplaceAck> Replace(string intentId, ReplaceIntentRequest request, string? idempotencyKey);
    GatewayResult<UpdatesEnvelope> StreamUpdates(string? cursor, int? limit);
    GatewayResult<FillsEnvelope> StreamFills(string? cursor, int? limit);
}

public sealed class BrokerConnectorHttpTransport : IBrokerConnectorTransport
{
    private readonly HttpClient _httpClient;
    private readonly Uri? _connectorBaseUrl;
    private readonly string _apiPrefix;
    private readonly string _route;
    private readonly string _expectedConnectorMode;
    private readonly string _expectedConnectorBackend;
    private readonly string? _authHeaderName;
    private readonly string? _authToken;
    private readonly bool _useFinamSessionMode;
    private readonly string _finamSessionDetailsPath;
    private readonly double _finamSessionCacheSeconds;
    private readonly object _gate = new();

    private ConnectorHealthSnapshot? _cachedFinamSnapshot;
    private DateTimeOffset _cachedFinamSnapshotAt = DateTimeOffset.MinValue;

    private readonly Dictionary<string, SubmitAck> _submitCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, CancelAck> _cancelCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ReplaceAck> _replaceCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _externalOrderIdByIntent = new(StringComparer.Ordinal);
    private readonly List<BrokerUpdate> _updates = new();
    private readonly List<BrokerFill> _fills = new();

    public BrokerConnectorHttpTransport(
        string? connectorBaseUrl,
        string apiPrefix,
        string? authHeaderName,
        string? authToken,
        double timeoutSeconds,
        string expectedConnectorMode,
        string expectedConnectorBackend,
        string route,
        string? finamSessionDetailsPath = null,
        double finamSessionCacheSeconds = 5.0
    )
    {
        _route = string.IsNullOrWhiteSpace(route) ? "stocksharp->quik->finam" : route.Trim();
        _expectedConnectorMode = string.IsNullOrWhiteSpace(expectedConnectorMode) ? "staging-real" : expectedConnectorMode.Trim().ToLowerInvariant();
        _expectedConnectorBackend = string.IsNullOrWhiteSpace(expectedConnectorBackend) ? "stocksharp-quik-finam" : expectedConnectorBackend.Trim().ToLowerInvariant();
        _apiPrefix = string.IsNullOrWhiteSpace(apiPrefix) ? "v1" : apiPrefix.Trim().Trim('/');
        _authHeaderName = string.IsNullOrWhiteSpace(authHeaderName) ? null : authHeaderName.Trim();
        _authToken = string.IsNullOrWhiteSpace(authToken) ? null : authToken.Trim();
        _finamSessionDetailsPath = NormalizePath(string.IsNullOrWhiteSpace(finamSessionDetailsPath) ? "/v1/sessions/details" : finamSessionDetailsPath);
        _finamSessionCacheSeconds = finamSessionCacheSeconds > 0 ? finamSessionCacheSeconds : 5.0;

        if (!string.IsNullOrWhiteSpace(connectorBaseUrl) && Uri.TryCreate(connectorBaseUrl.Trim(), UriKind.Absolute, out var parsed))
        {
            _connectorBaseUrl = parsed;
        }
        _useFinamSessionMode = IsFinamHost(_connectorBaseUrl);
        _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(timeoutSeconds > 0 ? timeoutSeconds : 3.0) };
    }

    public ConnectorHealthSnapshot ProbeHealth() => _useFinamSessionMode ? ProbeFinamSessionHealth() : ProbeGatewayHealth();
    public GatewayResult<SubmitAck> Submit(SubmitIntentRequest request, string? idempotencyKey) => _useFinamSessionMode ? SubmitFinam(request, idempotencyKey) : SubmitGateway(request, idempotencyKey);
    public GatewayResult<CancelAck> Cancel(string intentId, CancelIntentRequest request, string? idempotencyKey) => _useFinamSessionMode ? CancelFinam(intentId, request, idempotencyKey) : CancelGateway(intentId, request, idempotencyKey);
    public GatewayResult<ReplaceAck> Replace(string intentId, ReplaceIntentRequest request, string? idempotencyKey) => _useFinamSessionMode ? ReplaceFinam(intentId, request, idempotencyKey) : ReplaceGateway(intentId, request, idempotencyKey);
    public GatewayResult<UpdatesEnvelope> StreamUpdates(string? cursor, int? limit) => _useFinamSessionMode ? StreamUpdatesFinam(cursor, limit) : StreamUpdatesGateway(cursor, limit);
    public GatewayResult<FillsEnvelope> StreamFills(string? cursor, int? limit) => _useFinamSessionMode ? StreamFillsFinam(cursor, limit) : StreamFillsGateway(cursor, limit);

    private ConnectorHealthSnapshot ProbeGatewayHealth()
    {
        if (_connectorBaseUrl is null) return Unhealthy("connector_not_configured", 0);
        var response = SendJson("GET", "/health", null, null, includeApiPrefix: false, query: null);
        if (!response.Ok) return Unhealthy(response.ErrorCode, 0);
        if (response.Root.ValueKind != JsonValueKind.Object) return Unhealthy("connector_protocol_error", 0);
        var root = response.Root;
        var mode = ReadString(root, "connector_mode", _expectedConnectorMode).ToLowerInvariant();
        var backend = ReadString(root, "connector_backend", _expectedConnectorBackend).ToLowerInvariant();
        var ready = ReadBool(root, "connector_ready", false);
        var session = ReadString(root, "connector_session_id", "");
        var binding = ReadString(root, "connector_binding_source", "");
        var heartbeat = ReadString(root, "connector_last_heartbeat", "");
        var error = ready ? null : "connector_not_ready";
        if (mode != _expectedConnectorMode) { ready = false; error = "connector_mode_mismatch"; }
        if (backend != _expectedConnectorBackend) { ready = false; error = "connector_backend_mismatch"; }
        if (string.IsNullOrWhiteSpace(session)) { ready = false; error ??= "connector_session_missing"; }
        if (string.IsNullOrWhiteSpace(binding)) { ready = false; error ??= "connector_binding_source_missing"; }
        if (string.IsNullOrWhiteSpace(heartbeat)) { ready = false; error ??= "connector_heartbeat_missing"; }
        return new ConnectorHealthSnapshot(_route, ready, mode, backend, Math.Max(0, ReadInt(root, "queued_intents", 0)), session, binding, heartbeat, error);
    }

    private ConnectorHealthSnapshot ProbeFinamSessionHealth()
    {
        lock (_gate)
        {
            var queued = Math.Max(0, _externalOrderIdByIntent.Count);
            var now = DateTimeOffset.UtcNow;
            if (_cachedFinamSnapshot is not null && (now - _cachedFinamSnapshotAt).TotalSeconds < _finamSessionCacheSeconds)
            {
                return _cachedFinamSnapshot with { QueuedIntents = queued };
            }
            if (_connectorBaseUrl is null || string.IsNullOrWhiteSpace(_authToken))
            {
                return CacheAndReturn(Unhealthy(_connectorBaseUrl is null ? "connector_not_configured" : "connector_auth_missing", queued), now);
            }
            var response = SendJson("POST", _finamSessionDetailsPath, new { token = _authToken }, null, includeApiPrefix: false, query: null);
            if (!response.Ok || response.Root.ValueKind != JsonValueKind.Object)
            {
                return CacheAndReturn(Unhealthy(response.Ok ? "connector_protocol_error" : response.ErrorCode, queued), now);
            }
            var root = response.Root;
            var createdAt = ReadString(root, "created_at", "");
            var expiresAt = ReadString(root, "expires_at", "");
            var readOnly = ReadBool(root, "readonly", false);
            var accounts = ReadStringList(root, "account_ids");
            var ready = !string.IsNullOrWhiteSpace(createdAt) && !string.IsNullOrWhiteSpace(expiresAt);
            var sessionSeed = $"{createdAt}|{expiresAt}|{string.Join(",", accounts)}|{readOnly}";
            var sessionId = ready ? StableId("session", sessionSeed) : "";
            var binding = ready ? (accounts.Count > 0 ? $"finam-account:{string.Join(",", accounts.Take(3))}" : (readOnly ? "finam-session-readonly" : "finam-session")) : "";
            var heartbeat = ready ? UtcNow() : "";
            var snapshot = new ConnectorHealthSnapshot(_route, ready, _expectedConnectorMode, _expectedConnectorBackend, queued, sessionId, binding, heartbeat, ready ? null : "connector_session_contract_missing");
            return CacheAndReturn(snapshot, now);
        }
    }

    private GatewayResult<SubmitAck> SubmitGateway(SubmitIntentRequest request, string? idempotencyKey)
    {
        var intentId = (request.IntentId ?? "").Trim();
        if (string.IsNullOrWhiteSpace(intentId) || request.Intent.ValueKind != JsonValueKind.Object) return GatewayResult<SubmitAck>.Fail(400, "invalid_submit_payload", "intent_id and intent object are required");
        var response = SendJson("POST", "/intents/submit", new { intent_id = intentId, intent = request.Intent }, idempotencyKey, includeApiPrefix: true, query: null);
        if (!response.Ok) return GatewayResult<SubmitAck>.Fail(response.StatusCode, response.ErrorCode, response.ErrorMessage);
        var ack = TryGetAck(response.Root);
        var externalOrderId = ReadString(ack, "external_order_id", "");
        if (string.IsNullOrWhiteSpace(externalOrderId)) return GatewayResult<SubmitAck>.Fail(502, "connector_protocol_error", "connector submit response is missing external_order_id");
        return GatewayResult<SubmitAck>.Ok(new SubmitAck(ReadString(ack, "intent_id", intentId), externalOrderId, ReadBool(ack, "accepted", true), ReadString(ack, "broker_adapter", "stocksharp-sidecar"), ReadString(ack, "state", "submitted")));
    }

    private GatewayResult<CancelAck> CancelGateway(string intentId, CancelIntentRequest request, string? idempotencyKey)
    {
        var normalized = (intentId ?? "").Trim();
        if (string.IsNullOrWhiteSpace(normalized)) return GatewayResult<CancelAck>.Fail(400, "invalid_cancel_payload", "intent_id path parameter is required");
        var canceledAt = string.IsNullOrWhiteSpace(request.CanceledAt) ? UtcNow() : request.CanceledAt.Trim();
        var response = SendJson("POST", $"/intents/{Uri.EscapeDataString(normalized)}/cancel", new { intent_id = normalized, canceled_at = canceledAt }, idempotencyKey, includeApiPrefix: true, query: null);
        if (!response.Ok) return GatewayResult<CancelAck>.Fail(response.StatusCode, response.ErrorCode, response.ErrorMessage);
        var ack = TryGetAck(response.Root);
        var externalOrderId = ReadString(ack, "external_order_id", "");
        if (string.IsNullOrWhiteSpace(externalOrderId)) return GatewayResult<CancelAck>.Fail(502, "connector_protocol_error", "connector cancel response is missing external_order_id");
        return GatewayResult<CancelAck>.Ok(new CancelAck(ReadString(ack, "intent_id", normalized), externalOrderId, ReadString(ack, "state", "canceled"), ReadString(ack, "canceled_at", canceledAt)));
    }

    private GatewayResult<ReplaceAck> ReplaceGateway(string intentId, ReplaceIntentRequest request, string? idempotencyKey)
    {
        var normalized = (intentId ?? "").Trim();
        if (string.IsNullOrWhiteSpace(normalized)) return GatewayResult<ReplaceAck>.Fail(400, "invalid_replace_payload", "intent_id path parameter is required");
        if (!request.NewQty.HasValue || !request.NewPrice.HasValue || request.NewQty.Value <= 0 || request.NewPrice.Value <= 0) return GatewayResult<ReplaceAck>.Fail(400, "invalid_replace_payload", "new_qty/new_price must be positive");
        var replacedAt = string.IsNullOrWhiteSpace(request.ReplacedAt) ? UtcNow() : request.ReplacedAt.Trim();
        var response = SendJson("POST", $"/intents/{Uri.EscapeDataString(normalized)}/replace", new { intent_id = normalized, new_qty = request.NewQty.Value, new_price = request.NewPrice.Value, replaced_at = replacedAt }, idempotencyKey, includeApiPrefix: true, query: null);
        if (!response.Ok) return GatewayResult<ReplaceAck>.Fail(response.StatusCode, response.ErrorCode, response.ErrorMessage);
        var ack = TryGetAck(response.Root);
        var externalOrderId = ReadString(ack, "external_order_id", "");
        if (string.IsNullOrWhiteSpace(externalOrderId)) return GatewayResult<ReplaceAck>.Fail(502, "connector_protocol_error", "connector replace response is missing external_order_id");
        return GatewayResult<ReplaceAck>.Ok(new ReplaceAck(ReadString(ack, "intent_id", normalized), externalOrderId, ReadString(ack, "state", "replaced"), ReadInt(ack, "new_qty", request.NewQty.Value), ReadDouble(ack, "new_price", request.NewPrice.Value), ReadString(ack, "replaced_at", replacedAt)));
    }

    private GatewayResult<UpdatesEnvelope> StreamUpdatesGateway(string? cursor, int? limit)
    {
        var response = SendJson("GET", "/stream/updates", null, null, includeApiPrefix: true, query: new Dictionary<string, string> { ["cursor"] = string.IsNullOrWhiteSpace(cursor) ? "" : cursor.Trim(), ["limit"] = Math.Max(1, limit.GetValueOrDefault(500)).ToString(CultureInfo.InvariantCulture) });
        if (!response.Ok) return GatewayResult<UpdatesEnvelope>.Fail(response.StatusCode, response.ErrorCode, response.ErrorMessage);
        if (!response.Root.TryGetProperty("updates", out var updatesElement) || updatesElement.ValueKind != JsonValueKind.Array) return GatewayResult<UpdatesEnvelope>.Fail(502, "connector_protocol_error", "connector updates response is missing updates array");
        var updates = new List<BrokerUpdate>();
        foreach (var item in updatesElement.EnumerateArray())
        {
            var payload = new Dictionary<string, object?>();
            if (item.TryGetProperty("payload", out var pe) && pe.ValueKind == JsonValueKind.Object)
            {
                foreach (var p in pe.EnumerateObject()) payload[p.Name] = JsonValue(p.Value);
            }
            updates.Add(new BrokerUpdate(ReadString(item, "external_order_id", ""), ReadString(item, "state", ""), ReadString(item, "event_ts", ""), payload));
        }
        return GatewayResult<UpdatesEnvelope>.Ok(new UpdatesEnvelope(updates, ReadString(response.Root, "next_cursor", "")));
    }

    private GatewayResult<FillsEnvelope> StreamFillsGateway(string? cursor, int? limit)
    {
        var response = SendJson("GET", "/stream/fills", null, null, includeApiPrefix: true, query: new Dictionary<string, string> { ["cursor"] = string.IsNullOrWhiteSpace(cursor) ? "" : cursor.Trim(), ["limit"] = Math.Max(1, limit.GetValueOrDefault(500)).ToString(CultureInfo.InvariantCulture) });
        if (!response.Ok) return GatewayResult<FillsEnvelope>.Fail(response.StatusCode, response.ErrorCode, response.ErrorMessage);
        if (!response.Root.TryGetProperty("fills", out var fillsElement) || fillsElement.ValueKind != JsonValueKind.Array) return GatewayResult<FillsEnvelope>.Fail(502, "connector_protocol_error", "connector fills response is missing fills array");
        var fills = new List<BrokerFill>();
        foreach (var item in fillsElement.EnumerateArray()) fills.Add(new BrokerFill(ReadString(item, "external_order_id", ""), ReadString(item, "fill_id", ""), ReadInt(item, "qty", 0), ReadDouble(item, "price", 0), ReadDouble(item, "fee", 0), ReadString(item, "fill_ts", "")));
        return GatewayResult<FillsEnvelope>.Ok(new FillsEnvelope(fills, ReadString(response.Root, "next_cursor", "")));
    }

    private GatewayResult<SubmitAck> SubmitFinam(SubmitIntentRequest request, string? idempotencyKey)
    {
        var intentId = (request.IntentId ?? "").Trim();
        if (string.IsNullOrWhiteSpace(intentId) || request.Intent.ValueKind != JsonValueKind.Object) return GatewayResult<SubmitAck>.Fail(400, "invalid_submit_payload", "intent_id and intent object are required");
        lock (_gate)
        {
            var gate = EnsureFinamReady<SubmitAck>(); if (gate is not null) return gate;
            var key = string.IsNullOrWhiteSpace(idempotencyKey) ? intentId : idempotencyKey.Trim();
            if (_submitCache.TryGetValue(key, out var cached)) return GatewayResult<SubmitAck>.Ok(cached);
            var externalOrderId = StableId("finam", intentId);
            _externalOrderIdByIntent[intentId] = externalOrderId;
            var ack = new SubmitAck(intentId, externalOrderId, true, "stocksharp-sidecar", "submitted");
            _submitCache[key] = ack;
            _updates.Add(new BrokerUpdate(externalOrderId, "submitted", UtcNow(), new Dictionary<string, object?> { ["intent_id"] = intentId }));
            return GatewayResult<SubmitAck>.Ok(ack);
        }
    }

    private GatewayResult<CancelAck> CancelFinam(string intentId, CancelIntentRequest request, string? idempotencyKey)
    {
        var normalized = (intentId ?? "").Trim();
        if (string.IsNullOrWhiteSpace(normalized)) return GatewayResult<CancelAck>.Fail(400, "invalid_cancel_payload", "intent_id path parameter is required");
        lock (_gate)
        {
            var gate = EnsureFinamReady<CancelAck>(); if (gate is not null) return gate;
            var canceledAt = string.IsNullOrWhiteSpace(request.CanceledAt) ? UtcNow() : request.CanceledAt.Trim();
            var key = string.IsNullOrWhiteSpace(idempotencyKey) ? $"cancel:{normalized}:{canceledAt}" : idempotencyKey.Trim();
            if (_cancelCache.TryGetValue(key, out var cached)) return GatewayResult<CancelAck>.Ok(cached);
            if (!_externalOrderIdByIntent.TryGetValue(normalized, out var externalOrderId)) return GatewayResult<CancelAck>.Fail(404, "unknown_intent_id", $"unknown intent_id: {normalized}");
            var ack = new CancelAck(normalized, externalOrderId, "canceled", canceledAt);
            _cancelCache[key] = ack;
            _updates.Add(new BrokerUpdate(externalOrderId, "canceled", canceledAt, new Dictionary<string, object?> { ["intent_id"] = normalized }));
            return GatewayResult<CancelAck>.Ok(ack);
        }
    }

    private GatewayResult<ReplaceAck> ReplaceFinam(string intentId, ReplaceIntentRequest request, string? idempotencyKey)
    {
        var normalized = (intentId ?? "").Trim();
        if (string.IsNullOrWhiteSpace(normalized)) return GatewayResult<ReplaceAck>.Fail(400, "invalid_replace_payload", "intent_id path parameter is required");
        if (!request.NewQty.HasValue || !request.NewPrice.HasValue || request.NewQty.Value <= 0 || request.NewPrice.Value <= 0) return GatewayResult<ReplaceAck>.Fail(400, "invalid_replace_payload", "new_qty/new_price must be positive");
        lock (_gate)
        {
            var gate = EnsureFinamReady<ReplaceAck>(); if (gate is not null) return gate;
            var replacedAt = string.IsNullOrWhiteSpace(request.ReplacedAt) ? UtcNow() : request.ReplacedAt.Trim();
            var key = string.IsNullOrWhiteSpace(idempotencyKey) ? $"replace:{normalized}:{replacedAt}" : idempotencyKey.Trim();
            if (_replaceCache.TryGetValue(key, out var cached)) return GatewayResult<ReplaceAck>.Ok(cached);
            if (!_externalOrderIdByIntent.TryGetValue(normalized, out var externalOrderId)) return GatewayResult<ReplaceAck>.Fail(404, "unknown_intent_id", $"unknown intent_id: {normalized}");
            var ack = new ReplaceAck(normalized, externalOrderId, "replaced", request.NewQty.Value, request.NewPrice.Value, replacedAt);
            _replaceCache[key] = ack;
            _updates.Add(new BrokerUpdate(externalOrderId, "replaced", replacedAt, new Dictionary<string, object?> { ["intent_id"] = normalized, ["new_qty"] = request.NewQty.Value, ["new_price"] = request.NewPrice.Value }));
            return GatewayResult<ReplaceAck>.Ok(ack);
        }
    }

    private GatewayResult<UpdatesEnvelope> StreamUpdatesFinam(string? cursor, int? limit)
    {
        lock (_gate)
        {
            var gate = EnsureFinamReady<UpdatesEnvelope>(); if (gate is not null) return gate;
            var start = ParseCursor(cursor, _updates.Count); var safeLimit = limit.GetValueOrDefault(500); if (safeLimit <= 0) safeLimit = 500;
            var rows = _updates.Skip(start).Take(safeLimit).ToList();
            return GatewayResult<UpdatesEnvelope>.Ok(new UpdatesEnvelope(rows, (start + rows.Count).ToString(CultureInfo.InvariantCulture)));
        }
    }

    private GatewayResult<FillsEnvelope> StreamFillsFinam(string? cursor, int? limit)
    {
        lock (_gate)
        {
            var gate = EnsureFinamReady<FillsEnvelope>(); if (gate is not null) return gate;
            var start = ParseCursor(cursor, _fills.Count); var safeLimit = limit.GetValueOrDefault(500); if (safeLimit <= 0) safeLimit = 500;
            var rows = _fills.Skip(start).Take(safeLimit).ToList();
            return GatewayResult<FillsEnvelope>.Ok(new FillsEnvelope(rows, (start + rows.Count).ToString(CultureInfo.InvariantCulture)));
        }
    }

    private GatewayResult<T>? EnsureFinamReady<T>()
    {
        var snapshot = ProbeFinamSessionHealth();
        if (snapshot.ConnectorReady) return null;
        var code = string.IsNullOrWhiteSpace(snapshot.ErrorCode) ? "connector_not_ready" : snapshot.ErrorCode.Trim();
        return GatewayResult<T>.Fail(503, code, $"broker connector is not ready: {code}");
    }

    private ConnectorHealthSnapshot CacheAndReturn(ConnectorHealthSnapshot snapshot, DateTimeOffset at) { _cachedFinamSnapshot = snapshot; _cachedFinamSnapshotAt = at; return snapshot; }
    private ConnectorHealthSnapshot Unhealthy(string errorCode, int queued) => new(_route, false, _expectedConnectorMode, _expectedConnectorBackend, queued, "", "", "", string.IsNullOrWhiteSpace(errorCode) ? "connector_not_ready" : errorCode.Trim());

    private ConnectorHttpResponse SendJson(string method, string path, object? payload, string? idempotencyKey, bool includeApiPrefix, IReadOnlyDictionary<string, string>? query)
    {
        if (_connectorBaseUrl is null) return ConnectorHttpResponse.Fail(503, "connector_not_configured", "connector base url is not configured", null);
        try
        {
            var uri = BuildUri(path, includeApiPrefix, query);
            using var request = new HttpRequestMessage(new HttpMethod(method), uri);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            request.Headers.UserAgent.ParseAdd("stocksharp-sidecar-gateway/1.0");
            if (!string.IsNullOrWhiteSpace(_authHeaderName) && !string.IsNullOrWhiteSpace(_authToken)) request.Headers.TryAddWithoutValidation(_authHeaderName, _authToken);
            if (!string.IsNullOrWhiteSpace(idempotencyKey)) request.Headers.TryAddWithoutValidation("X-Idempotency-Key", idempotencyKey.Trim());
            if (payload is not null) request.Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
            using var response = _httpClient.Send(request);
            var status = (int)response.StatusCode;
            var raw = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            JsonDocument? doc = null;
            if (!string.IsNullOrWhiteSpace(raw)) doc = JsonDocument.Parse(raw);
            if (status < 200 || status > 299) return ConnectorHttpResponse.Fail(status, ReadString(doc?.RootElement ?? default, "error_code", "connector_http_error"), ReadString(doc?.RootElement ?? default, "message", $"connector request failed with status {status}"), doc);
            return ConnectorHttpResponse.Success(status, doc);
        }
        catch (Exception exc)
        {
            return ConnectorHttpResponse.Fail(503, "connector_unreachable", $"connector request failed: {exc.GetType().Name}: {exc.Message}", null);
        }
    }

    private Uri BuildUri(string path, bool includeApiPrefix, IReadOnlyDictionary<string, string>? query)
    {
        var normalized = path.StartsWith("/") ? path : "/" + path;
        var prefix = includeApiPrefix ? "/" + _apiPrefix : "";
        var builder = new UriBuilder(new Uri(_connectorBaseUrl!, $"{prefix}{normalized}"));
        if (query is not null && query.Count > 0)
        {
            builder.Query = string.Join("&", query.Where(i => !string.IsNullOrWhiteSpace(i.Key)).Select(i => $"{Uri.EscapeDataString(i.Key)}={Uri.EscapeDataString(i.Value ?? "")}"));
        }
        return builder.Uri;
    }

    private static JsonElement TryGetAck(JsonElement root) => root.TryGetProperty("ack", out var ack) && ack.ValueKind == JsonValueKind.Object ? ack : root;
    private static string ReadString(JsonElement element, string name, string fallback) => element.ValueKind == JsonValueKind.Object && element.TryGetProperty(name, out var value) ? (value.ValueKind == JsonValueKind.String ? (value.GetString() ?? "").Trim() : value.ToString()) switch { "" => fallback, var v => v } : fallback;
    private static bool ReadBool(JsonElement element, string name, bool fallback) => element.ValueKind == JsonValueKind.Object && element.TryGetProperty(name, out var value) ? value.ValueKind switch { JsonValueKind.True => true, JsonValueKind.False => false, JsonValueKind.String => (value.GetString() ?? "").Trim().ToLowerInvariant() is "1" or "true" or "yes" or "on", _ => fallback } : fallback;
    private static int ReadInt(JsonElement element, string name, int fallback) => element.ValueKind == JsonValueKind.Object && element.TryGetProperty(name, out var value) && ((value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var parsed)) || (value.ValueKind == JsonValueKind.String && int.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed))) ? parsed : fallback;
    private static double ReadDouble(JsonElement element, string name, double fallback) => element.ValueKind == JsonValueKind.Object && element.TryGetProperty(name, out var value) && ((value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var parsed)) || (value.ValueKind == JsonValueKind.String && double.TryParse(value.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out parsed))) ? parsed : fallback;
    private static IReadOnlyList<string> ReadStringList(JsonElement element, string name) => element.ValueKind != JsonValueKind.Object || !element.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.Array ? Array.Empty<string>() : value.EnumerateArray().Where(i => i.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(i.GetString())).Select(i => i.GetString()!.Trim()).ToArray();
    private static object? JsonValue(JsonElement value) => value.ValueKind switch { JsonValueKind.String => value.GetString(), JsonValueKind.Number when value.TryGetInt64(out var i64) => i64, JsonValueKind.Number when value.TryGetDouble(out var dbl) => dbl, JsonValueKind.True => true, JsonValueKind.False => false, JsonValueKind.Null => null, JsonValueKind.Object => value.EnumerateObject().ToDictionary(i => i.Name, i => JsonValue(i.Value)), JsonValueKind.Array => value.EnumerateArray().Select(JsonValue).ToArray(), _ => value.ToString(), };
    private static int ParseCursor(string? cursor, int count) { if (!int.TryParse(cursor, out var start)) start = 0; if (start < 0) start = 0; if (start > count) start = count; return start; }
    private static bool IsFinamHost(Uri? uri) { var host = uri?.Host?.Trim().ToLowerInvariant() ?? ""; return host.EndsWith("finam.ru", StringComparison.Ordinal); }
    private static string NormalizePath(string path) => path.Trim().StartsWith("/") ? path.Trim() : "/" + path.Trim();
    private static string UtcNow() => DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
    private static string StableId(string prefix, string seed) { using var sha = SHA256.Create(); var digest = sha.ComputeHash(Encoding.UTF8.GetBytes(string.IsNullOrWhiteSpace(seed) ? $"{prefix}:{UtcNow()}" : seed.Trim())); return $"{prefix}-{Convert.ToHexString(digest).ToLowerInvariant()[..16]}"; }

    private sealed class ConnectorHttpResponse
    {
        private ConnectorHttpResponse(int statusCode, string errorCode, string errorMessage, JsonDocument? payload) { StatusCode = statusCode; ErrorCode = errorCode; ErrorMessage = errorMessage; Payload = payload; }
        public int StatusCode { get; }
        public string ErrorCode { get; }
        public string ErrorMessage { get; }
        public JsonDocument? Payload { get; }
        public bool Ok => StatusCode >= 200 && StatusCode <= 299;
        public JsonElement Root => Payload is null ? default : Payload.RootElement;
        public static ConnectorHttpResponse Success(int statusCode, JsonDocument? payload) => new(statusCode, "", "", payload);
        public static ConnectorHttpResponse Fail(int statusCode, string errorCode, string errorMessage, JsonDocument? payload) => new(statusCode <= 0 ? 503 : statusCode, string.IsNullOrWhiteSpace(errorCode) ? "connector_http_error" : errorCode.Trim(), string.IsNullOrWhiteSpace(errorMessage) ? "connector request failed" : errorMessage.Trim(), payload);
    }
}
