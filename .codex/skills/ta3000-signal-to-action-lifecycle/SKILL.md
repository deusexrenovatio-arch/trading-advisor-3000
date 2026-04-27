---
name: ta3000-signal-to-action-lifecycle
description: Use for TA3000 product-plane work that turns validated strategy outputs into a reliable signal, alert, paper-trade, semi-automated, or live-execution delivery chain, including Telegram-style advisory signals, webhooks, auditability, risk gates, and operator workflow.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-COMPUTE
scope: TA3000 strategy output delivery from signal to action
routing_triggers:
  - signal delivery
  - Telegram signal
  - trading signal
  - webhook
  - alert delivery
  - paper trading
  - live trading
  - robot lifecycle
  - execution chain
  - signal-to-action
---

# TA3000 Signal To Action Lifecycle

## When To Use
- A strategy output must become a user-facing signal, Telegram alert, dashboard event, webhook, paper trade, semi-auto flow, or live robot action.
- The user is deciding whether the product output is an advisory signal or an executable robot.
- Work touches signal payloads, delivery channels, risk gates, idempotency, audit logs, monitoring, or operator controls.

Use with `ta3000-backtest-validation-and-overfit-control` before promoting a research result beyond research-only status.

## Knowledge Entry Points
Use external references when designing the delivery chain:
- Algorithm framework separation of alpha, portfolio, risk, and execution: `https://www.quantconnect.com/docs/v2/writing-algorithms/algorithm-framework/overview`
- Portfolio targets from signals/insights: `https://www.quantconnect.com/docs/v1/algorithm-framework/portfolio-construction`
- TradingView webhook behavior, JSON/plain-text body, timeouts, and delivery monitoring: `https://www.tradingview.com/support/solutions/43000529348-how-to-configure-webhook-alerts/`
- Telegram Bot API or chosen channel docs when implementing channel-specific delivery.

## Core Principle
Do not make "robot" the default output. Model a strategy output as a decision event that can be routed to different modes:

- `research_event`: for internal analysis only.
- `advisory_signal`: human-readable signal, such as Telegram or dashboard.
- `paper_signal`: advisory signal tracked as if acted on.
- `paper_trade`: simulated order target.
- `semi_auto`: human approval before execution.
- `full_auto`: broker/exchange order path with risk gates.

The same strategy can move between modes only through explicit validation and operational approval.

## Signal Contract
Every delivered signal should carry enough context to be auditable:
- stable `signal_id` for idempotency;
- strategy id and version;
- instrument and venue;
- signal timestamp and source bar timestamp;
- timeframe and clock profile;
- direction and action: watch, enter, exit, reduce, invalidate, no-trade;
- confidence or rank when meaningful;
- setup reason in user-readable terms;
- key levels: entry zone, invalidation, stop, target, or time stop when defined;
- risk notes and no-trade filters;
- data freshness and missing-data status;
- delivery mode and intended audience;
- expiration time or validity window.

## Delivery Chain
1. Strategy produces a causal event after the source bar is closed.
2. Validator checks required columns, data freshness, duplicate signal id, no-trade filters, and mode eligibility.
3. Router chooses channel: internal report, Telegram, dashboard, webhook, paper, semi-auto, or live.
4. Renderer formats the payload for the channel without changing trading semantics.
5. Delivery layer sends the message or order intent and records status.
6. Audit store captures payload, delivery result, retries, operator action, and later outcome.
7. Monitor reports stale feeds, failed delivery, duplicate signals, and unexpected silence.

## Advisory Signal Rules
- Prefer clarity over urgency: direction, instrument, timeframe, reason, invalidation, and validity window must be understandable.
- Do not imply execution or position size unless the mode includes portfolio/risk logic.
- Include "wait" and "invalidate" events when they are part of the strategy behavior.
- Preserve the difference between setup detected, entry triggered, exit triggered, and setup invalidated.
- Long-horizon strategies can use digest/batch delivery instead of noisy tick-by-tick alerts.

## Robot / Execution Rules
- Add portfolio construction, risk management, and execution only when the output mode requires them.
- Use risk gates before execution: max position, max daily loss, max concentration, stale data, duplicate order, market closed, and kill switch.
- Keep paper and live routing visibly separate.
- Treat live enablement as an operational decision, not as a side effect of a passing backtest.

## Done Criteria
- Output mode is explicit and not assumed.
- Signal payload has idempotency, timing, reason, validity, and audit fields.
- Channel-specific formatting is separated from strategy semantics.
- Delivery failures and duplicate signals are observable.
- Promotion from research to advisory, paper, semi-auto, or live has a named evidence gate.
