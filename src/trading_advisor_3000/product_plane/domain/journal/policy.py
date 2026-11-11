from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from trading_advisor_3000.product_plane.contracts.trade_workspace import (
    ChangeTrade,
    Entry,
    ExecutionSummary,
    Position,
    ReplaceCard,
    SetStatus,
    Snapshot,
)

TERMINAL = {"closed", "cancelled", "invalidated"}
TRANSITIONS = {
    "draft": {"planned", "active", "cancelled", "invalidated"},
    "planned": {"draft", "active", "cancelled", "invalidated"},
    "active": {"closed"},
    "closed": set(),
    "cancelled": set(),
    "invalidated": set(),
}


class JournalError(Exception):
    def __init__(self, code: str, message: str, status: int = 409):
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


def project(entries: list[Entry]) -> tuple[list[Position], ExecutionSummary]:
    superseded = {entry.supersedes_entry_id for entry in entries if entry.supersedes_entry_id}
    facts = [entry for entry in entries if entry.entry_id not in superseded and entry.execution]
    contracts = {entry.execution.contract_code for entry in facts if entry.execution is not None}
    accounts = {entry.execution.account.value for entry in facts if entry.execution is not None}
    if len(contracts) > 1 or len(accounts) > 1:
        raise JournalError("episode_scope", "One episode requires one contract and account context")
    quantity, completed = 0, False
    for entry in sorted(facts, key=lambda item: (item.event_at, item.sequence)):
        fact = entry.execution
        assert fact is not None
        if fact.action == "increase":
            if completed:
                raise JournalError("reentry_requires_new_trade", "Create a linked new episode")
            quantity += fact.contracts
        else:
            quantity -= fact.contracts
            if quantity < 0:
                raise JournalError("excess_reduction", "Reduction exceeds recorded position")
            if quantity == 0:
                completed = True
    if not facts:
        return [], ExecutionSummary(status="not_recorded", total_contracts=None, reason="no_facts")
    return [Position(contract_code=next(iter(contracts)), contracts=quantity)], ExecutionSummary(
        status="recorded", total_contracts=quantity, reason=None
    )


def apply_change(
    old: Snapshot, command: ChangeTrade, entries: list[Entry], now: datetime
) -> tuple[Snapshot, list[Entry]]:
    new = old.model_copy(deep=True)
    new_entries: list[Entry] = []
    active = {entry.entry_id: entry for entry in entries}
    for entry in entries:
        if entry.supersedes_entry_id is not None:
            active.pop(entry.supersedes_entry_id, None)
    if len(entries) + sum(op.kind == "add_entry" for op in command.operations) > 5000:
        raise JournalError("entry_limit", "Maximum 5000 entries per card", 422)
    correcting_status = any(op.kind == "correct_status" for op in command.operations)
    if sum(op.kind in {"set_status", "correct_status"} for op in command.operations) > 1:
        raise JournalError("status_batch", "Only one status operation per command", 422)
    for operation in command.operations:
        if isinstance(operation, ReplaceCard):
            if operation.card.related_trade_id != old.card.related_trade_id:
                raise JournalError("immutable_relation", "Episode relationship is immutable")
            if operation.card.relation != old.card.relation:
                raise JournalError("immutable_relation", "Episode relationship is immutable")
            if any(entry.execution for entry in entries):
                previous_analysis = old.card.analysis
                next_analysis = operation.card.analysis
                if (
                    next_analysis.symbol,
                    next_analysis.asset,
                    next_analysis.market_type,
                    next_analysis.price_unit,
                ) != (
                    previous_analysis.symbol,
                    previous_analysis.asset,
                    previous_analysis.market_type,
                    previous_analysis.price_unit,
                ) or operation.card.direction != old.card.direction:
                    raise JournalError(
                        "immutable_identity", "Executed episode identity cannot change"
                    )
            new.card = operation.card
        elif isinstance(operation, SetStatus):
            if operation.kind == "set_status" and operation.status != old.status:
                if operation.status not in TRANSITIONS[old.status]:
                    raise JournalError(
                        "invalid_transition", "Status requires correction or new episode"
                    )
            new.status = operation.status
        else:
            payload = operation.entry
            target = None
            if payload.supersedes_entry_id:
                target = active.get(payload.supersedes_entry_id)
                if not target or target.kind != payload.kind:
                    raise JournalError(
                        "invalid_supersedes", "Correction must reference active same-kind entry"
                    )
                active.pop(target.entry_id)
            if payload.execution and payload.execution.action == "increase":
                corrects_old_increase = bool(
                    target and target.execution and target.execution.action == "increase"
                )
                if (old.status in TERMINAL or correcting_status) and not corrects_old_increase:
                    raise JournalError("reentry_requires_new_trade", "Create a linked new episode")
            item = Entry(
                **payload.model_dump(),
                entry_id=str(uuid4()),
                trade_id=old.trade_id,
                revision=old.revision + 1,
                sequence=len(entries) + len(new_entries) + 1,
                recorded_at=now,
            )
            active[item.entry_id] = item
            new_entries.append(item)
    positions, summary = project(entries + new_entries)
    quantity = summary.total_contracts or 0
    if (new.status == "active") != (quantity > 0):
        raise JournalError(
            "status_position_mismatch", "Active requires open facts; other statuses require flat"
        )
    if new.status == "closed" and summary.status != "recorded":
        raise JournalError("closed_without_execution", "Closed requires execution facts")
    new.positions, new.execution_summary = positions, summary
    new.entry_ids += [entry.entry_id for entry in new_entries]
    new.revision += 1
    new.updated_at, new.event_at = now, command.event_at
    new.source, new.reason = command.source, command.reason
    return new, new_entries
