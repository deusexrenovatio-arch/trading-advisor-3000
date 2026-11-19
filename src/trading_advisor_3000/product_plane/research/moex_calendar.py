"""Read explicit futures-market day flags from retained MOEX calendar responses."""

from __future__ import annotations

import json
from datetime import date
from html.parser import HTMLParser
from urllib.parse import parse_qs

from trading_advisor_3000.product_plane.common.immutable_files import pinned_bytes
from trading_advisor_3000.product_plane.contracts.identity_v3 import _Evaluator


def packet_bytes(root, reference):
    evaluator = _Evaluator({})
    evaluator.check(
        evaluator._document("market_identity.v3.json")["$defs"]["native_research_read_pin"],
        reference,
    )
    return pinned_bytes(root, reference)


def _unique_object(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("duplicate key in MOEX calendar source")
    return result


def calendar_json(raw):
    return json.loads(raw, object_pairs_hook=_unique_object)


class _CalendarPage(HTMLParser):
    def __init__(self):
        super().__init__()
        self.payloads = []
        self.in_payload = False

    def handle_starttag(self, tag, attrs):
        self.in_payload = tag == "script" and dict(attrs).get("id") == "__NEXT_DATA__"
        if self.in_payload:
            self.payloads.append("")

    def handle_data(self, data):
        if self.in_payload:
            self.payloads[-1] += data

    def handle_endtag(self, tag):
        if tag == "script":
            self.in_payload = False


def futures_workdays(raw, url):
    if url.hostname == "iss.moex.com" and url.path == "/iss/calendars.json":
        if parse_qs(url.query).get("show_all_days") != ["1"]:
            raise ValueError("MOEX futures calendar requires show_all_days=1")
        block = calendar_json(raw)["off_days"]
        columns = block["columns"]
        if len(columns) != len(set(columns)):
            raise ValueError("duplicate MOEX calendar source columns")
        rows = [dict(zip(columns, row, strict=True)) for row in block["data"]]
        pairs = [(row["tradedate"], row["futures_workday"]) for row in rows]
    elif url.hostname == "www.moex.com" and url.path.rstrip("/") == "/ru/tradingcalendar":
        page = _CalendarPage()
        page.feed(raw.decode("utf-8"))
        if len(page.payloads) != 1:
            raise ValueError("expected one MOEX trading calendar page payload")
        props = calendar_json(page.payloads[0])["props"]["pageProps"]
        calendars = [
            v["initData"]["offDays"]["futures"]
            for v in props["recursiveComponentContentProps"].values()
            if "offDays" in v.get("initData", {})
        ]
        if len(calendars) != 1:
            raise ValueError("expected one dated MOEX futures calendar")
        pairs = []
        for year, dates in calendars[0].items():
            for day, record in dates.items():
                if record["tradedate"] != day or day[:4] != year:
                    raise ValueError(f"MOEX futures calendar date mismatch: {day}")
                pairs.append((day, record["is_traded"]))
    else:
        raise ValueError("unrecognized MOEX futures calendar source URL")
    result = {}
    for day, flag in pairs:
        if day in result or date.fromisoformat(day).isoformat() != day:
            raise ValueError(f"duplicate or invalid MOEX futures date: {day}")
        if type(flag) is not int or flag not in (0, 1):
            raise ValueError(f"unknown MOEX futures workday flag: {day}")
        result[day] = "open" if flag else "closed"
    return result
