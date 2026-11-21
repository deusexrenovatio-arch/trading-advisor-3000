"""Read explicit futures-market day flags from retained MOEX calendar responses."""

from __future__ import annotations

import json
from calendar import isleap
from datetime import date, timedelta
from hashlib import sha256
from html.parser import HTMLParser
from urllib.parse import parse_qs

from trading_advisor_3000.product_plane.common.immutable_files import pinned_bytes
from trading_advisor_3000.product_plane.contracts.identity_v3 import _Evaluator

ARCHIVE_PREFIX = "moex-futures-workdays-archive-v1-"
REVIEWED_LAST_TRADE_CHANGES = {
    "BRN0": (
        "BR-7.20",
        "2020-07-01",
        "2020-07-02",
        "n28692",
        "f1625cc193bac722b0f8156722b3f02d65c3f1a7092879af0590c841d941efec",
    ),
    "BRF1": (
        "BR-1.21",
        "2020-12-31",
        "2021-01-04",
        "n30741",
        "d77036c415d8191fe72fb1026cab2c2834e93e8b09d9627f1afaf798109df8d6",
    ),
    "BRK1": (
        "BR-5.21",
        "2021-05-03",
        "2021-05-04",
        "n30741",
        "355809c2ee4bf808f80f993166e244a014cf6c2c9350c11ab42140c9db9df594",
    ),
    "NGK0": (
        "NG-5.20",
        "2020-05-28",
        "2020-05-27",
        "n28386",
        "03de3a58fbdd39016360a009f53391c958b98f9a15830b1f5b127b14fcc977bf",
    ),
    "NGM0": (
        "NG-6.20",
        "2020-06-29",
        "2020-06-26",
        "n28386",
        "4c6ba44ef8eee2bb95bf5c9beb97a99548d63f8d30b4dfc241150993939ae3ad",
    ),
    "NGN0": (
        "NG-7.20",
        "2020-07-30",
        "2020-07-29",
        "n28386",
        "1318400337498dc2f21c3ac52c47e2e6aa2c62652055b2daba6be89354c29a6b",
    ),
    "NGQ0": (
        "NG-8.20",
        "2020-08-28",
        "2020-08-27",
        "n28386",
        "498ab99ba9ad605403f5d57dda4d063af98153e763931c6ba0e27a986d4bd070",
    ),
    "NGU0": (
        "NG-9.20",
        "2020-09-29",
        "2020-09-28",
        "n28386",
        "5c5623b79c82cb35771d412c77a861bb1a6055ff4330b83b642d1bcf046caca3",
    ),
    "NGV0": (
        "NG-10.20",
        "2020-10-29",
        "2020-10-28",
        "n28386",
        "580cca371fa65bc46583f9708e2db949a96203644bfa0ce3e666b646227b72fb",
    ),
    "NGX0": (
        "NG-11.20",
        "2020-11-26",
        "2020-11-25",
        "n28386",
        "df3d22df489fe802fbb48120d6e066dc4955ae2476d6de0f3e9903ad0cc2e35f",
    ),
}
LAST_TRADE_NOTICES = {
    "n28386": "fa6a4d53a099947df2d62451e4eed52eb98cb96ae1682945b54545120aca2c4f",
    "n28692": "d8b11a18b611b93ffbef9cfc0a00bfcc6c53cdf4988a474d631bb24cd355b7ac",
    "n30741": "72ec0dbd7bd314c911f711c815e28fb8da0b5f3127fafaab8b20faad814e6c5f",
}
# Exact annual ISS response and digest of the reviewed open dates.
REVIEWED_ARCHIVES = {
    "2018": (
        "be9b5c41d80cd2b39e0806d72ea28996fff495669e5601912a3ab635d07cbf36",
        "bab97dfa4aab979615f4d27921e4b3e328d99179bb7107a92b11794d5d96a6cb",
    ),
    "2019": (
        "5d75746db1ffa13635020128a11dd293f064b8a5a9c8885f071f4cb53f68c4e3",
        "6ca810189f12a45a15b1ddfcbafef5af7ab514f1b28864a8707cd712908364ef",
    ),
    "2020": (
        "1ff54a934694b92be41db4ecdec0b310498aeaac9462a74be594546839f84d0f",
        "cda041d02c1e642a35fad2fb485792e49defc16782b6616648c25781579a33b9",
    ),
    "2021": (
        "93eee77d1f32995acf71cef903effd6a2cdac127eb2296bdb9b8cd02bd40bc8a",
        "b501d41bd38ab4d28a30f312cd6a990deb71f4a5e8d4754058e7695e8a993600",
    ),
    "2023": (
        "8052827547b133620336174dd1893ddb2e8e6424d5ac2dad0d50ed51eac56257",
        "31165b03b80ae98ede4844ee5cf15537e6ca8009ebe489720e3f1614f8f525c2",
    ),
    "2024": (
        "e46bc290c582f4a594c01e21b1bdef85bc9df80197cc92d2bf041e361f6bc077",
        "78533ec2bce4068265065eab08414f3795a3b5588b7cf56d80d0d54e5a3ff415",
    ),
}

ARCHIVE_NOTICE_DIGESTS = {
    "n18128": "cdf03b38458bdbb1109666534a942fec0bb8c2cf8a55fa11e4dbafb5e01601c9",
    "n18635": "a8e586a70b96dad3036837002ad1ed81d46b5dab531dbca2ace3a639def6d870",
    "n19363": "cec90e2acdd8b0225adb49dd33f2a68cea1004b494f6a44c162b3e2b6b388a8e",
    "n19729": "e200b89d732ba31b5f9610d41f138489100cb4cd15e68ca29a21b84f27d3654b",
    "n21311": "97987db80ebb8c6be7c45a23bf0cea8ccdb967c5f5a67ec5a895b6a27018a6b4",
    "n22112": "ccf09ca3ef04722155294ab071527eec4253d739a4e6018f651aa1d2db96701d",
    "n23388": "f7bf04ac9cc104033e22873a4769c80efd750c066c91cde7017bc1c0b4aa7f41",
    "n26438": "44fc8aa09bf1b1e8e621f8647044664effe484ad892d55ea9e47bbfc6ac8ef80",
    "n28298": "0da31302c322abdf4cca318050891ddf43ddcdd11b2ef2760b66c692ad2f1804",
    "n31902": "f2193974313da2200dd275428d09cbddd4093559984e2a87957240520f44d72e",
    "n32519": "a4a3e1f8354e137ed72905ca1c13bcc8eb4442d162976e1985ed15ad3a211b7f",
    "n33785": "2436648822f0b828a118c3ddaa38c9b58e85f00405d41589b61bf8ca67e1ae19",
    "n34597": "5e1b00d5c6e5a897d0c002d268b332215d03df44356deb005243033778a2fadb",
    "n37085": "af272ac48cbc777260221a89896356e5314fa0b8579119fc23672b14732fe466",
    "n51887": "b22938f526b5428aeed48578a799ca380d8f14c4e6d6c6a02016568e1912544b",
    "n64121": "e75694008951b38fc98c654dadd2670d05dfb2e7f7cd55aeeb428a0e6440fa7b",
}


def archive_notice_dates(raw, year):
    """Bind dated notices where the annual response lacks a market-specific flag."""
    blocks = calendar_json(raw)
    explicit = {day for _, day, engine, *_ in blocks["off_days"]["data"] if engine == "futures"}
    result = {}
    for _, day, kind, link in blocks["holidays"]["data"]:
        # These notices explicitly opened FORTS on the listed dates despite
        # a null type. The 31 May 2021 notice only concerned US shares.
        if day in explicit or (kind is None and link not in {"/n33785", "/n51887"}):
            continue
        # The 2024 response omits the link for seven dates expressly listed by MOEX.
        if link is None and year == "2024":
            link = "/n64121"
        result[day] = link.removeprefix("/")
    return result


def verify_archive_sources(year, rules, read_pair):
    if year not in REVIEWED_ARCHIVES:
        raise ValueError(f"unreviewed MOEX derivatives calendar year: {year}")
    prefix = ARCHIVE_PREFIX + year + "-"
    by_id = {r["rule_id"]: r for r in rules}
    annual_id = prefix + "events"
    annual = by_id.get(annual_id)
    if annual is None or annual["kind"] != "annual_calendar":
        raise ValueError(f"missing MOEX archive annual rule: {year}")
    raw, url = read_pair(annual)
    expected_url = f"https://iss.moex.com/iss/apps/calendar/events.json?year={year}&iss.only=holidays,off_days&iss.meta=off"
    if (
        annual["body"]["sha256"] != REVIEWED_ARCHIVES[year][0]
        or url.geturl() != expected_url
        or annual["effective_from"] != year + "-01-01"
        or annual["effective_to"] != f"{int(year) + 1}-01-01"
    ):
        raise ValueError(f"MOEX archive rule differs from reviewed original: {annual_id}")
    notices = archive_notice_dates(raw, year)
    if set(by_id) != {annual_id, *(prefix + notice for notice in notices.values())}:
        raise ValueError(f"missing or ambiguous MOEX archive rules: {year}")
    for notice in set(notices.values()):
        rule = by_id[prefix + notice]
        _, url = read_pair(rule)
        days = sorted(d for d, n in notices.items() if n == notice)
        if (
            rule["kind"] != "dated_schedule"
            or rule["body"]["sha256"] != ARCHIVE_NOTICE_DIGESTS[notice]
            or url.geturl() != "https://www.moex.com/" + notice
            or rule["precedence"] <= annual["precedence"]
            or rule["effective_from"] != days[0]
            or rule["effective_to"]
            != (date.fromisoformat(days[-1]) + timedelta(days=1)).isoformat()
        ):
            raise ValueError(f"MOEX archive rule differs from reviewed original: {rule['rule_id']}")
    return notices


def verify_archive_dates(year, dates, notices):
    prefix = ARCHIVE_PREFIX + year + "-"
    if len(dates) != 365 + isleap(int(year)):
        raise ValueError(f"incomplete MOEX derivatives calendar: {year}")
    for item in dates:
        day = item["source_date"]
        required = [prefix + "events"]
        if day in notices:
            required.append(prefix + notices[day])
        if item["governing_rule_ids"] != required:
            raise ValueError(f"wrong governing MOEX archive rules: {day}")
    opens = sorted(item["source_date"] for item in dates if item["state"] == "open")
    digest = sha256("".join(day + "\n" for day in opens).encode("ascii")).hexdigest()
    if digest != REVIEWED_ARCHIVES[year][1]:
        raise ValueError(f"planned dates disagree with reviewed MOEX derivatives calendar: {year}")


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
