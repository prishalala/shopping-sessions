"""Exact CSV reader and timestamp decoder for the reference ingestion path."""

import re
from datetime import date, datetime
from pathlib import Path

DAY = 86_400_000
EPOCH = date(1970, 1, 1).toordinal()
ISO = re.compile(
    rb"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|[+-][0-9]{2}:[0-9]{2})"
)


def number(value, max_digits):
    if not value.isdigit():
        return None
    value = value.lstrip(b"0")
    if len(value) <= max_digits:
        return int(value or b"0")
    return None


def timestamp(value):
    if value.isdigit():
        return number(value, 18)
    match = ISO.fullmatch(value)
    if not match:
        return None
    try:
        dt = datetime(*map(int, match.groups()[:6]))
    except ValueError:
        return None
    zone = match[7]
    offset = 0
    if zone != b"Z":
        h, m = int(zone[1:3]), int(zone[4:])
        if h > 23 or m > 59:
            return None
        offset = (h * 60 + m) * (1 if zone[:1] == b"+" else -1)
    return (dt.toordinal() - EPOCH) * DAY + (
        (dt.hour * 60 + dt.minute - offset) * 60 + dt.second
    ) * 1000


def rows(paths, width):
    for path in paths:
        with Path(path).open("rb") as f:
            next(f, None)
            for line in f:
                fields = line.removesuffix(b"\n").split(b",")
                if len(fields) != width:
                    continue
                for i, field in enumerate(fields):
                    if b'"' in field:
                        if (
                            len(field) < 2
                            or field[0] != 34
                            or field[-1] != 34
                            or b'"' in field[1:-1]
                        ):
                            break
                        fields[i] = field[1:-1]
                else:
                    yield fields


def identities(paths):
    parent = {}

    def root(x):
        while x != parent.setdefault(x, x):
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in rows(paths, 2):
        if a and b:
            a, b = root(a), root(b)
            parent[max(a, b)] = min(a, b)
    return {x: root(x) for x in parent}


def read_data(data, start, end):
    import polars as pl

    person = identities(data.glob("links/*.csv"))
    products = dict(rows([data / "products.csv"], 2))
    campaigns = {k.decode(): v.decode() for k, v in rows([data / "campaigns.csv"], 2)}
    winners = {}
    for key, user, ts, ingest, kind, product, price in rows(
        data.glob("events/*.csv"), 7
    ):
        if key and (key not in winners or ingest < winners[key][0]):
            winners[key] = (ingest, user, ts, kind, product, price)

    def events():
        for ingest, user, raw, kind, product, price in winners.values():
            if not user or not product or kind not in (b"view", b"cart", b"purchase"):
                continue
            ts = timestamp(raw)
            if ts is None or not start <= ts < end or timestamp(ingest) - ts > DAY:
                continue
            revenue = 0
            category = None
            if kind == b"purchase":
                revenue = number(price, 8)
                if revenue is None or not 1 <= revenue <= 10_000_000:
                    continue
                category = products.get(product, b"unknown").decode()
            yield person.get(user, user).decode(), ts, revenue, category

    frame = pl.DataFrame(
        events(),
        schema={
            "user_id": pl.String,
            "ts": pl.Int64,
            "revenue": pl.Int64,
            "category": pl.String,
        },
        orient="row",
    )
    winners.clear()
    seen = set()

    def clicks():
        for key, user, campaign, raw in rows(data.glob("clicks/*.csv"), 4):
            if not key or key in seen:
                continue
            seen.add(key)
            ts = timestamp(raw)
            if (
                user
                and campaign.decode() in campaigns
                and ts is not None
                and start - 7 * DAY <= ts < end
            ):
                yield (
                    person.get(user, user).decode(),
                    ts,
                    key.decode(),
                    campaign.decode(),
                )

    click_frame = pl.DataFrame(
        clicks(),
        schema={
            "user_id": pl.String,
            "ts": pl.Int64,
            "click_id": pl.String,
            "campaign_id": pl.String,
        },
        orient="row",
    )
    return (
        frame,
        click_frame,
        pl.DataFrame(
            list(campaigns.items()),
            schema={"campaign_id": pl.String, "channel": pl.String},
            orient="row",
        ),
    )
