"""Shopping Sessions scorer entry point."""

import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

os.environ.setdefault("POLARS_MAX_THREADS", os.environ.get("NCPU", "8"))
import polars as pl
from ingest import DAY, read_data, rows, timestamp

c = pl.col
WEEK = 7 * DAY


def asof(left, right, on, right_on, strategy, **kwargs):
    # Both inputs are ordered by person and time, including within duplicate times.
    return left.join_asof(
        right,
        left_on=on,
        right_on=right_on,
        by="user_id",
        strategy=strategy,
        check_sortedness=False,
        **kwargs,
    )


def aggregate(events, clicks, channels):
    people = pl.Categories.random()
    person_type = pl.Categorical(people)
    events = events.with_columns(c("user_id").cast(person_type))
    clicks = clicks.with_columns(c("user_id").cast(person_type))
    names = people.to_series()
    events = events.with_columns(c("user_id").to_physical()).sort("user_id", "ts")
    clicks = clicks.with_columns(c("user_id").to_physical())
    starts = (
        (c("user_id") != c("user_id").shift()) | (c("ts").diff() > 1_800_000)
    ).fill_null(True)
    events = events.with_columns(
        pl.when(starts).then(c("ts")).forward_fill().alias("start"),
        starts.cast(pl.UInt32).cum_sum().set_sorted().alias("session"),
    )
    events = events.with_columns(
        (c("start") // DAY).cast(pl.Int32).cast(pl.Date).alias("day")
    )
    sessions = (
        events.group_by("session", maintain_order=True)
        .agg(
            c("user_id").first(),
            c("day").first(),
            c("start").first(),
            pl.len().alias("events"),
            c("revenue").sum(),
        )
        .drop("session")
    )
    users = user_daily(sessions, events.filter(c("revenue") > 0))
    clicks = clicks.sort(["user_id", "ts", "click_id"], descending=[False, False, True])
    credited = asof(sessions, clicks, "start", "ts", "backward", tolerance=WEEK)
    # Disjoint inclusive ranges per campaign. Cap each click at the instant before
    # its next click. Use session ordinals to count an entire day in one subtraction.
    intervals = clicks.with_columns(
        pl.min_horizontal(
            c("ts") + WEEK, c("ts").shift(-1).over(["user_id", "campaign_id"]) - 1
        ).alias("end")
    )
    intervals = intervals.filter(c("end") >= c("ts"))
    index = sessions.select("user_id", "start", "day").with_row_index("idx")
    left = asof(intervals, index, "ts", "start", "forward").rename(
        {"idx": "lo", "day": "first_day"}
    )
    both = asof(
        left.drop("start").sort("user_id", "end"), index, "end", "start", "backward"
    )
    spans = (
        both.filter(c("lo") <= c("idx"))
        .select(
            "user_id",
            "campaign_id",
            "lo",
            c("idx").alias("hi"),
            pl.int_ranges(
                c("first_day").cast(pl.Int32), c("day").cast(pl.Int32) + 1
            ).alias("day"),
        )
        .explode("day")
        .with_columns(c("day").cast(pl.Date))
    )
    limits = index.group_by("user_id", "day").agg(
        c("idx").min().alias("first"), c("idx").max().alias("last")
    )
    assists = (
        spans.join(limits, on=["user_id", "day"])
        .group_by("campaign_id", "day")
        .agg(
            (
                pl.min_horizontal("last", "hi").cast(pl.Int64)
                - pl.max_horizontal("first", "lo")
                + 1
            )
            .sum()
            .alias("assisted_sessions")
        )
    )
    campaigns = campaign_daily(credited, assists, channels)
    return users.with_columns(
        names.gather(users["user_id"]).alias("user_id")
    ), campaigns


def user_daily(sessions, purchases):
    daily = sessions.group_by("user_id", "day").agg(
        pl.len().alias("sessions"),
        c("events").sum(),
        c("revenue").sum().alias("revenue_cents"),
    )
    top = (
        purchases.group_by("user_id", "day", "category")
        .agg(c("revenue").sum())
        .sort(["revenue", "category"], descending=[True, False])
        .unique(["user_id", "day"], keep="first")
        .select("user_id", "day", c("category").alias("top_category"))
    )
    return daily.join(top, on=["user_id", "day"], how="left")


def campaign_daily(credited, assists, channels):
    credited = (
        credited.with_columns(c("campaign_id").fill_null("organic"))
        .group_by("campaign_id", "day")
        .agg(
            pl.len().alias("sessions"),
            (c("revenue") > 0).sum().alias("purchasing_sessions"),
            c("revenue").sum().alias("revenue_cents"),
        )
    )

    return (
        credited.join(assists, on=["campaign_id", "day"], how="full", coalesce=True)
        .fill_null(0)
        .join(channels, on="campaign_id", how="left")
        .with_columns(c("channel").fill_null("organic"))
    )


def main():
    data = Path(os.environ.get("DATA_DIR", "/data"))
    out = Path(os.environ.get("OUTPUT_DIR", "/output"))
    manifest = json.loads((data / "manifest.json").read_text())
    start, end = (
        timestamp(manifest[k].encode()) for k in ("window_start", "window_end")
    )
    t = time.perf_counter()
    native = Path(os.environ.get("INGEST_BIN", "/app/ingest"))
    for name in ("user_daily", "campaign_daily"):
        (out / name).mkdir(parents=True, exist_ok=True)
    if native.is_file():
        with tempfile.TemporaryDirectory(dir=os.environ.get("SCRATCH_DIR")) as tmp:
            scratch = Path(tmp)
            subprocess.run(
                [
                    str(native.resolve()),
                    str(data),
                    tmp,
                    str(start),
                    str(end),
                    os.environ["POLARS_MAX_THREADS"],
                ],
                check=True,
            )
            print(
                f"ingestion and session processing {time.perf_counter() - t:.3f}s",
                flush=True,
            )
            channels = pl.DataFrame(
                [
                    (k.decode(), v.decode())
                    for k, v in rows([data / "campaigns.csv"], 2)
                ],
                schema={"campaign_id": pl.String, "channel": pl.String},
                orient="row",
            )
            partials = []
            for path in scratch.glob("sessions*.csv"):
                part = path.stem.removeprefix("sessions")
                sessions = read_part(
                    path,
                    {
                        "user_id": pl.UInt32,
                        "day": pl.Date,
                        "events": pl.Int64,
                        "revenue": pl.Int64,
                        "campaign_id": pl.String,
                    },
                )
                purchases = read_part(
                    scratch / f"purchases{part}.csv",
                    {
                        "user_id": pl.UInt32,
                        "day": pl.Date,
                        "category": pl.String,
                        "revenue": pl.Int64,
                    },
                )
                assists = read_part(
                    scratch / f"assists{part}.csv",
                    {
                        "campaign_id": pl.String,
                        "day": pl.Date,
                        "assisted_sessions": pl.Int64,
                    },
                )
                names = read_part(scratch / f"names{part}.csv", {"user_id": pl.String})[
                    "user_id"
                ]
                users = user_daily(sessions, purchases)
                users = users.with_columns(
                    names.gather(users["user_id"]).alias("user_id")
                )
                campaigns = campaign_daily(sessions, assists, channels)
                users.write_parquet(out / "user_daily" / f"part-{part}.parquet")
                partials.append(campaigns)
            campaigns = (
                pl.concat(partials)
                .group_by("campaign_id", "day", "channel")
                .agg(pl.all().sum())
            )
    else:
        inputs = read_data(data, start, end)
        print(f"ingestion {time.perf_counter() - t:.3f}s", flush=True)
        users, campaigns = aggregate(*inputs)
        users.write_parquet(out / "user_daily" / "part-0.parquet")
    campaigns.write_parquet(out / "campaign_daily" / "part-0.parquet")
    print(f"pipeline {time.perf_counter() - t:.3f}s", flush=True)
    peak = Path("/sys/fs/cgroup/memory.peak")
    if peak.is_file():
        print(f"container peak memory {int(peak.read_text()) / (1024**2):.0f} MiB")


def read_part(path, schema):
    # Framing preserves a leading UTF-8 BOM and terminal CR in every field.
    if not path.stat().st_size:
        return pl.DataFrame(schema=schema)
    physical = {
        name: pl.Int32 if dtype == pl.Date else dtype for name, dtype in schema.items()
    }
    return (
        pl.read_csv(path, has_header=False, schema=physical, quote_char=None)
        .cast(schema)
        .with_columns(
            c(name).str.strip_prefix("!").str.strip_suffix("!")
            for name, dtype in schema.items()
            if dtype == pl.String
        )
    )


if __name__ == "__main__":
    main()
