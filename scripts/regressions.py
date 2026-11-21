#!/usr/bin/env python3
"""Additional end-to-end cases. Accepts the same command as run_fixtures.py."""

import json
import sys
import tempfile
from pathlib import Path

from run_fixtures import main as run_fixtures


def put(folder, name, text):
    path = folder / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8"))


def case(root, name, events, clicks, links, users, campaigns):
    case_dir = root / name
    data = case_dir / "input"
    put(
        data,
        "manifest.json",
        json.dumps(
            {
                "window_start": "2028-02-29T00:00:00Z",
                "window_end": "2028-03-10T00:00:00Z",
            }
        ),
    )
    put(data, "products.csv", 'product_id,category\np,é\nq,"Z\r"\n')
    put(data, "campaigns.csv", "campaign_id,channel\na,paid\nb,social\n")
    for kind, header, parts in [
        (
            "events",
            "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
            events,
        ),
        ("clicks", "click_id,user_id,campaign_id,ts", clicks),
        ("links", "user_id_a,user_id_b", links),
    ]:
        put(data, f"{kind}/empty.csv", "")
        put(data, f"{kind}/header.csv", header)
        for i, part in enumerate(parts):
            put(data, f"{kind}/{i}.csv", header + "\n" + "\n".join(part))
    put(
        case_dir,
        "expected/user_daily.csv",
        "user_id,day,sessions,events,revenue_cents,top_category\n" + users,
    )
    put(
        case_dir,
        "expected/campaign_daily.csv",
        "campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n"
        + campaigns,
    )


def main():
    command = sys.argv[sys.argv.index("--") + 1 :]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        at = "2028-02-29T00:00:00Z"
        later = "2028-02-29T00:01:00Z"

        def row(key, user, ts, ingest, kind="view", product="p", price=""):
            return f"{key},{user},{ts},{ingest},{kind},{product},{price}"

        case(root, "empty", [], [], [], "", "")
        case(
            root,
            "calendar-and-file-boundaries",
            [
                [
                    row("bad", "x", at, later),
                    row("ok", "u", at, at, "purchase", "q", "0" * 5000 + "10"),
                    row("badquote", "u", at, at, "view", "p", 'x"'),
                    row("badyear", "x", "202A-02-29T00:00:00Z", at),
                    row("wrong-month", "x", "2028-02-30T00:00:00Z", at),
                ],
                [
                    row("bad", "u", "bad", at),
                    row("badquote", "u", at, later, "purchase", "p", "10"),
                    row("cap", "x", at, at, "purchase", "p", "10000001"),
                    row("oversize", "x", "9" * 5010, at),
                    row("offset", "u", "2028-02-28T00:01:00-23:59", at),
                    row("old", "x", "2027-02-29T00:00:00Z", at),
                ],
            ],
            [[f"z,u,b,{at}", f"é,u,a,{at}"], [f'"z",u,b,{at}']],
            [["u,A", "A,u"]],
            'A,2028-02-29,1,3,20,"Z\r"\n',
            "a,paid,2028-02-29,0,0,0,1\nb,social,2028-02-29,1,1,20,1\n",
        )
        name = "\ufeff!person!\r"
        case(
            root,
            "literal-string-ends",
            [[row("key", name, at, at, "purchase", "p", "10000000")]],
            [[f"long!,{name},b,{at}", f"long,{name},a,{at}"]],
            [],
            f'"{name}",2028-02-29,1,1,10000000,é\n',
            "a,paid,2028-02-29,1,1,10000000,1\nb,social,2028-02-29,0,0,0,1\n",
        )
        days = ["2028-02-29", "2028-03-01", "2028-03-07", "2028-03-08"]
        case(
            root,
            "overlapping-assists",
            [
                [
                    row(str(i), "z300", d + "T00:00:00Z", d + "T00:00:00Z")
                    for i, d in enumerate(days)
                ]
            ],
            [[f"c,z0,a,{at}", f"d,z1,a,{at}", "a,z1,b,2028-03-01T00:00:00Z"]],
            [
                [f"z{i},z{i + 1}" for i in range(0, 300, 2)],
                [f"z{i},z{i + 1}" for i in range(1, 300, 2)],
            ],
            "".join(f"z0,{d},1,1,0,\n" for d in days),
            f"a,paid,{days[0]},1,0,0,1\n"
            + "".join(f"a,paid,{d},0,0,0,1\n" for d in days[1:3])
            + "".join(f"b,social,{d},1,0,0,1\n" for d in days[1:]),
        )
        sys.argv = [sys.argv[0], str(root), "--", *command]
        run_fixtures()


if __name__ == "__main__":
    main()
