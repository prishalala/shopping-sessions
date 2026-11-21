# Shopping Sessions

A single static C++17 binary that implements [the track rules](rules.md): it parses the dirty CSV logs, deduplicates events, joins linked user IDs, builds sessions, attributes clicks and assists, and writes the two Parquet tables directly. The Docker image is `FROM scratch` with only that binary, so container start-up costs almost nothing.

## Build and run

The scorer builds the root `Dockerfile` (linux/amd64) and runs the image with no arguments. Inputs come from `DATA_DIR` (default `/data`), outputs go to `OUTPUT_DIR` (default `/output`), and `NCPU` sets the thread count.

```sh
make build                                   # docker buildx build --platform linux/amd64 --load
make fixtures                                # all 36 supplied edge-case fixtures, scorer flags
make regressions                             # 4 extra end-to-end cases
scripts/docker_run.sh /path/to/input /path/to/empty-output
python3 scripts/compare.py /path/to/output dev-answer   # exact, order-insensitive diff
```

The checkers need Python 3.10+ with `polars==2.1.0` (`pip install -r requirements.txt`). To build the binary without Docker:

```sh
g++ -O3 -march=x86-64-v3 -std=c++17 -pthread native/kernel.cpp -o kernel
DATA_DIR=dev OUTPUT_DIR=out ./kernel
```

## How it works (`native/kernel.cpp`)

1. **Input.** Every CSV is memory-mapped and cut into 1 MB byte ranges; each worker skips to the next line start itself, so nothing is touched before parsing begins. Read-ahead hints run on a background thread. Lines are split with AVX2 masks for commas, quotes and LF, enforcing whole-field quoting, the exact field count, and `\r` as an ordinary character.
2. **Linked IDs.** User IDs are interned in a lock-free open-addressing table whose 32-byte slots hold up to 15 key bytes inline, so an exact hit is one cache line. Links are joined with union-find; each component is named by its byte-wise smallest ID (parallel CAS minimum).
3. **Events.** Timestamps are validated with 64-bit SWAR checks (all three forms, offsets, calendar limits, ASCII-only digits). Each valid event becomes an 8-byte record (person, relative time, purchase flag), routed to a person partition; purchases keep price and category in a side list.
4. **Dedup before validation.** Every well-formed event ID sets a bit in a partitioned bitmap. Slots seen twice are "contested": those rows are re-read, grouped by exact key, the earliest `ingest_ts` copy is kept, and only then validated. A rejected earliest copy drops the event. All other rows are unique by construction.
5. **Sessions and attribution.** Each partition owns whole people. Records are radix-sorted by (person, time); a sweep splits sessions on gaps over 30 minutes, credits the latest click in `[start - 7 days, start]` (ties to the byte-wise smallest `click_id`), and counts distinct assisting campaigns with a sliding window that charges runs of sessions per campaign and day.
6. **Output.** Parquet is written directly (Thrift compact metadata, PLAIN pages, `DATE` logical type, nullable `top_category`), one `user_daily` file per worker plus one `campaign_daily` file.

`pipeline/` and `native/ingest.cpp` are an earlier Python/Polars implementation kept as an independent cross-check.

## Validation

All results are exact row-for-row comparisons with zero missing, extra or duplicate keys.

- 36/36 supplied fixtures and 4/4 extra regression cases, in the linux/amd64 image with scorer flags.
- Public dev set (seed 1, 1M events): matches `dev-answer` (117,038 + 27,400 rows).
- Seed 37, 20M events: matches the earlier implementation (2,248,281 + 28,014 rows).
- Seed 73, ~60M events: matches the earlier implementation (6,612,082 + 28,014 rows); peak memory about 10.6 GB.
- The SWAR timestamp parser was fuzzed against the original scalar parser on 6M generated and mutated strings with zero mismatches.

## Acknowledgements

Built for the Ohio Builders Buildathon 2026, Databricks Shopping Sessions track, from the organizers' starter, rules, fixtures and generator. The eight-digit SWAR conversion follows the well-known technique popularized by Daniel Lemire. The checker scripts use Polars. Developed with AI assistance (OpenAI Codex, then Claude Code).
