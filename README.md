# Shopping Sessions

This Linux AMD64 image resolves identities, sessionizes events, attributes clicks and writes the two Parquet tables defined in [the track rules](rules.md).

## Run

Use Docker with Buildx and Python 3.10+ with the pinned Polars version for the local checkers. On Windows, run the shell commands in WSL with access to a Linux Docker daemon. The scorer invokes the image without arguments. The supplied runner mounts the input read-only, disables networking, runs as uid 1000 and gives the container a writable output directory and scratch space.

```sh
python3 -m pip install -r requirements.txt
make build
make fixtures
make regressions
scripts/docker_run.sh /path/to/input /path/to/empty-output
```

With the published Rust generator on PATH and the public answer extracted into `dev-answer/`, generate and compare the public dataset.

```sh
make dev run-dev check-dev
```

The native stage reads `DATA_DIR` and writes partition summaries under `SCRATCH_DIR`. Polars writes Parquet directly into `OUTPUT_DIR/user_daily/` and `OUTPUT_DIR/campaign_daily/`. Defaults inside the container are `/data`, `/scratch` and `/output`. The runner sets `NCPU=8`, a 24 GB memory limit with swap disabled, a read-only root and a 64 MB temporary filesystem.

## Implementation

1. The C++17 reader memory-maps the inputs and divides them at LF boundaries. It skips each file's header once, checks whole-field quoting and parses Gregorian timestamps and ASCII integers. A weighted union-find tracks the byte-wise minimum name of each component.
2. Two bitmaps identify event IDs that could occur more than once. A clear repeat bit proves uniqueness. Every contested bit goes through an exact comparison of the full event ID and earliest ingest time. The reader copies contested keys and user IDs into per-worker storage and releases the event mappings before probing the exact tables. An invalid earliest row can still suppress a valid later row.
3. Workers partition by canonical person, then sort compact events and clicks by person and time. Equal-time clicks sort in reverse ID order so the last eligible click wins the tie. Each session uses its starting UTC day. Active campaign counts track distinct assists. The sweep adds a run of assisted sessions when its last qualifying click expires, the day changes or the person ends.
4. Polars groups session metrics and purchase categories, resolves revenue ties, combines credited and assist-only campaign rows, and writes Parquet DATE columns. Intermediate text fields are framed to preserve literal carriage returns and leading UTF-8 byte-order marks in IDs.

The Python-only path provides a smaller-data cross-check with a separate parser and as-of join implementation. Run it with `python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py` outside the image.

## Local benchmarks

The supplied Linux AMD64 Docker runner used an eight-CPU quota on an Intel Core i7-1355U under WSL2. The guest had 7.6 GiB of RAM. Elapsed times include Docker startup and runner cleanup. Peaks are the container's cgroup memory high-water marks. These are individual local runs.

- Seed 1, 1M requested events and 250K clicks, 96.2 MB of input. The runner took 4.26 seconds and peaked at 195 MiB. The public reference matched all 117,038 user-day and 27,400 campaign-day rows.
- Seed 37, 20M requested events and 5M clicks, 1.915 GB of input. The runner took 23.66 seconds and peaked at 3,079 MiB.
- Seed 73, 60M requested events and 15M clicks, 5.606 GB of input. The runner took 52.43 seconds and peaked at 6,267 MiB. Native ingestion and session processing took 41.97 seconds. The pipeline through Parquet output took 49.47 seconds.

Both larger runs matched an earlier implementation that sessionized with Polars and computed assists with as-of joins. The full-size input contained 62,883,502 physical event data lines, 15,275,551 click data lines and 2,668,329 link data lines, including duplicates and malformed rows. The full-size comparison covered 6,612,082 user-day and 28,014 campaign-day rows. The public dev set has organizer-provided answers. The larger seeds are local benchmarks.

On the 60M input, copying contested key bytes reduced wall time from 95.21 to 52.43 seconds. Sampled container disk reads fell from 24.45 GB to 11.48 GB. The same change increased the measured 20M time from 21.30 to 23.66 seconds.

Reproduce the practice-size input with the published generator.

```sh
generate --seed 37 --events 20000000 --clicks 5000000 --output bench20
time scripts/docker_run.sh bench20 out20
```

For a full-size run use seed 73, 60,000,000 events and 15,000,000 clicks. To fit generation on this host, the local harness spooled generator shards to disk before applying the published shuffle. All 87 dev input files from that harness matched the stock generator byte for byte.

## Submission

After publishing this repository to a public GitHub remote, enter its URL and the full output of `git rev-parse HEAD` on the Shopping Sessions track page. The Dockerfile is at the repository root. The generator, data, answers and build caches are excluded from the submission.
