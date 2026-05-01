#!/usr/bin/env python3
"""Download local source files for Retrosheet pitcher historical backfill.

This is an acquisition-only layer. It creates a reproducible local directory
layout but does not parse files and does not write to the database.

Currently implemented:
- Chadwick Register people shards, combined locally as `people.csv`

Scaffolded, but intentionally not implemented yet:
- Retrosheet/Chadwick pitcher box-score CSV downloads
- Retrosheet event-file downloads

Those Retrosheet paths need explicit stable source URLs or a documented mirror
before the script should claim success.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import tempfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


CHADWICK_PEOPLE_SHARD_URLS = [
    f"https://raw.githubusercontent.com/chadwickbureau/register/master/data/people-{suffix}.csv"
    for suffix in "0123456789abcdef"
]


def _download(url: str, dest: Path, *, force: bool) -> bool:
    if dest.exists() and not force:
        print(f"[retrosheet-sources] exists skip dest={dest}")
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)
    req = Request(url, headers={"User-Agent": "proppadia-retrosheet-source-downloader/1.0"})
    try:
        with urlopen(req, timeout=30) as resp:
            status = getattr(resp, "status", 200)
            if int(status) >= 400:
                raise RuntimeError(f"HTTP {status} downloading {url}")
            with tempfile.NamedTemporaryFile(delete=False, dir=str(dest.parent)) as tmp:
                shutil.copyfileobj(resp, tmp)
                tmp_path = Path(tmp.name)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise SystemExit(f"Failed to download {url}: {type(exc).__name__}: {exc}") from exc

    tmp_path.replace(dest)
    print(f"[retrosheet-sources] downloaded url={url} dest={dest}")
    return True


def _combine_csv_shards(shards: list[Path], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(dest.parent), newline="", encoding="utf-8") as tmp:
        writer = None
        tmp_path = Path(tmp.name)
        for shard in shards:
            with shard.open("r", newline="", encoding="utf-8") as fh:
                reader = csv.reader(fh)
                header = next(reader, None)
                if not header:
                    continue
                if writer is None:
                    writer = csv.writer(tmp)
                    writer.writerow(header)
                for row in reader:
                    writer.writerow(row)
    tmp_path.replace(dest)
    print(f"[retrosheet-sources] combined shards={len(shards)} dest={dest}")


def _download_chadwick_register(dest_dir: Path, *, force: bool) -> int:
    dest_dir.mkdir(parents=True, exist_ok=True)
    shard_paths: list[Path] = []
    downloaded = 0
    for url in CHADWICK_PEOPLE_SHARD_URLS:
        name = url.rsplit("/", 1)[-1]
        dest = dest_dir / name
        downloaded += int(_download(url, dest, force=force))
        shard_paths.append(dest)

    combined = dest_dir / "people.csv"
    if force or not combined.exists() or downloaded:
        _combine_csv_shards(shard_paths, combined)
    else:
        print(f"[retrosheet-sources] exists skip combined={combined}")
    return downloaded


def _ensure_dirs(out_dir: Path) -> dict[str, Path]:
    dirs = {
        "root": out_dir,
        "chadwick_register": out_dir / "chadwick_register",
        "csv_downloads": out_dir / "csv_downloads",
        "event_files": out_dir / "event_files",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def _fail_retrosheet_csvs(season: str) -> None:
    suffix = f" for season {season}" if season else ""
    raise SystemExit(
        "Retrosheet CSV download is not configured yet"
        f"{suffix}. TODO: choose/document stable URLs or a Chadwick-derived box-score export source. "
        "Until then, place pitcher box-score CSVs under "
        "backend/mlb/data/raw/retrosheet/csv_downloads/ and run ingest with "
        "--retrosheet-gamelogs-dir backend/mlb/data/raw/retrosheet/csv_downloads."
    )


def _fail_retrosheet_events(season: str) -> None:
    suffix = f" for season {season}" if season else ""
    raise SystemExit(
        "Retrosheet event-file download is not configured yet"
        f"{suffix}. TODO: choose/document stable Retrosheet event-file URLs or local archive naming. "
        "Until then, place *.EVN/*.EVA files under "
        "backend/mlb/data/raw/retrosheet/event_files/ and run ingest with "
        "--retrosheet-events-dir backend/mlb/data/raw/retrosheet/event_files "
        "--allow-event-parser-preview."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="backend/mlb/data/raw/retrosheet")
    parser.add_argument("--download-chadwick-register", action="store_true")
    parser.add_argument("--download-retrosheet-csvs", action="store_true")
    parser.add_argument("--download-retrosheet-events", action="store_true")
    parser.add_argument("--season", default="")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    dirs = _ensure_dirs(out_dir)

    if not any(
        [
            args.download_chadwick_register,
            args.download_retrosheet_csvs,
            args.download_retrosheet_events,
        ]
    ):
        args.download_chadwick_register = True

    downloaded = 0
    if args.download_chadwick_register:
        downloaded += _download_chadwick_register(dirs["chadwick_register"], force=args.force)

    if args.download_retrosheet_csvs:
        _fail_retrosheet_csvs(args.season)

    if args.download_retrosheet_events:
        _fail_retrosheet_events(args.season)

    print(
        "[retrosheet-sources] "
        f"out_dir={out_dir} chadwick_register={dirs['chadwick_register'] / 'people.csv'} "
        f"csv_downloads_dir={dirs['csv_downloads']} event_files_dir={dirs['event_files']} "
        f"downloaded={downloaded}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
