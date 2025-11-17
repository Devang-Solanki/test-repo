#!/usr/bin/env python3
import os
import sys
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests
import zlib
import logging

# ---------- Config ----------
YEAR = 2025
MONTH = 11
START_DAY = 15
END_DAY = 17
BASE_URL = "https://data.gharchive.org"
TARGET = "Karthiks-19/vega_observe"

DOWNLOAD_DIR = Path("tmp_downloads")
SAVED_DIR = Path("saved")
LOGFILE = Path("matches.log")

WORKERS = 8
CHUNK_SIZE = 64 * 1024
# ----------------------------


# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("script.log", mode="w", encoding="utf-8")
    ]
)
log = logging.getLogger("gharchive")
# -----------------------------------

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
SAVED_DIR.mkdir(parents=True, exist_ok=True)

LOGFILE.write_text(f"Searching for: {TARGET}\n------------------------------------\n")


def make_urls():
    urls = []
    for day in range(START_DAY, END_DAY + 1):
        day_str = f"{day:02d}"
        for hour in range(0, 24):
            hour_str = str(hour)  # ❗ hour NOT zero-padded
            filename = f"{YEAR}-{MONTH:02d}-{day_str}-{hour_str}.json.gz"
            urls.append((f"{BASE_URL}/{filename}", filename))
    return urls


def process_url(url, filename):
    log.info(f"Downloading {filename}")

    tmp_fd, tmp_path = tempfile.mkstemp(prefix="gh_", suffix=".json.gz", dir=DOWNLOAD_DIR)
    os.close(tmp_fd)

    matched_lines = []
    line_no = 0
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)

    try:
        resp = requests.get(url, stream=True, timeout=60)
        if resp.status_code != 200:
            os.remove(tmp_path)
            log.warning(f"{filename} -> HTTP {resp.status_code}, skipping")
            return filename, 0, f"HTTP {resp.status_code}"

        partial = ""

        with open(tmp_path, "wb") as ftmp:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue

                ftmp.write(chunk)

                try:
                    decomp_bytes = decompressor.decompress(chunk)
                except zlib.error as e:
                    log.error(f"{filename} decompress error: {e}")
                    os.remove(tmp_path)
                    return filename, 0, str(e)

                if decomp_bytes:
                    text = (partial + decomp_bytes.decode("utf-8", errors="ignore"))
                    lines = text.split("\n")

                    if not text.endswith("\n"):
                        partial = lines.pop()
                    else:
                        partial = ""

                    for ln in lines:
                        line_no += 1
                        if TARGET in ln:
                            matched_lines.append((line_no, ln.rstrip("\n")))

            # flush decompressor at end
            rest = decompressor.flush()
            if rest:
                text = partial + rest.decode("utf-8", errors="ignore")
                for ln in text.split("\n"):
                    if ln.strip():
                        line_no += 1
                        if TARGET in ln:
                            matched_lines.append((line_no, ln.rstrip("\n")))

        if matched_lines:
            saved_path = SAVED_DIR / filename
            shutil.move(tmp_path, saved_path)

            log.info(f"FOUND {len(matched_lines)} matches in {filename}")

            with LOGFILE.open("a", encoding="utf-8") as lf:
                for ln_no, ln_text in matched_lines:
                    lf.write(f"{filename}:{ln_no}:{ln_text}\n")

            return filename, len(matched_lines), None

        else:
            os.remove(tmp_path)
            log.info(f"No matches in {filename}")
            return filename, 0, None

    except requests.RequestException as e:
        log.error(f"{filename} network error: {e}")
        os.remove(tmp_path)
        return filename, 0, str(e)

    except Exception as e:
        log.exception(f"Unhandled error processing {filename}: {e}")
        os.remove(tmp_path)
        return filename, 0, str(e)


def main():
    urls = make_urls()
    total = len(urls)
    log.info(f"Starting search across {total} GH Archive files...")
    log.info(f"Looking for target: {TARGET}")

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(process_url, url, fn): (url, fn) for url, fn in urls}

        for fut in as_completed(futures):
            _, fn = futures[fut]
            try:
                filename, match_count, err = fut.result()
                if err:
                    log.warning(f"{filename} → ERROR: {err}")
                else:
                    if match_count > 0:
                        log.info(f"{filename} → saved with {match_count} matches")
                    else:
                        log.debug(f"{filename} → no matches")
            except Exception as e:
                log.error(f"{fn} → crashed: {e}")

    log.info("Done. Matches are in matches.log, saved files in saved/")


if __name__ == "__main__":
    try:
        import requests
    except ImportError:
        print("Missing dependency: requests\nInstall with: pip install requests")
        sys.exit(1)

    main()
