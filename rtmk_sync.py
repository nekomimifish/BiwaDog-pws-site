#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RTMK 10分CSVを FTP から取得し、data/YYYY-MM.csv に月別で追記する同期スクリプト
(GitHub Actions / ローカル両対応)

✅ GitHub Actions で使う前提（Secrets/Env から設定を読む）
- FTP_HOST, FTP_USER, FTP_PASS, FTP_DIR(任意), FTP_PORT(任意)
- START_FROM_DATE (任意, 例: 2025-11-01)
- REBUILD_MONTHS (任意, "1" なら対象月のCSVを作り直す)
- STATE_FILE (任意, 既定: state/downloaded_files.json)

出力:
- data/YYYY-MM.csv
- state/downloaded_files.json  （処理済みファイル名のリスト）
- log/rtmk_sync.log

注意:
- GitHub Actions は毎回クリーン環境なので、増分運用には state をコミットして保持します。
  （workflow 側で data/*.csv と state/*.json を add/commit してください）
"""

from __future__ import annotations

import csv
import datetime as dt
import ftplib
import io
import json
import os
import time
from pathlib import Path
from typing import List, Optional, Set, Tuple


# ----------------------------
# 設定（Env → デフォルト）
# ----------------------------
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

ROOT = Path(_env("PWS_ROOT", "")).resolve() if _env("PWS_ROOT", "") else Path(__file__).resolve().parent

CONFIG = {
    # FTP
    "ftp_host": _env("FTP_HOST"),
    "ftp_user": _env("FTP_USER"),
    "ftp_pass": _env("FTP_PASS"),
    "ftp_dir":  _env("FTP_DIR", "/"),     # 例: "/FTP"
    "ftp_port": int(_env("FTP_PORT", "21")),

    # 対象期間（この日付以降のみ）
    "start_from_date": _env("START_FROM_DATE", "2025-11-01"),

    # 初回再構築（Trueだと対象月CSVを一旦削除して作り直す）
    "rebuild_months": _env("REBUILD_MONTHS", "0") in ("1", "true", "TRUE", "yes", "YES"),

    # 状態ファイル（増分用）: repo内に置くのがポイント
    "state_file": _env("STATE_FILE", "state/downloaded_files.json"),

    # タイムアウト・リトライ
    "connect_timeout": int(_env("FTP_CONNECT_TIMEOUT", "10")),
    "transfer_timeout": int(_env("FTP_TRANSFER_TIMEOUT", "20")),
    "connect_retries": int(_env("FTP_CONNECT_RETRIES", "3")),
    "retry_sleep_sec": float(_env("FTP_RETRY_SLEEP", "2.0")),
}

START_DATE = dt.datetime.strptime(CONFIG["start_from_date"], "%Y-%m-%d").date()

RAW_DIR = ROOT / "raw"
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "log"
STATE_PATH = ROOT / CONFIG["state_file"]


# ----------------------------
# ログ / 便利関数
# ----------------------------
def now_str() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str) -> None:
    line = f"[{now_str()}] {msg}"
    print(line, flush=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with (LOG_DIR / "rtmk_sync.log").open("a", encoding="utf-8") as f:
        f.write(line + "\n")

def ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

def load_state() -> List[str]:
    if not STATE_PATH.exists():
        return []
    try:
        with STATE_PATH.open("r", encoding="utf-8") as f:
            v = json.load(f)
        return v if isinstance(v, list) else []
    except Exception:
        return []

def save_state(files: List[str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(sorted(set(files)), f, ensure_ascii=False, indent=2)

def extract_datetime_from_fname(fname: str) -> Optional[dt.datetime]:
    """
    対応例:
      235_20251219T170000.CSV -> 2025-12-19 17:00:00
      D000_20250616115008.CSV -> 2025-06-16 11:50:08
    """
    base = os.path.splitext(fname)[0]
    parts = base.split("_")
    if len(parts) < 2:
        return None
    tail = parts[1]

    # 20251219T170000
    try:
        if len(tail) >= 15 and tail[8] == "T":
            return dt.datetime.strptime(tail[:15], "%Y%m%dT%H%M%S")
    except Exception:
        pass

    # 20250616115008
    try:
        if tail[:14].isdigit():
            return dt.datetime.strptime(tail[:14], "%Y%m%d%H%M%S")
    except Exception:
        pass

    return None

def file_in_scope(fname: str) -> bool:
    dttm = extract_datetime_from_fname(fname)
    return bool(dttm and dttm.date() >= START_DATE)

def year_month_for(fname: str) -> str:
    dttm = extract_datetime_from_fname(fname)
    if not dttm:
        now = dt.datetime.now()
        return f"{now.year}-{now.month:02d}"
    return f"{dttm.year}-{dttm.month:02d}"

def month_path(ym: str) -> Path:
    return DATA_DIR / f"{ym}.csv"


# ----------------------------
# FTP
# ----------------------------
def ftp_connect() -> ftplib.FTP:
    if not CONFIG["ftp_host"] or not CONFIG["ftp_user"] or not CONFIG["ftp_pass"]:
        raise RuntimeError("FTP_HOST/FTP_USER/FTP_PASS が未設定です（GitHub Secrets/Env を確認）")

    last_err: Optional[Exception] = None
    for attempt in range(1, CONFIG["connect_retries"] + 1):
        try:
            log(f"Connecting to FTP... (attempt {attempt}/{CONFIG['connect_retries']})")
            ftp = ftplib.FTP()
            ftp.connect(CONFIG["ftp_host"], CONFIG["ftp_port"], timeout=CONFIG["connect_timeout"])
            ftp.login(CONFIG["ftp_user"], CONFIG["ftp_pass"])
            ftp.set_pasv(True)  # 多くの環境で安定
            ftp.cwd(CONFIG["ftp_dir"])
            ftp.sock.settimeout(CONFIG["transfer_timeout"])
            return ftp
        except Exception as e:
            last_err = e
            log(f"FTP connect failed: {e}")
            time.sleep(CONFIG["retry_sleep_sec"])
    raise RuntimeError(f"FTP接続に失敗しました: {last_err}")

def list_remote_csv(ftp: ftplib.FTP) -> List[str]:
    # cwd 済み想定
    names = ftp.nlst()
    return [n for n in names if n.lower().endswith(".csv")]


# ----------------------------
# Download & merge
# ----------------------------
def download_file(ftp: ftplib.FTP, fname: str) -> Path:
    local_path = RAW_DIR / fname
    with local_path.open("wb") as lf:
        ftp.retrbinary(f"RETR {fname}", lf.write)
    return local_path

def append_raw_to_monthly(raw_path: Path, ym: str) -> int:
    """
    1つの raw CSV から data行(ヘッダ除く)を月別CSVへ追記する。
    戻り値: 追記したデータ行数
    """
    monthly_path = month_path(ym)

    # 文字コードは環境差があるので errors="ignore"
    text = raw_path.read_text(encoding="utf-8", errors="ignore")
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return 0

    header = rows[0]
    data_rows = [r for r in rows[1:] if r]

    exists = monthly_path.exists()
    with monthly_path.open("a", newline="", encoding="utf-8") as mf:
        w = csv.writer(mf)
        if not exists:
            w.writerow(header)
        for r in data_rows:
            w.writerow(r)
    return len(data_rows)

def clear_target_month_files_if_rebuild(scope_files: List[str]) -> None:
    if not CONFIG["rebuild_months"]:
        return
    months = sorted(set(year_month_for(f) for f in scope_files))
    for ym in months:
        p = month_path(ym)
        if p.exists():
            p.unlink()
            log(f"[rebuild] removed {p.name}")

def main() -> None:
    ensure_dirs()
    log("=== RTMK sync start ===")

    state = load_state()
    state_set: Set[str] = set(state)

    ftp: Optional[ftplib.FTP] = None
    try:
        ftp = ftp_connect()
        remote = sorted(list_remote_csv(ftp))
        scope = [f for f in remote if file_in_scope(f)]
        if not scope:
            log("対象期間のファイルが見つかりません（START_FROM_DATE/FTP_DIR を確認）")
            return

        clear_target_month_files_if_rebuild(scope)

        new_files = [f for f in scope if f not in state_set]
        if not new_files:
            log("差分なし（新しいファイルなし）")
            return

        ok = 0
        skipped = 0
        for f in new_files:
            ym = year_month_for(f)
            try:
                log(f"Downloading {f}")
                raw_path = download_file(ftp, f)
                added = append_raw_to_monthly(raw_path, ym)
                state_set.add(f)
                ok += 1
                log(f"merged -> {month_path(ym).name} (+{added}行)")
            except Exception as e:
                skipped += 1
                log(f"[WARN] failed {f}: {e}")

        save_state(list(state_set))
        log(f"=== RTMK sync end (success) === ok={ok}, skipped={skipped}")

    except KeyboardInterrupt:
        log("中断: KeyboardInterrupt（Ctrl+C）")
    except Exception as e:
        log(f"エラー終了: {e}")
        raise
    finally:
        if ftp:
            try:
                ftp.quit()
            except Exception:
                pass


if __name__ == "__main__":
    main()
