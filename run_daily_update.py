#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
import zipfile
from datetime import datetime, time, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

import TW_u_future_date as fut_module
import call_put_capital_flow_v3 as cp_module

BASE_DIR = Path(__file__).resolve().parent

PART1_ODS = BASE_DIR / "call_put_capital_flow.ods"
PART1_SHEET = "買權"

PART2_ODS = BASE_DIR / "u_future_OI.ods"
PART2_SHEET = "散戶微台未平倉"

NS = {
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Daily updater for call_put_capital_flow.ods and u_future_OI.ods."
        )
    )
    parser.add_argument(
        "--cutoff",
        default="16:01",
        help="daily update cutoff in HH:MM; skip before this time unless --force",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="force run regardless of cutoff",
    )
    parser.add_argument(
        "--today",
        default="",
        help="override today date (YYYY/M/D or YYYY/MM/DD) for testing",
    )
    return parser.parse_args()


def safe_console_text(text: str) -> str:
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    return text.encode(encoding, errors="replace").decode(encoding, errors="replace")


def normalize_date_input(value: str) -> str:
    raw = str(value).strip()
    if not raw:
        raise ValueError("date is empty")
    raw = raw.replace("-", "/")
    parts = raw.split("/")
    if len(parts) != 3:
        raise ValueError(f"invalid date format: {value}")
    year, month, day = (int(x) for x in parts)
    dt = datetime(year=year, month=month, day=day)
    return dt.strftime("%Y/%m/%d")


def parse_ymd(value: str) -> datetime:
    return datetime.strptime(value, "%Y/%m/%d")


def ensure_odfpy_available() -> None:
    try:
        import odf  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Missing odfpy. Install with: conda install -n ibkr odfpy or pip install odfpy"
        ) from exc


def parse_cutoff(value: str) -> time:
    parts = value.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid cutoff format: {value}")
    return time(hour=int(parts[0]), minute=int(parts[1]))


def compact_sheet_rows_by_first_date(ods_path: Path, sheet_name: str) -> bool:
    """
    Move date rows above the first blank date row so there are no blank rows
    between old and new dates.
    """
    try:
        from odf.opendocument import load
        from odf.table import Table, TableCell, TableRow
        from odf.text import P
    except ImportError:
        return False

    if not ods_path.exists():
        return False

    def first_cell_text(row: TableRow) -> str:
        cells = row.getElementsByType(TableCell)
        if not cells:
            return ""
        first = cells[0]
        texts = first.getElementsByType(P)
        text = "".join((p.firstChild.data if p.firstChild else "") for p in texts).strip()
        if text:
            return text
        sval = first.getAttribute("stringvalue")
        if sval and str(sval).strip():
            return str(sval).strip()
        val = first.getAttribute("value")
        if val not in (None, ""):
            return str(val).strip()
        return ""

    def is_date_row(row: TableRow) -> bool:
        text = first_cell_text(row)
        if not text or text == "日期":
            return False
        try:
            normalize_date_input(text)
            return True
        except Exception:
            return False

    doc = load(str(ods_path))
    target = None
    for table in doc.spreadsheet.getElementsByType(Table):
        if table.getAttribute("name") == sheet_name:
            target = table
            break
    if target is None:
        return False

    rows = list(target.getElementsByType(TableRow))
    if len(rows) <= 1:
        return False

    seen_date = False
    blank_anchor: TableRow | None = None
    moved = False
    for row in rows[1:]:
        if is_date_row(row):
            seen_date = True
            if blank_anchor is not None:
                target.insertBefore(row, blank_anchor)
                moved = True
            continue
        if seen_date and blank_anchor is None:
            blank_anchor = row

    if moved:
        doc.save(str(ods_path), addsuffix=False)
    return moved


def get_first_cell_text(row: ET.Element) -> str:
    for cell in row:
        tag = cell.tag
        if not tag.endswith("table-cell") and not tag.endswith("covered-table-cell"):
            continue
        return "".join(cell.itertext()).strip()
    return ""


def get_latest_date_from_ods(ods_path: Path, sheet_name: str) -> str | None:
    if not ods_path.exists():
        return None

    with zipfile.ZipFile(ods_path, "r") as zf:
        root = ET.fromstring(zf.read("content.xml"))

    table_node = None
    for table in root.findall(".//table:table", NS):
        if table.attrib.get(f"{{{NS['table']}}}name") == sheet_name:
            table_node = table
            break
    if table_node is None:
        return None

    latest_dt: datetime | None = None
    latest_text: str | None = None
    for row in table_node.findall("table:table-row", NS):
        first = get_first_cell_text(row)
        if not first or first == "日期":
            continue
        try:
            norm = normalize_date_input(first)
            dt = parse_ymd(norm)
        except Exception:
            continue
        if latest_dt is None or dt > latest_dt:
            latest_dt = dt
            latest_text = norm
    return latest_text


def build_update_range(latest_date: str, end_date: str) -> tuple[str, str]:
    start_dt = parse_ymd(latest_date) + timedelta(days=1)
    end_dt = parse_ymd(end_date)
    return start_dt.strftime("%Y/%m/%d"), end_dt.strftime("%Y/%m/%d")


def read_existing_dates_from_csv(csv_path: Path) -> set[str]:
    if not csv_path.exists():
        return set()
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception:
        return set()
    if "日期" not in df.columns:
        return set()

    out: set[str] = set()
    for value in df["日期"]:
        try:
            out.add(normalize_date_input(str(value)))
        except Exception:
            continue
    return out


def append_rows_to_csv_unique(csv_path: Path, rows: list[dict[str, object]], columns: list[str]) -> int:
    if not rows:
        return 0

    existing_dates = read_existing_dates_from_csv(csv_path)
    filtered_rows: list[dict[str, object]] = []
    for row in rows:
        date_text = normalize_date_input(str(row.get("日期", "")))
        if date_text in existing_dates:
            continue
        filtered_rows.append(row)
        existing_dates.add(date_text)

    if not filtered_rows:
        return 0

    df = pd.DataFrame(filtered_rows, columns=columns)
    file_exists = csv_path.exists()
    df.to_csv(
        csv_path,
        mode="a",
        header=not file_exists,
        index=False,
        encoding="utf-8-sig",
    )
    return len(filtered_rows)


def normalize_signature_value(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def build_row_signature(row: dict[str, object], columns: list[str]) -> tuple[str, ...]:
    return tuple(normalize_signature_value(row.get(col)) for col in columns if col != "日期")


def read_last_csv_row(csv_path: Path) -> dict[str, object] | None:
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception:
        return None
    if df.empty or "日期" not in df.columns:
        return None

    date_df = df[df["日期"].notna()].copy()
    if date_df.empty:
        return None
    return date_df.iloc[-1].to_dict()


def trim_duplicate_tail_rows_from_csv(csv_path: Path, columns: list[str]) -> int:
    if not csv_path.exists():
        return 0
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception:
        return 0
    if df.empty or "日期" not in df.columns:
        return 0

    removed = 0
    while len(df) >= 2:
        last = df.iloc[-1].to_dict()
        prev = df.iloc[-2].to_dict()
        if build_row_signature(last, columns) != build_row_signature(prev, columns):
            break
        df = df.iloc[:-1].copy()
        removed += 1

    if removed:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return removed


def trim_duplicate_tail_rows_from_ods(ods_path: Path, sheet_name: str) -> int:
    try:
        from odf.opendocument import load
        from odf.table import Table, TableCell, TableRow
        from odf.text import P
    except ImportError:
        return 0

    if not ods_path.exists():
        return 0

    def row_values(row: TableRow) -> list[str]:
        values: list[str] = []
        for cell in row.getElementsByType(TableCell):
            texts = cell.getElementsByType(P)
            text = "".join((p.firstChild.data if p.firstChild else "") for p in texts).strip()
            if not text:
                text = (cell.getAttribute("stringvalue") or cell.getAttribute("value") or "").strip()
            values.append(text)
        return values

    def row_signature(values: list[str]) -> tuple[str, ...]:
        return tuple(normalize_signature_value(v) for v in values[1:])

    doc = load(str(ods_path))
    target = None
    for table in doc.spreadsheet.getElementsByType(Table):
        if table.getAttribute("name") == sheet_name:
            target = table
            break
    if target is None:
        return 0

    date_rows: list[tuple[TableRow, list[str]]] = []
    for row in target.getElementsByType(TableRow):
        values = row_values(row)
        if not values:
            continue
        try:
            if normalize_date_input(values[0]):
                date_rows.append((row, values))
        except Exception:
            continue

    removed = 0
    while len(date_rows) >= 2:
        last_row, last_values = date_rows[-1]
        _, prev_values = date_rows[-2]
        if row_signature(last_values) != row_signature(prev_values):
            break
        target.removeChild(last_row)
        date_rows.pop()
        removed += 1

    if removed:
        doc.save(str(ods_path), addsuffix=False)
    return removed


def cleanup_part1_duplicate_tail() -> dict[str, int]:
    call_csv = BASE_DIR / cp_module.OUT_CALL_CSV
    put_csv = BASE_DIR / cp_module.OUT_PUT_CSV
    out_ods = BASE_DIR / cp_module.OUT_ODS

    removed_call_csv = trim_duplicate_tail_rows_from_csv(call_csv, cp_module.OUT_COLUMNS)
    removed_put_csv = trim_duplicate_tail_rows_from_csv(put_csv, cp_module.OUT_COLUMNS)
    removed_call_ods = trim_duplicate_tail_rows_from_ods(out_ods, "買權")
    removed_put_ods = trim_duplicate_tail_rows_from_ods(out_ods, "賣權")

    return {
        "call_csv": removed_call_csv,
        "put_csv": removed_put_csv,
        "call_ods": removed_call_ods,
        "put_ods": removed_put_ods,
    }


def run_part1(start_date: str, end_date: str) -> dict[str, int]:
    session = cp_module.get_session()
    rows_call: list[dict[str, object]] = []
    rows_put: list[dict[str, object]] = []
    rows_hedge: list[dict[str, object]] = []
    last_call_row = read_last_csv_row(BASE_DIR / cp_module.OUT_CALL_CSV)
    last_put_row = read_last_csv_row(BASE_DIR / cp_module.OUT_PUT_CSV)
    last_call_sig = build_row_signature(last_call_row, cp_module.OUT_COLUMNS) if last_call_row else None
    last_put_sig = build_row_signature(last_put_row, cp_module.OUT_COLUMNS) if last_put_row else None

    for query_date in cp_module.daterange(start_date, end_date):
        try:
            raw_df = cp_module.fetch_table_by_date(session, query_date)
            base_df, right_col, identity_col = cp_module.prepare_base_df(raw_df)
            v_call = cp_module.extract_for_right(base_df, right_col, identity_col, "買權")
            v_put = cp_module.extract_for_right(base_df, right_col, identity_col, "賣權")
        except Exception:
            continue

        ok_call = all(cp_module.has_all_values(v_call[k]) for k in ["5a", "5b", "5c", "5d"])
        ok_put = all(cp_module.has_all_values(v_put[k]) for k in ["5a", "5b", "5c", "5d"])
        if not (ok_call and ok_put):
            continue

        call_row = cp_module.build_output_row(query_date, v_call)
        put_row = cp_module.build_output_row(query_date, v_put)
        call_sig = build_row_signature(call_row, cp_module.OUT_COLUMNS)
        put_sig = build_row_signature(put_row, cp_module.OUT_COLUMNS)
        if last_call_sig is not None and last_put_sig is not None:
            if call_sig == last_call_sig and put_sig == last_put_sig:
                print(
                    safe_console_text(
                        f"[part1] skip stale duplicate payload on {query_date}"
                    )
                )
                continue

        print(
            safe_console_text(
                f"日期: {query_date} | 買權 5a/5b/5c/5d(自營商/投信/外資): "
                f"{v_call['5a']['自營商']}/{v_call['5a']['投信']}/{v_call['5a']['外資']} , "
                f"{v_call['5b']['自營商']}/{v_call['5b']['投信']}/{v_call['5b']['外資']} , "
                f"{v_call['5c']['自營商']}/{v_call['5c']['投信']}/{v_call['5c']['外資']} , "
                f"{v_call['5d']['自營商']}/{v_call['5d']['投信']}/{v_call['5d']['外資']}"
            )
        )
        print(
            safe_console_text(
                f"日期: {query_date} | 賣權 5a/5b/5c/5d(自營商/投信/外資): "
                f"{v_put['5a']['自營商']}/{v_put['5a']['投信']}/{v_put['5a']['外資']} , "
                f"{v_put['5b']['自營商']}/{v_put['5b']['投信']}/{v_put['5b']['外資']} , "
                f"{v_put['5c']['自營商']}/{v_put['5c']['投信']}/{v_put['5c']['外資']} , "
                f"{v_put['5d']['自營商']}/{v_put['5d']['投信']}/{v_put['5d']['外資']}"
            )
        )

        rows_call.append(call_row)
        rows_put.append(put_row)
        last_call_sig = call_sig
        last_put_sig = put_sig

        try:
            fut_df = cp_module.fetch_table_by_date(
                session=session,
                query_date=query_date,
                url=cp_module.FUT_URL,
                commodity_id=cp_module.FUT_CONTRACT_ID,
            )
            fut_lots, fut_amt = cp_module.extract_fut_hedge_values(fut_df)
            rows_hedge.append(
                {
                    "日期": query_date,
                    "未平倉口數_自營商期貨": fut_lots,
                    "未平倉金額_自營商期貨": fut_amt,
                }
            )
            print(safe_console_text(f"日期: {query_date} | 自營商對沖(期貨): {fut_lots}/{fut_amt}"))
        except Exception:
            pass

    out_ods = BASE_DIR / cp_module.OUT_ODS
    if rows_call or rows_put or rows_hedge:
        ensure_odfpy_available()

    if rows_call:
        fut_module.upsert_sheet_rows_to_ods(
            ods_path=str(out_ods),
            sheet_name="買權",
            rows=rows_call,
            columns=cp_module.OUT_COLUMNS,
        )
    if rows_put:
        fut_module.upsert_sheet_rows_to_ods(
            ods_path=str(out_ods),
            sheet_name="賣權",
            rows=rows_put,
            columns=cp_module.OUT_COLUMNS,
        )
    if rows_hedge:
        fut_module.upsert_sheet_rows_to_ods(
            ods_path=str(out_ods),
            sheet_name="自營商期貨對沖",
            rows=rows_hedge,
            columns=cp_module.HEDGE_COLUMNS,
        )

    call_csv = BASE_DIR / cp_module.OUT_CALL_CSV
    put_csv = BASE_DIR / cp_module.OUT_PUT_CSV
    call_csv_added = append_rows_to_csv_unique(call_csv, rows_call, cp_module.OUT_COLUMNS)
    put_csv_added = append_rows_to_csv_unique(put_csv, rows_put, cp_module.OUT_COLUMNS)

    return {
        "call_rows": len(rows_call),
        "put_rows": len(rows_put),
        "hedge_rows": len(rows_hedge),
        "call_csv_added": call_csv_added,
        "put_csv_added": put_csv_added,
    }


def run_part2(start_date: str, end_date: str) -> dict[str, int]:
    out_csv = (
        BASE_DIR
        / f"futContractsDate_{fut_module.COMMODITY_ID}_{start_date.replace('/', '')}_{end_date.replace('/', '')}.csv"
    )
    try:
        csv_path = fut_module.download_fut_contracts_csv(
            start_date=start_date,
            end_date=end_date,
            commodity_id=fut_module.COMMODITY_ID,
            out_file=str(out_csv),
        )
        raw_df = fut_module.read_csv_with_fallback(csv_path)
        close_map = fut_module.fetch_twse_index_close(start_date, end_date)
        summary_rows = fut_module.build_summary_rows(raw_df, close_map)
    except Exception as exc:
        print(safe_console_text(f"[part2] no valid data in range, skip. reason={exc}"))
        return {"rows": 0, "skipped": 1}

    if not summary_rows:
        print("[part2] no rows to append, skip")
        return {"rows": 0, "skipped": 1}

    ensure_odfpy_available()
    ods_path = fut_module.upsert_sheet_rows_to_ods(
        ods_path=str(PART2_ODS),
        sheet_name=PART2_SHEET,
        rows=summary_rows,
        columns=fut_module.OUT_COLUMNS,
    )
    print(
        safe_console_text(
            f"[part2] ODS updated: {Path(ods_path).resolve()} "
            f"(sheet: {PART2_SHEET}, rows: {len(summary_rows)})"
        )
    )
    return {"rows": len(summary_rows), "skipped": 0}


def run_one_part(
    part_name: str,
    ods_path: Path,
    sheet_name: str,
    today_date: str,
    runner,
) -> int:
    compacted = compact_sheet_rows_by_first_date(ods_path, sheet_name)
    if compacted:
        print(f"[{part_name}] compacted blank rows in {ods_path.name}/{sheet_name}")

    latest_date = get_latest_date_from_ods(ods_path, sheet_name)
    if latest_date is None:
        print(f"[{part_name}] no date found in {ods_path} sheet={sheet_name}, skip")
        return 0

    start_date, end_date = build_update_range(latest_date, today_date)
    print(f"[{part_name}] Date_last_time={latest_date}")
    print(f"[{part_name}] START_DATE={start_date}")
    print(f"[{part_name}] END_DATE={end_date}")

    if parse_ymd(end_date).date() <= parse_ymd(start_date).date():
        print(f"[{part_name}] END_DATE <= START_DATE, skip update")
        return 0

    try:
        summary = runner(start_date, end_date)
    except Exception as exc:
        print(safe_console_text(f"[{part_name}] failed: {exc}"))
        return 1

    print(safe_console_text(f"[{part_name}] success: {summary}"))
    return 0


def main() -> int:
    args = parse_args()
    now = datetime.now()
    cutoff = parse_cutoff(args.cutoff)

    if not args.force and now.time() < cutoff:
        print(
            f"[daily-update] skip-before-cutoff "
            f"({now.strftime('%H:%M')} < {cutoff.strftime('%H:%M')})"
        )
        return 0

    today_date = normalize_date_input(args.today) if args.today else now.strftime("%Y/%m/%d")
    print(f"[daily-update] TODAY={today_date}")

    cleanup_summary = cleanup_part1_duplicate_tail()
    if any(cleanup_summary.values()):
        print(f"[daily-update] trimmed stale part1 tail: {cleanup_summary}")

    rc1 = run_one_part(
        part_name="part1",
        ods_path=PART1_ODS,
        sheet_name=PART1_SHEET,
        today_date=today_date,
        runner=run_part1,
    )
    rc2 = run_one_part(
        part_name="part2",
        ods_path=PART2_ODS,
        sheet_name=PART2_SHEET,
        today_date=today_date,
        runner=run_part2,
    )

    if rc1 == 0 and rc2 == 0:
        print("[daily-update] all success")
        return 0

    print(f"[daily-update] done with errors: part1={rc1}, part2={rc2}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
