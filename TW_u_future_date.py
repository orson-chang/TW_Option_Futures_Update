#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numbers
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

DOWNLOAD_URL = "https://www.taifex.com.tw/cht/3/futContractsDateDown"
QUERY_URL = "https://www.taifex.com.tw/cht/3/futContractsDate"
REFERER_URL = "https://www.taifex.com.tw/cht/3/futContractsDateView?menuid1=03"

START_DATE = "2026/01/11"
END_DATE = "2026/03/03"
COMMODITY_ID = "TMF"  # 微型臺指期貨

OUT_CSV = f"futContractsDate_{COMMODITY_ID}_{START_DATE.replace('/', '')}_{END_DATE.replace('/', '')}.csv"
OUT_ODS = "u_future_OI.ods"
OUT_SHEET = "散戶微台未平倉"
OUT_COLUMNS = ["日期", "自營商", "投信", "外資及陸資", "散戶", "加權指數收盤"]
TWSE_URL = "https://www.twse.com.tw/indicesReport/MI_5MINS_HIST"


def build_session() -> requests.Session:
    """建立帶有瀏覽器標頭的 requests 連線，降低被網站阻擋風險。"""
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Referer": REFERER_URL,
        }
    )
    return session


def parse_ymd(date_str: str) -> datetime:
    """將 YYYY/MM/DD 字串轉為 datetime 物件。"""
    return datetime.strptime(date_str, "%Y/%m/%d")


def month_iter(start_date: str, end_date: str) -> list[str]:
    """產生涵蓋區間的每月首日（YYYYMMDD）清單。"""
    start = parse_ymd(start_date)
    end = parse_ymd(end_date)
    cur = start.replace(day=1)
    months = []
    while cur <= end:
        months.append(cur.strftime("%Y%m%d"))
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    return months


def date_iter(start_date: str, end_date: str) -> list[str]:
    """產生起訖日期區間內的每日清單（YYYY/MM/DD）。"""
    start = parse_ymd(start_date)
    end = parse_ymd(end_date)
    days: list[str] = []
    cur = start
    while cur <= end:
        days.append(cur.strftime("%Y/%m/%d"))
        cur += timedelta(days=1)
    return days


def normalize_date_twse(date_text: str) -> str:
    """將 TWSE 日期（民國/西元）正規化為 YYYY/MM/DD。"""
    raw = str(date_text).strip()
    parts = raw.replace("/", "").split()
    token = parts[0] if parts else raw
    if "/" in raw:
        seg = raw.split("/")
        if len(seg[0]) == 3:
            y = int(seg[0]) + 1911
            return f"{y:04d}/{int(seg[1]):02d}/{int(seg[2]):02d}"
    if len(token) == 7 and token.isdigit():
        y = int(token[:3]) + 1911
        m = int(token[3:5])
        d = int(token[5:7])
        return f"{y:04d}/{m:02d}/{d:02d}"
    return raw


def normalize_date_ymd(date_text: object) -> str:
    """將混合格式日期正規化為 YYYY/MM/DD，避免合併比對失敗。"""
    raw = str(date_text).strip()
    if not raw:
        return raw
    raw = raw.replace("-", "/")
    seg = raw.split("/")
    if len(seg) == 3:
        if len(seg[0]) == 3 and seg[0].isdigit():
            y = int(seg[0]) + 1911
            return f"{y:04d}/{int(seg[1]):02d}/{int(seg[2]):02d}"
        if len(seg[0]) == 4 and seg[0].isdigit():
            return f"{int(seg[0]):04d}/{int(seg[1]):02d}/{int(seg[2]):02d}"
    token = raw.replace("/", "")
    if len(token) == 7 and token.isdigit():
        y = int(token[:3]) + 1911
        m = int(token[3:5])
        d = int(token[5:7])
        return f"{y:04d}/{m:02d}/{d:02d}"
    if len(token) == 8 and token.isdigit():
        y = int(token[:4])
        m = int(token[4:6])
        d = int(token[6:8])
        return f"{y:04d}/{m:02d}/{d:02d}"
    return raw


def fetch_twse_index_close(start_date: str, end_date: str) -> dict[str, float]:
    """抓取指定日期區間的加權指數收盤價。"""
    start = parse_ymd(start_date)
    end = parse_ymd(end_date)
    close_map: dict[str, float] = {}

    session = build_session()
    for month in month_iter(start_date, end_date):
        params = {"response": "json", "date": month}
        resp = session.get(TWSE_URL, params=params, timeout=30)
        if resp.status_code != 200:
            resp = session.get(TWSE_URL, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        if "data" not in payload:
            total = payload.get("total")
            stat = str(payload.get("stat", ""))
            if total in (0, "0") or "沒有符合條件的資料" in stat:
                continue
            raise RuntimeError(f"TWSE response missing data field. Keys: {list(payload.keys())}")

        for row in payload["data"]:
            if len(row) < 5:
                continue
            date_norm = normalize_date_twse(row[0])
            try:
                date_obj = parse_ymd(date_norm)
            except Exception:
                continue
            if date_obj < start or date_obj > end:
                continue
            close_raw = str(row[4]).replace(",", "").strip()
            try:
                close_val = float(close_raw)
            except Exception:
                continue
            close_map[date_norm] = close_val
    return close_map


def flatten_column_label(column: object) -> str:
    """將 HTML 表格的多層欄位名稱攤平成單一字串。"""
    if isinstance(column, tuple):
        parts = []
        for part in column:
            text = str(part).strip()
            if not text or text.startswith("Unnamed:"):
                continue
            parts.append(text)
        return "_".join(parts)
    return str(column).strip()


def fetch_fut_contracts_table_by_date(
    session: requests.Session,
    query_date: str,
    commodity_id: str,
) -> pd.DataFrame:
    """抓取單日期期貨契約頁面，並解析成資料表。"""
    params = {
        "queryDate": query_date,
        "commodityId": commodity_id,
    }
    resp = session.get(QUERY_URL, params=params, timeout=30)
    resp.raise_for_status()

    try:
        tables = pd.read_html(StringIO(resp.text))
    except ValueError:
        return pd.DataFrame()

    if not tables:
        return pd.DataFrame()

    df = tables[0].copy()
    if df.empty:
        return pd.DataFrame()

    first_row_text = "".join(str(v) for v in df.iloc[0].tolist())
    if "查無資料" in first_row_text:
        return pd.DataFrame()

    df.columns = [flatten_column_label(col) for col in df.columns]
    df.insert(0, "日期", query_date)
    return df


def download_fut_contracts_csv(
    start_date: str,
    end_date: str,
    commodity_id: str,
    out_file: str,
) -> Path:
    """依日期區間與商品代號逐日抓取期交所資料，彙整後輸出 CSV。"""
    frames: list[pd.DataFrame] = []
    with build_session() as session:
        for query_date in date_iter(start_date, end_date):
            daily_df = fetch_fut_contracts_table_by_date(
                session=session,
                query_date=query_date,
                commodity_id=commodity_id,
            )
            if daily_df.empty:
                continue
            frames.append(daily_df)

    if frames:
        merged_df = pd.concat(frames, ignore_index=True)
    else:
        merged_df = pd.DataFrame(
            columns=["日期", "商品名稱", "身份別", "未平倉餘額_多空淨額_口數"]
        )

    out_path = Path(out_file)
    merged_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def read_csv_with_fallback(path: Path) -> pd.DataFrame:
    """以多組常見編碼讀取 CSV，提升台灣資料檔相容性。"""
    encodings = ["utf-8-sig", "cp950", "big5", "utf-8"]
    last_error = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Failed to read CSV with known encodings: {path}") from last_error


def normalize_label(text: object) -> str:
    """移除空白並正規化標籤文字，方便欄位模糊比對。"""
    return str(text).replace(" ", "").replace("\u3000", "").strip()


def find_column(df: pd.DataFrame, include_terms: list[str]) -> str:
    """找出欄名中同時包含指定關鍵字的欄位。"""
    for col in df.columns:
        normalized = normalize_label(col)
        if all(term in normalized for term in include_terms):
            return str(col)
    raise RuntimeError(f"Cannot find column: {include_terms}; columns={list(df.columns)}")


def find_column_any(df: pd.DataFrame, term_groups: list[list[str]]) -> str:
    """用多組候選關鍵字嘗試找出對應欄位。"""
    errors = []
    for terms in term_groups:
        try:
            return find_column(df, terms)
        except Exception as exc:
            errors.append(str(exc))
    raise RuntimeError(" | ".join(errors))


def pick_identity(value: object) -> str | None:
    """將身分別文字映射為目標群組，無法識別則回傳 None。"""
    text = normalize_label(value)
    if "自營商" in text:
        return "自營商"
    if "投信" in text:
        return "投信"
    if "外資及陸資" in text or text == "外資" or "外資" in text:
        return "外資及陸資"
    return None


def build_summary_rows(df: pd.DataFrame, close_map: dict[str, float]) -> list[dict[str, object]]:
    """彙整每日資料列，輸出三大法人、散戶與加權收盤價欄位。"""
    date_col = find_column_any(df, [["日期"], ["交易", "日期"]])
    identity_col = find_column_any(df, [["身份", "別"], ["身分", "別"]])
    net_oi_col = find_column_any(
        df,
        [
            ["多空", "未平倉", "口數", "淨額"],
            ["未平倉", "口數", "淨額"],
            ["多空", "淨額"],
        ],
    )

    source = df[[date_col, identity_col, net_oi_col]].copy()
    source.columns = ["日期", "身份別", "多空未平倉口數淨額"]
    source["日期"] = source["日期"].map(normalize_date_ymd)
    source["身份別_目標"] = source["身份別"].map(pick_identity)

    source = source[source["身份別_目標"].notna()].copy()
    if source.empty:
        raise RuntimeError("No matching rows for 自營商/投信/外資及陸資.")

    source["多空未平倉口數淨額"] = pd.to_numeric(source["多空未平倉口數淨額"], errors="coerce")
    source = source.dropna(subset=["多空未平倉口數淨額"])
    source = source.drop_duplicates(subset=["日期", "身份別_目標"], keep="first")

    pivot = source.pivot(index="日期", columns="身份別_目標", values="多空未平倉口數淨額")
    pivot = pivot.reset_index()

    for key in ["自營商", "投信", "外資及陸資"]:
        if key not in pivot.columns:
            pivot[key] = None

    pivot["散戶"] = -1 * (
        pivot["自營商"].fillna(0)
        + pivot["投信"].fillna(0)
        + pivot["外資及陸資"].fillna(0)
    )
    pivot["加權指數收盤"] = pivot["日期"].map(close_map)

    pivot = pivot[OUT_COLUMNS].sort_values("日期").reset_index(drop=True)
    rows: list[dict[str, object]] = pivot.to_dict(orient="records")
    return rows


def read_existing_dates_from_ods(ods_path: str, sheet_name: str) -> set[str]:
    """讀取 ODS 分頁已存在日期，避免重複追加。"""
    try:
        from odf.opendocument import load
        from odf.table import Table, TableRow, TableCell
        from odf.text import P
    except ImportError:
        return set()

    ods_file = Path(ods_path)
    if not ods_file.exists():
        return set()

    doc = load(str(ods_file))
    tables = doc.spreadsheet.getElementsByType(Table)
    target = None
    for table in tables:
        if table.getAttribute("name") == sheet_name:
            target = table
            break
    if target is None:
        return set()

    rows = target.getElementsByType(TableRow)
    if not rows:
        return set()

    existing: set[str] = set()
    for row in rows[1:]:
        cells = row.getElementsByType(TableCell)
        if not cells:
            continue
        texts = cells[0].getElementsByType(P)
        date_text = ""
        if texts:
            date_text = "".join(t.firstChild.data for t in texts if t.firstChild)
        if not date_text:
            date_text = cells[0].getAttribute("stringvalue") or ""
        if not date_text:
            date_text = cells[0].getAttribute("value") or ""
        date_norm = normalize_date_ymd(date_text)
        if date_norm:
            existing.add(date_norm)
    return existing


def upsert_sheet_rows_to_ods(
    ods_path: str,
    sheet_name: str,
    rows: list[dict[str, object]],
    columns: list[str],
) -> Path:
    """在保留既有內容與圖表前提下，將新資料追加到目標分頁。"""
    try:
        from odf.opendocument import OpenDocumentSpreadsheet, load
        from odf.table import Table, TableCell, TableRow
        from odf.text import P
    except ImportError as exc:
        raise RuntimeError(
            "Missing odfpy. Install with: conda install -n ibkr odfpy or pip install odfpy"
        ) from exc

    def make_cell(value: object) -> TableCell:
        if isinstance(value, numbers.Number) and not pd.isna(value):
            cell = TableCell(valuetype="float", value=float(value))
            cell.addElement(P(text=str(int(value) if float(value).is_integer() else value)))
            return cell
        text = "" if value is None or pd.isna(value) else str(value)
        cell = TableCell(valuetype="string", stringvalue=text)
        cell.addElement(P(text=text))
        return cell

    def make_row(values: list[object]) -> TableRow:
        row = TableRow()
        for value in values:
            row.addElement(make_cell(value))
        return row

    def get_first_cell_text_from_row(row: TableRow) -> str:
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

    ods_file = Path(ods_path)
    if ods_file.exists():
        doc = load(str(ods_file))
    else:
        doc = OpenDocumentSpreadsheet()

    tables = doc.spreadsheet.getElementsByType(Table)
    target = None
    for table in tables:
        if table.getAttribute("name") == sheet_name:
            target = table
            break

    if target is None:
        target = Table(name=sheet_name)
        doc.spreadsheet.addElement(target)
        target.addElement(make_row(columns))

    existing_dates = read_existing_dates_from_ods(ods_path, sheet_name)
    table_rows = target.getElementsByType(TableRow)
    insert_anchor = None
    if len(table_rows) > 1:
        seen_any_date_row = False
        for row in table_rows[1:]:
            first_text = get_first_cell_text_from_row(row)
            first_norm = normalize_date_ymd(first_text) if first_text else ""
            if first_norm:
                seen_any_date_row = True
                continue
            if seen_any_date_row:
                insert_anchor = row
                break

    for row_data in rows:
        if row_data.get("日期") in existing_dates:
            continue
        new_row = make_row([row_data.get(col) for col in columns])
        if insert_anchor is not None:
            target.insertBefore(new_row, insert_anchor)
        else:
            target.addElement(new_row)

    doc.save(str(ods_file), addsuffix=False)
    return ods_file


if __name__ == "__main__":
    csv_path = download_fut_contracts_csv(
        start_date=START_DATE,
        end_date=END_DATE,
        commodity_id=COMMODITY_ID,
        out_file=OUT_CSV,
    )
    print(f"CSV downloaded: {csv_path.resolve()}")

    raw_df = read_csv_with_fallback(csv_path)
    close_map = fetch_twse_index_close(START_DATE, END_DATE)
    summary_rows = build_summary_rows(raw_df, close_map)
    ods_path = upsert_sheet_rows_to_ods(
        ods_path=OUT_ODS,
        sheet_name=OUT_SHEET,
        rows=summary_rows,
        columns=OUT_COLUMNS,
    )
    print(f"ODS updated: {ods_path.resolve()} (sheet: {OUT_SHEET}, rows: {len(summary_rows)})")
