# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import numbers
import re
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET
import zipfile

import pandas as pd
import requests

URL = "https://www.taifex.com.tw/cht/3/callsAndPutsDate"
FUT_URL = "https://www.taifex.com.tw/cht/3/futContractsDate"

CONTRACT_ID = "TXO"  # 臺指選擇權
FUT_CONTRACT_ID = "TXF"  # 臺股期貨
TARGET_PRODUCT = "臺指選擇權"
TARGET_IDENTITIES = ["自營商", "投信", "外資"]
FUT_TARGET_PRODUCT = "臺股期貨"
FUT_TARGET_IDENTITY = "自營商"

COL_TRADE_DIFF_LOTS = "交易口數與契約金額/買賣差額/口數"
COL_TRADE_DIFF_AMT = "交易口數與契約金額/買賣差額/契約金額"
COL_OI_DIFF_LOTS = "未平倉餘額/買賣差額/口數"
COL_OI_DIFF_AMT = "未平倉餘額/買賣差額/契約金額"

OUT_CALL_CSV = "call_capital_flow.csv"
OUT_PUT_CSV = "put_capital_flow.csv"
OUT_ODS = "call_put_capital_flow.ods"

OUT_COLUMNS = [
    "日期",
    "交易口數_自營商",
    "交易契約金額_自營商",
    "交易口數_投信",
    "交易契約金額_投信",
    "交易口數_外資",
    "交易契約金額_外資",
    "未平倉口數_自營商",
    "未平倉金額_自營商",
    "未平倉口數_投信",
    "未平倉金額_投信",
    "未平倉口數_外資",
    "未平倉金額_外資",
    "交易契約金額_散戶",
    "未平倉金額_散戶",
]
HEDGE_COLUMNS = ["日期", "未平倉口數_自營商期貨", "未平倉金額_自營商期貨"]

NS = {
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
}


def ensure_odfpy_available() -> None:
    try:
        import odf  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "缺少 odfpy，無法在保留圖表的前提下更新 ODS。請安裝：`conda install -n ibkr odfpy` 或 `pip install odfpy`"
        ) from exc


def normalize_date_input(value: str) -> str:
    """將日期字串正規化成 YYYY/MM/DD。"""
    raw = str(value).strip()
    if not raw:
        raise ValueError("date is empty")

    raw = raw.replace("-", "/")
    parts = raw.split("/")
    if len(parts) != 3:
        raise ValueError(f"invalid date format: {value}")

    year, month, day = (int(p) for p in parts)
    dt = datetime(year=year, month=month, day=day)
    return dt.strftime("%Y/%m/%d")


def parse_ymd(value: str) -> datetime:
    return datetime.strptime(value, "%Y/%m/%d")


def set_date_range(start_date: str, end_date: str) -> tuple[str, str]:
    """
    START_DATE / END_DATE 設定函數，供其他程式呼叫。
    回傳正規化後的 (start_date, end_date)。
    """
    start_norm = normalize_date_input(start_date)
    end_norm = normalize_date_input(end_date)
    if parse_ymd(end_norm) < parse_ymd(start_norm):
        raise ValueError(f"END_DATE < START_DATE ({end_norm} < {start_norm})")
    return start_norm, end_norm


def daterange(start_date: str, end_date: str) -> Iterable[str]:
    start = parse_ymd(start_date)
    end = parse_ymd(end_date)
    cur = start
    while cur <= end:
        yield cur.strftime("%Y/%m/%d")
        cur += timedelta(days=1)


def get_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        }
    )
    return session


def fetch_table_by_date(
    session: requests.Session,
    query_date: str,
    url: str,
    commodity_id: str,
) -> pd.DataFrame:
    payload = {
        "queryType": "1",
        "goDay": "",
        "doQuery": "1",
        "dateaddcnt": "",
        "queryDate": query_date,
        "commodityId": commodity_id,
    }

    resp = session.post(url, data=payload, timeout=20)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"

    tables = pd.read_html(StringIO(resp.text))
    if not tables:
        raise RuntimeError("找不到任何表格")
    return tables[0]


def normalize_table(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [
            "/".join([str(v).strip() for v in col if str(v) != "nan"]) for col in df.columns
        ]

    def to_num(x):
        if pd.isna(x):
            return x
        s = str(x).replace(",", "").strip()
        if re.fullmatch(r"-?\d+(\.\d+)?", s):
            return float(s) if "." in s else int(s)
        return x

    for col in df.columns:
        df[col] = df[col].map(to_num)
    return df


def find_column(df: pd.DataFrame, include_terms: list[str]) -> str:
    for col in df.columns:
        text = str(col).replace(" ", "")
        if all(term in text for term in include_terms):
            return str(col)
    raise RuntimeError(f"找不到欄位條件 {include_terms}，目前欄位: {list(df.columns)}")


def find_exact_path_column(df: pd.DataFrame, path_text: str) -> str:
    target = path_text.replace(" ", "")
    for col in df.columns:
        if str(col).replace(" ", "") == target:
            return str(col)
    raise RuntimeError(f"找不到欄位路徑 {path_text}，目前欄位: {list(df.columns)}")


def prepare_base_df(df: pd.DataFrame) -> tuple[pd.DataFrame, str, str]:
    df = normalize_table(df)
    product_col = find_column(df, ["商品", "名稱"])
    right_col = find_column(df, ["權", "別"])
    identity_col = find_column(df, ["身份", "別"])

    df = df.copy()
    df[[product_col, right_col, identity_col]] = df[[product_col, right_col, identity_col]].ffill()
    df = df[df[product_col] == TARGET_PRODUCT].copy()
    if df.empty:
        raise RuntimeError(f"查無商品 {TARGET_PRODUCT} 資料")
    return df, right_col, identity_col


def extract_for_right(
    base_df: pd.DataFrame,
    right_col: str,
    identity_col: str,
    right_value: str,
) -> dict[str, dict[str, object]]:
    col_4a = find_exact_path_column(base_df, COL_TRADE_DIFF_LOTS)
    col_4b = find_exact_path_column(base_df, COL_TRADE_DIFF_AMT)
    col_4c = find_exact_path_column(base_df, COL_OI_DIFF_LOTS)
    col_4d = find_exact_path_column(base_df, COL_OI_DIFF_AMT)

    out = {"5a": {}, "5b": {}, "5c": {}, "5d": {}}
    for identity in TARGET_IDENTITIES:
        sub = base_df[(base_df[right_col] == right_value) & (base_df[identity_col] == identity)]
        if sub.empty:
            raise RuntimeError(f"找不到 {right_value}/{identity} 資料")

        out["5a"][identity] = sub.iloc[0][col_4a]
        out["5b"][identity] = sub.iloc[0][col_4b]
        out["5c"][identity] = sub.iloc[0][col_4c]
        out["5d"][identity] = sub.iloc[0][col_4d]
    return out


def has_all_values(values: dict[str, object]) -> bool:
    return all(v is not None and not pd.isna(v) for v in values.values())


def build_output_row(query_date: str, v: dict[str, dict[str, object]]) -> dict[str, object]:
    trade_retail = -1 * (v["5b"]["自營商"] + v["5b"]["投信"] + v["5b"]["外資"])
    oi_retail = -1 * (v["5d"]["自營商"] + v["5d"]["投信"] + v["5d"]["外資"])

    return {
        "日期": query_date,
        "交易口數_自營商": v["5a"]["自營商"],
        "交易契約金額_自營商": v["5b"]["自營商"],
        "交易口數_投信": v["5a"]["投信"],
        "交易契約金額_投信": v["5b"]["投信"],
        "交易口數_外資": v["5a"]["外資"],
        "交易契約金額_外資": v["5b"]["外資"],
        "未平倉口數_自營商": v["5c"]["自營商"],
        "未平倉金額_自營商": v["5d"]["自營商"],
        "未平倉口數_投信": v["5c"]["投信"],
        "未平倉金額_投信": v["5d"]["投信"],
        "未平倉口數_外資": v["5c"]["外資"],
        "未平倉金額_外資": v["5d"]["外資"],
        "交易契約金額_散戶": trade_retail,
        "未平倉金額_散戶": oi_retail,
    }


def extract_fut_hedge_values(df: pd.DataFrame) -> tuple[object, object]:
    df = normalize_table(df)
    product_col = find_column(df, ["商品", "名稱"])
    identity_col = find_column(df, ["身份", "別"])
    lots_col = find_exact_path_column(df, "未平倉餘額/多空淨額/口數")
    amt_col = find_exact_path_column(df, "未平倉餘額/多空淨額/契約金額")

    df = df.copy()
    df[[product_col, identity_col]] = df[[product_col, identity_col]].ffill()
    matched = df[(df[product_col] == FUT_TARGET_PRODUCT) & (df[identity_col] == FUT_TARGET_IDENTITY)]
    if matched.empty:
        raise RuntimeError("找不到臺股期貨/自營商資料")

    row = matched.iloc[0]
    return row[lots_col], row[amt_col]


def print_option_values(query_date: str, right_label: str, v: dict[str, dict[str, object]]) -> None:
    print(
        f"日期: {query_date} | {right_label} 5a/5b/5c/5d(自營商/投信/外資): "
        f"{v['5a']['自營商']}/{v['5a']['投信']}/{v['5a']['外資']} , "
        f"{v['5b']['自營商']}/{v['5b']['投信']}/{v['5b']['外資']} , "
        f"{v['5c']['自營商']}/{v['5c']['投信']}/{v['5c']['外資']} , "
        f"{v['5d']['自營商']}/{v['5d']['投信']}/{v['5d']['外資']}"
    )


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


def append_rows_to_csv(path: str, rows: list[dict[str, object]], columns: list[str]) -> int:
    if not rows:
        return 0

    csv_path = Path(path)
    existing_dates = read_existing_dates_from_csv(csv_path)

    filtered_rows: list[dict[str, object]] = []
    for row in rows:
        row_date = normalize_date_input(str(row.get("日期", "")))
        if row_date in existing_dates:
            continue
        filtered_rows.append(row)
        existing_dates.add(row_date)

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


def read_existing_dates_from_ods(ods_path: Path, sheet_name: str) -> set[str]:
    if not ods_path.exists():
        return set()
    out: set[str] = set()
    with zipfile.ZipFile(ods_path, "r") as zf:
        root = ET.fromstring(zf.read("content.xml"))

    table_node = None
    for table in root.findall(".//table:table", NS):
        if table.attrib.get(f"{{{NS['table']}}}name") == sheet_name:
            table_node = table
            break
    if table_node is None:
        return set()

    for row in table_node.findall("table:table-row", NS):
        first_cell_value = ""
        for cell in row:
            tag = cell.tag
            if not tag.endswith("table-cell") and not tag.endswith("covered-table-cell"):
                continue
            first_cell_value = "".join(cell.itertext()).strip()
            break
        if not first_cell_value or first_cell_value == "日期":
            continue
        try:
            out.add(normalize_date_input(first_cell_value))
        except Exception:
            continue
    return out


def append_rows_to_ods(
    ods_path: str,
    sheet_name: str,
    rows: list[dict[str, object]],
    columns: list[str],
) -> int:
    if not rows:
        return 0

    try:
        from odf.opendocument import OpenDocumentSpreadsheet, load
        from odf.table import Table, TableCell, TableRow
        from odf.text import P
    except ImportError as exc:
        raise RuntimeError(
            "缺少 odfpy，無法在保留圖表的前提下更新 ODS。請安裝：`conda install -n ibkr odfpy` 或 `pip install odfpy`"
        ) from exc

    def make_cell(value: object) -> TableCell:
        if isinstance(value, numbers.Number) and not pd.isna(value):
            cell = TableCell(valuetype="float", value=float(value))
            cell.addElement(P(text=str(value)))
            return cell
        text = "" if value is None else str(value)
        text = text.lstrip("'")
        cell = TableCell(valuetype="string", stringvalue=text)
        cell.addElement(P(text=text))
        return cell

    def make_row(values: list[object]) -> TableRow:
        row_node = TableRow()
        for value in values:
            row_node.addElement(make_cell(value))
        return row_node

    ods_file = Path(ods_path)
    existing_dates = read_existing_dates_from_ods(ods_file, sheet_name)

    filtered_rows: list[dict[str, object]] = []
    for row in rows:
        row_date = normalize_date_input(str(row.get("日期", "")))
        if row_date in existing_dates:
            continue
        filtered_rows.append(row)
        existing_dates.add(row_date)

    if not filtered_rows:
        return 0

    if ods_file.exists():
        doc = load(str(ods_file))
    else:
        doc = OpenDocumentSpreadsheet()

    tables = doc.spreadsheet.getElementsByType(Table)
    target_table = None
    for table in tables:
        if table.getAttribute("name") == sheet_name:
            target_table = table
            break
    if target_table is None:
        target_table = Table(name=sheet_name)
        doc.spreadsheet.addElement(target_table)
        target_table.addElement(make_row(columns))

    for row in filtered_rows:
        values = [row.get(col) for col in columns]
        target_table.addElement(make_row(values))

    doc.save(str(ods_file), addsuffix=False)
    return len(filtered_rows)


def collect_option_rows_for_date(
    session: requests.Session,
    query_date: str,
) -> tuple[dict[str, object], dict[str, object]] | None:
    try:
        raw_df = fetch_table_by_date(
            session=session,
            query_date=query_date,
            url=URL,
            commodity_id=CONTRACT_ID,
        )
        base_df, right_col, identity_col = prepare_base_df(raw_df)
        v_call = extract_for_right(base_df, right_col, identity_col, "買權")
        v_put = extract_for_right(base_df, right_col, identity_col, "賣權")
    except Exception:
        return None

    ok_call = all(has_all_values(v_call[k]) for k in ["5a", "5b", "5c", "5d"])
    ok_put = all(has_all_values(v_put[k]) for k in ["5a", "5b", "5c", "5d"])
    if not (ok_call and ok_put):
        return None

    print_option_values(query_date, "買權", v_call)
    print_option_values(query_date, "賣權", v_put)
    return build_output_row(query_date, v_call), build_output_row(query_date, v_put)


def collect_fut_hedge_row_for_date(
    session: requests.Session,
    query_date: str,
) -> dict[str, object] | None:
    try:
        fut_df = fetch_table_by_date(
            session=session,
            query_date=query_date,
            url=FUT_URL,
            commodity_id=FUT_CONTRACT_ID,
        )
        fut_lots, fut_amt = extract_fut_hedge_values(fut_df)
        if fut_lots is None or pd.isna(fut_lots) or fut_amt is None or pd.isna(fut_amt):
            return None
    except Exception:
        return None

    return {
        "日期": query_date,
        "未平倉口數_自營商期貨": fut_lots,
        "未平倉金額_自營商期貨": fut_amt,
    }


def update_call_put_capital_flow(
    start_date: str,
    end_date: str,
    out_call_csv: str = OUT_CALL_CSV,
    out_put_csv: str = OUT_PUT_CSV,
    out_ods: str = OUT_ODS,
) -> dict[str, int]:
    ensure_odfpy_available()
    start_norm, end_norm = set_date_range(start_date, end_date)
    session = get_session()

    rows_call: list[dict[str, object]] = []
    rows_put: list[dict[str, object]] = []
    rows_hedge: list[dict[str, object]] = []

    for query_date in daterange(start_norm, end_norm):
        option_rows = collect_option_rows_for_date(session, query_date)
        if option_rows is not None:
            call_row, put_row = option_rows
            rows_call.append(call_row)
            rows_put.append(put_row)

        hedge_row = collect_fut_hedge_row_for_date(session, query_date)
        if hedge_row is not None:
            rows_hedge.append(hedge_row)

    call_csv_added = append_rows_to_csv(out_call_csv, rows_call, OUT_COLUMNS)
    put_csv_added = append_rows_to_csv(out_put_csv, rows_put, OUT_COLUMNS)

    call_ods_added = append_rows_to_ods(out_ods, "買權", rows_call, OUT_COLUMNS)
    put_ods_added = append_rows_to_ods(out_ods, "賣權", rows_put, OUT_COLUMNS)
    hedge_ods_added = append_rows_to_ods(out_ods, "自營商期貨對沖", rows_hedge, HEDGE_COLUMNS)

    return {
        "call_rows_scraped": len(rows_call),
        "put_rows_scraped": len(rows_put),
        "hedge_rows_scraped": len(rows_hedge),
        "call_csv_added": call_csv_added,
        "put_csv_added": put_csv_added,
        "call_ods_added": call_ods_added,
        "put_ods_added": put_ods_added,
        "hedge_ods_added": hedge_ods_added,
    }


def parse_args() -> argparse.Namespace:
    today = datetime.now().strftime("%Y/%m/%d")
    parser = argparse.ArgumentParser(
        description="抓取期交所選擇權/期貨資料並追加更新 call_put_capital_flow.ods",
    )
    parser.add_argument("--start-date", default=today, help="開始日期，格式 YYYY/M/D 或 YYYY/MM/DD")
    parser.add_argument("--end-date", default=today, help="結束日期，格式 YYYY/M/D 或 YYYY/MM/DD")
    parser.add_argument("--out-call-csv", default=OUT_CALL_CSV, help="買權 CSV 輸出路徑")
    parser.add_argument("--out-put-csv", default=OUT_PUT_CSV, help="賣權 CSV 輸出路徑")
    parser.add_argument("--out-ods", default=OUT_ODS, help="ODS 輸出路徑")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = update_call_put_capital_flow(
        start_date=args.start_date,
        end_date=args.end_date,
        out_call_csv=args.out_call_csv,
        out_put_csv=args.out_put_csv,
        out_ods=args.out_ods,
    )

    print(
        "[summary] "
        f"scraped(call/put/hedge)={summary['call_rows_scraped']}/"
        f"{summary['put_rows_scraped']}/{summary['hedge_rows_scraped']} | "
        f"added_csv(call/put)={summary['call_csv_added']}/{summary['put_csv_added']} | "
        f"added_ods(call/put/hedge)={summary['call_ods_added']}/"
        f"{summary['put_ods_added']}/{summary['hedge_ods_added']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
