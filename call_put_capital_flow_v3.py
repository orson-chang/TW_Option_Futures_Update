# -*- coding: utf-8 -*-
import re
import numbers
from pathlib import Path
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests

URL = "https://www.taifex.com.tw/cht/3/callsAndPutsDate"
FUT_URL = "https://www.taifex.com.tw/cht/3/futContractsDate"
START_DATE = "2026/02/11"
END_DATE = "2026/02/11"
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


def daterange(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y/%m/%d")
    end = datetime.strptime(end_date, "%Y/%m/%d")
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
    url: str = URL,
    commodity_id: str = CONTRACT_ID,
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

    for c in df.columns:
        df[c] = df[c].map(to_num)

    return df


def find_column(df: pd.DataFrame, include_terms: list[str]) -> str:
    for col in df.columns:
        text = str(col).replace(" ", "")
        if all(term in text for term in include_terms):
            return col
    raise RuntimeError(f"找不到欄位條件 {include_terms}，目前欄位: {list(df.columns)}")


def find_exact_path_column(df: pd.DataFrame, path_text: str) -> str:
    target = path_text.replace(" ", "")
    for col in df.columns:
        if str(col).replace(" ", "") == target:
            return col
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


def append_to_csv(path: str, df: pd.DataFrame) -> pd.DataFrame:
    csv_path = Path(path)
    file_exists = csv_path.exists()
    df.to_csv(
        csv_path,
        mode="a",
        header=not file_exists,
        index=False,
        encoding="utf-8-sig",
    )
    return pd.read_csv(csv_path, encoding="utf-8-sig")


def append_rows_to_ods(
    ods_path: str,
    sheet_name: str,
    rows: list[dict[str, object]],
    columns: list[str],
) -> None:
    try:
        from odf.opendocument import OpenDocumentSpreadsheet, load
        from odf.table import Table, TableRow, TableCell
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
        # 清理前導單引號，避免顯示為文字符號前綴
        text = text.lstrip("'")
        cell = TableCell(valuetype="string", stringvalue=text)
        cell.addElement(P(text=text))
        return cell

    def make_row(values: list[object]) -> TableRow:
        tr = TableRow()
        for v in values:
            tr.addElement(make_cell(v))
        return tr

    ods_file = Path(ods_path)
    if ods_file.exists():
        doc = load(str(ods_file))
    else:
        doc = OpenDocumentSpreadsheet()

    tables = doc.spreadsheet.getElementsByType(Table)
    target = None
    for t in tables:
        if t.getAttribute("name") == sheet_name:
            target = t
            break
    if target is None:
        target = Table(name=sheet_name)
        doc.spreadsheet.addElement(target)
        target.addElement(make_row(columns))

    for r in rows:
        values = [r.get(c) for c in columns]
        target.addElement(make_row(values))

    doc.save(str(ods_file), addsuffix=False)


if __name__ == "__main__":
    session = get_session()
    rows_call: list[dict[str, object]] = []
    rows_put: list[dict[str, object]] = []
    rows_hedge: list[dict[str, object]] = []

    for query_date in daterange(START_DATE, END_DATE):
        try:
            raw_df = fetch_table_by_date(session, query_date)
            base_df, right_col, identity_col = prepare_base_df(raw_df)
            v_call = extract_for_right(base_df, right_col, identity_col, "買權")
            v_put = extract_for_right(base_df, right_col, identity_col, "賣權")
        except Exception:
            continue

        ok_call = all(has_all_values(v_call[k]) for k in ["5a", "5b", "5c", "5d"])
        ok_put = all(has_all_values(v_put[k]) for k in ["5a", "5b", "5c", "5d"])
        if not (ok_call and ok_put):
            continue

        print(
            f"日期: {query_date} | 買權 5a/5b/5c/5d(自營商/投信/外資): "
            f"{v_call['5a']['自營商']}/{v_call['5a']['投信']}/{v_call['5a']['外資']} , "
            f"{v_call['5b']['自營商']}/{v_call['5b']['投信']}/{v_call['5b']['外資']} , "
            f"{v_call['5c']['自營商']}/{v_call['5c']['投信']}/{v_call['5c']['外資']} , "
            f"{v_call['5d']['自營商']}/{v_call['5d']['投信']}/{v_call['5d']['外資']}"
        )
        print(
            f"日期: {query_date} | 賣權 5a/5b/5c/5d(自營商/投信/外資): "
            f"{v_put['5a']['自營商']}/{v_put['5a']['投信']}/{v_put['5a']['外資']} , "
            f"{v_put['5b']['自營商']}/{v_put['5b']['投信']}/{v_put['5b']['外資']} , "
            f"{v_put['5c']['自營商']}/{v_put['5c']['投信']}/{v_put['5c']['外資']} , "
            f"{v_put['5d']['自營商']}/{v_put['5d']['投信']}/{v_put['5d']['外資']}"
        )

        rows_call.append(build_output_row(query_date, v_call))
        rows_put.append(build_output_row(query_date, v_put))

        # 期貨：臺股期貨/自營商 對沖資料
        try:
            fut_df = fetch_table_by_date(
                session,
                query_date,
                url=FUT_URL,
                commodity_id=FUT_CONTRACT_ID,
            )
            fut_lots, fut_amt = extract_fut_hedge_values(fut_df)
            rows_hedge.append(
                {
                    "日期": query_date,
                    "未平倉口數_自營商期貨": fut_lots,
                    "未平倉金額_自營商期貨": fut_amt,
                }
            )
            print(f"日期: {query_date} | 自營商對沖(期貨): {fut_lots}/{fut_amt}")
        except Exception:
            # 期貨當日無資料時略過，不影響選擇權流程
            pass

    df_call = pd.DataFrame(rows_call, columns=OUT_COLUMNS)
    df_put = pd.DataFrame(rows_put, columns=OUT_COLUMNS)

    # 直接 append 到原始 csv，不覆蓋舊資料
    append_to_csv(OUT_CALL_CSV, df_call)
    append_to_csv(OUT_PUT_CSV, df_put)

    # ODS 僅追加新列到既有分頁，避免重建檔案造成圖表遺失
    append_rows_to_ods(OUT_ODS, "買權", rows_call, OUT_COLUMNS)
    append_rows_to_ods(OUT_ODS, "賣權", rows_put, OUT_COLUMNS)
    append_rows_to_ods(OUT_ODS, "自營商期貨對沖", rows_hedge, HEDGE_COLUMNS)
