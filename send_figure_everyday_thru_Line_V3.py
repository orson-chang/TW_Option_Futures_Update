#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import math
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from line_push_requests_cloudinary import (
    push_line_image_url,
    push_line_message,
    upload_image_cloudinary_with_retry,
    wait_until_image_ready,
)

SEND_RETRIES = 3
URL_READY_RETRIES = 15
URL_READY_DELAY_SEC = 1.0
POST_UPLOAD_SETTLE_SEC = 6.0
DEFAULT_PUSH_GAP_SEC = 6.0
FIGSIZE = (14, 8)
DPI = 180
MAX_X_LABELS = 8

CALL_CSV = Path("call_capital_flow.csv")
PUT_CSV = Path("put_capital_flow.csv")
CALL_PUT_ODS = Path("call_put_capital_flow.ods")
U_FUTURE_ODS = Path("u_future_OI.ods")

PART1_CAPTIONS = (
    "買權減賣權",
    "買權",
    "賣權",
    "自營商期貨對沖",
)
PART2_CAPTIONS = (
    "散戶微台未平倉_加權指數收盤",
    "散戶微台未平倉_散戶微台未平倉",
)


@dataclass(frozen=True)
class ChartImage:
    caption: str
    image_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render chart images from source data and send them through LINE.",
    )
    parser.add_argument(
        "--part",
        choices=["all", "part1", "part2"],
        default="all",
        help="which task group to run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="render images without sending to LINE",
    )
    parser.add_argument(
        "--push-gap-sec",
        type=float,
        default=DEFAULT_PUSH_GAP_SEC,
        help="delay between each LINE push",
    )
    return parser.parse_args()


def discover_cjk_font_families() -> list[str]:
    try:
        from matplotlib import font_manager as fm
    except ModuleNotFoundError:
        return []

    try:
        fm.fontManager = fm._load_fontmanager(try_read_cache=False)
    except Exception:
        pass

    candidate_tokens = (
        "notosanscjk",
        "noto sans cjk",
        "notoserifcjk",
        "sourcehansans",
        "source han sans",
        "sourcehanserif",
        "source han serif",
        "wenquanyi",
        "simhei",
        "microsoft jhenghei",
        "microsoft yahei",
        "msjh",
        "msyh",
    )
    font_paths: list[str] = []
    seen_paths: set[str] = set()

    def add_font_path(path_value: str | Path) -> None:
        path_str = str(path_value)
        if not path_str:
            return
        key = path_str.lower()
        if key in seen_paths:
            return
        seen_paths.add(key)
        font_paths.append(path_str)

    for extension in ("ttf", "otf"):
        for path in fm.findSystemFonts(fontext=extension):
            lower = path.lower().replace("\\", "/")
            if any(token in lower for token in candidate_tokens):
                add_font_path(path)

    for root in (
        Path("/usr/share/fonts"),
        Path("/usr/local/share/fonts"),
        Path.home() / ".fonts",
        Path.home() / ".local/share/fonts",
        Path("C:/Windows/Fonts"),
    ):
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.suffix.lower() not in {".ttf", ".otf", ".ttc"}:
                continue
            lower = str(path).lower().replace("\\", "/")
            if any(token in lower for token in candidate_tokens):
                add_font_path(path)

    families: list[str] = []
    seen_families: set[str] = set()
    for path in font_paths:
        try:
            fm.fontManager.addfont(path)
            family = fm.FontProperties(fname=path).get_name()
        except Exception:
            continue
        if not family:
            continue
        if family in seen_families:
            continue
        seen_families.add(family)
        families.append(family)
    return families


def configure_matplotlib() -> None:
    try:
        import matplotlib
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "matplotlib is required. Install with: pip install matplotlib"
        ) from exc

    matplotlib.use("Agg")

    import matplotlib.pyplot as plt
    from matplotlib import font_manager as fm

    discovered_families = discover_cjk_font_families()
    available_families = {entry.name for entry in fm.fontManager.ttflist}
    preferred_families = discovered_families + [
        "Microsoft JhengHei",
        "Microsoft YaHei",
        "Noto Sans CJK TC",
        "Noto Sans CJK SC",
        "Noto Sans CJK JP",
        "Noto Serif CJK TC",
        "Noto Serif CJK SC",
        "Noto Serif CJK JP",
        "SimHei",
        "WenQuanYi Zen Hei",
        "Arial Unicode MS",
        "DejaVu Sans",
    ]
    ordered_families: list[str] = []
    seen: set[str] = set()
    for family in preferred_families:
        if family in seen:
            continue
        if family not in available_families:
            continue
        seen.add(family)
        ordered_families.append(family)
    if not ordered_families:
        ordered_families = ["DejaVu Sans"]
    plt.rcParams["font.family"] = ordered_families
    plt.rcParams["font.sans-serif"] = ordered_families
    plt.rcParams["axes.unicode_minus"] = False
    print(f"[fonts] matplotlib font candidates: {ordered_families}")


def read_csv_required(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    return pd.read_csv(csv_path, encoding="utf-8-sig")


def read_ods_sheet(ods_path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        from odf.opendocument import load
        from odf.table import Table, TableCell, TableRow
        from odf.text import P
    except ImportError as exc:
        raise RuntimeError(
            "odfpy is required. Install with: conda install -n ibkr odfpy or pip install odfpy"
        ) from exc

    if not ods_path.exists():
        raise FileNotFoundError(f"ODS file not found: {ods_path}")

    def cell_text(cell: TableCell) -> str:
        parts: list[str] = []
        for p in cell.getElementsByType(P):
            for child in p.childNodes:
                if hasattr(child, "data") and child.data:
                    parts.append(child.data)
        texts = "".join(parts).strip()
        if texts:
            return texts
        for attr in ("stringvalue", "value"):
            value = cell.getAttribute(attr)
            if value not in (None, ""):
                return str(value)
        return ""

    doc = load(str(ods_path))
    target = None
    for table in doc.spreadsheet.getElementsByType(Table):
        if table.getAttribute("name") == sheet_name:
            target = table
            break
    if target is None:
        raise RuntimeError(f"Sheet not found in {ods_path}: {sheet_name}")

    rows: list[list[str]] = []
    for row in target.getElementsByType(TableRow):
        row_repeat = row.getAttribute("numberrowsrepeated")
        try:
            row_repeat_count = int(row_repeat) if row_repeat not in (None, "") else 1
        except Exception:
            row_repeat_count = 1

        values: list[str] = []
        for cell in row.getElementsByType(TableCell):
            repeat = cell.getAttribute("numbercolumnsrepeated")
            try:
                repeat_count = int(repeat) if repeat not in (None, "") else 1
            except Exception:
                repeat_count = 1
            value = cell_text(cell)
            values.extend([value] * repeat_count)

        for _ in range(max(1, row_repeat_count)):
            rows.append(list(values))

    if not rows:
        raise RuntimeError(f"Sheet is empty: {ods_path}/{sheet_name}")

    header = rows[0]
    last_non_empty = -1
    for idx, name in enumerate(header):
        if str(name).strip():
            last_non_empty = idx
    if last_non_empty >= 0:
        header = header[: last_non_empty + 1]
    else:
        raise RuntimeError(f"Header row is empty: {ods_path}/{sheet_name}")

    data_rows = []
    for row in rows[1:]:
        values = row[: len(header)]
        if len(values) < len(header):
            values.extend([""] * (len(header) - len(values)))
        if not any(str(value).strip() for value in values):
            continue
        data_rows.append(values)

    return pd.DataFrame(data_rows, columns=header)


def normalize_dates(df: pd.DataFrame, column: str = "日期") -> pd.DataFrame:
    data = df.copy()
    data[column] = pd.to_datetime(data[column], format="%Y/%m/%d", errors="coerce")
    data = data.dropna(subset=[column]).sort_values(column).reset_index(drop=True)
    return data


def to_numeric_series(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    data = df.copy()
    for column in columns:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    return data


def build_buy_minus_sell_df(call_df: pd.DataFrame, put_df: pd.DataFrame) -> pd.DataFrame:
    call_base = call_df[["日期", "未平倉金額_自營商", "未平倉金額_外資"]].copy()
    put_base = put_df[["日期", "未平倉金額_自營商", "未平倉金額_外資"]].copy()

    merged = call_base.merge(
        put_base,
        on="日期",
        how="inner",
        suffixes=("_call", "_put"),
    )
    merged["未平倉金額_自營商"] = (
        merged["未平倉金額_自營商_call"] - merged["未平倉金額_自營商_put"]
    )
    merged["外資"] = merged["未平倉金額_外資_call"] - merged["未平倉金額_外資_put"]
    return merged[["日期", "未平倉金額_自營商", "外資"]].sort_values("日期").reset_index(drop=True)


def apply_date_ticks(ax, dates: pd.Series, max_labels: int = MAX_X_LABELS) -> None:
    if dates.empty:
        return

    positions = list(range(len(dates)))
    step = max(1, math.ceil(len(positions) / max_labels))
    tick_positions = positions[::step]
    if tick_positions[-1] != positions[-1]:
        tick_positions.append(positions[-1])

    tick_labels = [dates.iloc[i].strftime("%Y/%m/%d") for i in tick_positions]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=45, ha="right")
    ax.set_xlim(-0.6, len(dates) - 0.4)


def style_axis(ax, y_label: str) -> None:
    ax.set_facecolor("white")
    ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.35)
    ax.set_axisbelow(True)
    ax.set_ylabel(y_label, fontsize=11)
    ax.set_xlabel("日期", fontsize=11, labelpad=14)


def save_figure(fig, out_path: Path) -> Path:
    fig.savefig(out_path, dpi=DPI, bbox_inches="tight", facecolor="white")
    return out_path


def crop_blank_area(src_path: Path, out_path: Path, threshold: int = 245, pad: int = 6) -> Path:
    try:
        from PIL import Image
    except ModuleNotFoundError as exc:
        raise RuntimeError("Pillow is required. Install with: pip install pillow") from exc

    with Image.open(src_path) as img:
        image = img.convert("RGB")
        gray = image.convert("L")
        mask = gray.point(lambda value: 255 if value < threshold else 0)
        bbox = mask.getbbox()

        if bbox is None:
            image.save(out_path, format="JPEG", quality=90)
            return out_path

        left, top, right, _bottom = bbox
        left = max(0, left - pad)
        top = max(0, top - pad)
        right = min(image.width, right + pad)
        bottom = image.height
        cropped = image.crop((left, top, right, bottom))
        cropped.save(out_path, format="JPEG", quality=90)
    return out_path


def send_failure_alert(message: str, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run alert] {message}", flush=True)
        return
    try:
        push_line_message(message)
    except Exception as exc:
        print(f"[alert failed] {exc}", flush=True)


def send_image_with_retry(image_path: Path, text: str, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] {text}: {image_path}", flush=True)
        return

    last_error: Exception | None = None
    for attempt in range(1, SEND_RETRIES + 1):
        try:
            image_url, preview_image_url = upload_image_cloudinary_with_retry(file_path=image_path)
            wait_until_image_ready(
                image_url=preview_image_url,
                retries=URL_READY_RETRIES,
                delay_sec=URL_READY_DELAY_SEC,
            )
            wait_until_image_ready(
                image_url=image_url,
                retries=URL_READY_RETRIES,
                delay_sec=URL_READY_DELAY_SEC,
            )
            time.sleep(POST_UPLOAD_SETTLE_SEC)
            push_line_image_url(
                image_url=image_url,
                preview_image_url=preview_image_url,
                text=text,
            )
            print(
                f"sent (cloudinary): {text} -> original={image_url} preview={preview_image_url}",
                flush=True,
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt < SEND_RETRIES:
                time.sleep(1.5 * attempt)

    raise RuntimeError(f"Failed to send {text}: {last_error}") from last_error


def render_part1_images(work_dir: Path) -> list[ChartImage]:
    configure_matplotlib()
    import matplotlib.pyplot as plt

    call_df = normalize_dates(read_ods_sheet(CALL_PUT_ODS, "買權"))
    put_df = normalize_dates(read_ods_sheet(CALL_PUT_ODS, "賣權"))

    call_df = to_numeric_series(
        call_df,
        ["未平倉金額_自營商", "未平倉金額_外資", "未平倉金額_散戶"],
    )
    put_df = to_numeric_series(
        put_df,
        ["未平倉金額_自營商", "未平倉金額_外資", "未平倉金額_散戶"],
    )
    buy_minus_sell_df = build_buy_minus_sell_df(call_df, put_df)
    if buy_minus_sell_df.empty:
        raise RuntimeError("Computed 買權減賣權 data has no usable rows")

    hedge_df = normalize_dates(read_ods_sheet(CALL_PUT_ODS, "自營商期貨對沖"))
    hedge_df = to_numeric_series(hedge_df, ["未平倉口數_自營商期貨", "未平倉金額_自營商期貨"])

    images: list[ChartImage] = []
    width = 0.25
    colors = {
        "dealer": "#0f4c81",
        "foreign": "#ff5a1f",
        "retail": "#f0c419",
    }

    chart_specs = [
        (
            "買權減賣權",
            buy_minus_sell_df,
            [
                ("未平倉金額_自營商", "未平倉金額_自營商", colors["dealer"], -width / 2),
                ("外資", "未平倉金額_外資", colors["foreign"], width / 2),
            ],
            "買權減賣權 (未平倉金額)",
        ),
        (
            "買權",
            call_df,
            [
                ("未平倉金額_自營商", "未平倉金額_自營商", colors["dealer"], -width),
                ("未平倉金額_外資", "未平倉金額_外資", colors["foreign"], 0.0),
                ("未平倉金額_散戶", "未平倉金額_散戶", colors["retail"], width),
            ],
            "買權 (CALL)",
        ),
        (
            "賣權",
            put_df,
            [
                ("未平倉金額_自營商", "未平倉金額_自營商", colors["dealer"], -width),
                ("未平倉金額_外資", "未平倉金額_外資", colors["foreign"], 0.0),
                ("未平倉金額_散戶", "未平倉金額_散戶", colors["retail"], width),
            ],
            "賣權 (PUT)",
        ),
    ]

    for caption, df, series_defs, ylabel in chart_specs:
        fig, ax = plt.subplots(figsize=FIGSIZE, constrained_layout=False)
        positions = list(range(len(df)))
        for column, label, color, offset in series_defs:
            shifted = [value + offset for value in positions]
            ax.bar(shifted, df[column].fillna(0), width=width, label=label, color=color)
        style_axis(ax, ylabel)
        apply_date_ticks(ax, df["日期"])
        ax.legend(loc="upper center", ncol=3, frameon=False, fontsize=10)
        fig.subplots_adjust(bottom=0.24, top=0.88)
        raw_path = save_figure(fig, work_dir / f"{caption}_raw.png")
        plt.close(fig)
        send_path = crop_blank_area(raw_path, work_dir / f"{caption}.jpg")
        images.append(ChartImage(caption=caption, image_path=send_path))

    fig, ax = plt.subplots(figsize=FIGSIZE, constrained_layout=False)
    hx = list(range(len(hedge_df)))
    ax.bar(hx, hedge_df["未平倉口數_自營商期貨"].fillna(0), width=0.5, color=colors["dealer"], label="未平倉口數_自營商期貨")
    style_axis(ax, "自營商期貨 對沖")
    apply_date_ticks(ax, hedge_df["日期"])
    ax.legend(loc="upper center", ncol=1, frameon=False, fontsize=10)
    fig.subplots_adjust(bottom=0.24, top=0.88)
    raw_path = save_figure(fig, work_dir / "自營商期貨對沖_raw.png")
    plt.close(fig)
    send_path = crop_blank_area(raw_path, work_dir / "自營商期貨對沖.jpg")
    images.append(ChartImage(caption="自營商期貨對沖", image_path=send_path))

    return images


def render_part2_images(work_dir: Path) -> list[ChartImage]:
    configure_matplotlib()
    import matplotlib.pyplot as plt

    future_df = normalize_dates(read_ods_sheet(U_FUTURE_ODS, "散戶微台未平倉"))
    future_df = to_numeric_series(future_df, ["散戶_微台未平倉", "加權指數收盤"])

    images: list[ChartImage] = []
    x = list(range(len(future_df)))

    fig, ax = plt.subplots(figsize=FIGSIZE, constrained_layout=False)
    ax.plot(x, future_df["加權指數收盤"], linewidth=3.0, color="#0f4c81", label="加權指數收盤")
    style_axis(ax, "加權指數收盤")
    apply_date_ticks(ax, future_df["日期"])
    ax.legend(loc="upper center", ncol=1, frameon=False, fontsize=10)
    fig.subplots_adjust(bottom=0.24, top=0.88)
    raw_path = save_figure(fig, work_dir / "散戶微台未平倉_加權指數收盤_raw.png")
    plt.close(fig)
    send_path = crop_blank_area(raw_path, work_dir / "散戶微台未平倉_加權指數收盤.jpg")
    images.append(ChartImage(caption="散戶微台未平倉_加權指數收盤", image_path=send_path))

    fig, ax = plt.subplots(figsize=FIGSIZE, constrained_layout=False)
    ax.bar(x, future_df["散戶_微台未平倉"].fillna(0), width=0.55, color="#0f4c81", label="散戶_微台未平倉")
    style_axis(ax, "散戶_微台未平倉")
    apply_date_ticks(ax, future_df["日期"])
    ax.legend(loc="upper center", ncol=1, frameon=False, fontsize=10)
    fig.subplots_adjust(bottom=0.24, top=0.88)
    raw_path = save_figure(fig, work_dir / "散戶微台未平倉_散戶微台未平倉_raw.png")
    plt.close(fig)
    send_path = crop_blank_area(raw_path, work_dir / "散戶微台未平倉_散戶微台未平倉.jpg")
    images.append(ChartImage(caption="散戶微台未平倉_散戶微台未平倉", image_path=send_path))

    return images


def select_images(work_dir: Path, part: str) -> list[ChartImage]:
    images: list[ChartImage] = []
    if part in ("all", "part1"):
        images.extend(render_part1_images(work_dir))
    if part in ("all", "part2"):
        images.extend(render_part2_images(work_dir))
    return images


def main() -> int:
    args = parse_args()
    if args.dry_run:
        print("[dry-run] LINE push disabled by design", flush=True)

    tmp_dir = Path(tempfile.mkdtemp(prefix="line_fig_v2_"))
    try:
        images = select_images(tmp_dir, args.part)
        for idx, item in enumerate(images, start=1):
            send_image_with_retry(item.image_path, item.caption, dry_run=args.dry_run)
            if idx < len(images):
                time.sleep(args.push_gap_sec)
    except Exception as exc:
        alert = f"[發送失敗] send_figure_everyday_thru_Line_V3 | error={exc}"
        print(alert, flush=True)
        send_failure_alert(alert, dry_run=args.dry_run)
        raise
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    print("all done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
