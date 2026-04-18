import io
import re
import time
import json
import base64
from datetime import datetime, timedelta

import requests
import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.express as px
import plotly.graph_objects as go
from bs4 import BeautifulSoup
import streamlit_authenticator as stauth

# ─── GitHub クラウド保存 ──────────────────────────────────────────
GITHUB_REPO = "teruk1771-alt/portfolio-app"

def _github_headers() -> dict:
    token = st.secrets.get("GITHUB_TOKEN", "")
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

def _user_data_path() -> str:
    """ログイン中ユーザーのGitHubデータパスを返す"""
    username = st.session_state.get("username", "default")
    return f"data/{username}/portfolio.json"

def load_portfolio_from_github() -> list:
    """GitHubからポートフォリオデータを読み込む"""
    try:
        token = st.secrets.get("GITHUB_TOKEN", "")
        if not token:
            return []
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{_user_data_path()}"
        resp = requests.get(url, headers=_github_headers(), timeout=10)
        if resp.status_code == 200:
            content = base64.b64decode(resp.json()["content"]).decode("utf-8")
            return json.loads(content)
    except Exception:
        pass
    return []

def save_portfolio_to_github(holdings: list) -> bool:
    """GitHubにポートフォリオデータを保存する"""
    try:
        token = st.secrets.get("GITHUB_TOKEN", "")
        if not token:
            return False
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{_user_data_path()}"
        content = base64.b64encode(
            json.dumps(holdings, ensure_ascii=False, indent=2).encode()
        ).decode()
        # 既存ファイルのSHAを取得（更新時に必要）
        get_resp = requests.get(url, headers=_github_headers(), timeout=10)
        payload: dict = {"message": "Update portfolio data", "content": content}
        if get_resp.status_code == 200:
            payload["sha"] = get_resp.json()["sha"]
        put_resp = requests.put(url, headers=_github_headers(), json=payload, timeout=15)
        return put_resp.status_code in (200, 201)
    except Exception:
        return False

# yfinance industry → 東証業種分類マッピング
INDUSTRY_TO_JP_SECTOR = {
    # 食料品
    "Farm Products": "食料品", "Packaged Foods": "食料品",
    "Beverages - Non-Alcoholic": "食料品", "Beverages - Brewers": "食料品",
    "Confectioners": "食料品",
    # 繊維製品
    "Textile Manufacturing": "繊維製品", "Apparel Manufacturing": "繊維製品",
    # パルプ・紙
    "Paper & Paper Products": "パルプ・紙",
    # 化学
    "Chemicals": "化学", "Specialty Chemicals": "化学",
    "Agricultural Inputs": "化学",
    # 医薬品
    "Drug Manufacturers - General": "医薬品",
    "Drug Manufacturers - Specialty & Generic": "医薬品",
    "Biotechnology": "医薬品",
    # 石油・石炭製品
    "Oil & Gas Integrated": "石油・石炭製品", "Oil & Gas E&P": "石油・石炭製品",
    "Oil & Gas Refining & Marketing": "石油・石炭製品",
    # ゴム製品
    "Rubber & Plastics": "ゴム製品", "Auto Parts": "ゴム製品",
    # ガラス・土石製品
    "Building Materials": "ガラス・土石製品",
    # 鉄鋼
    "Steel": "鉄鋼",
    # 非鉄金属
    "Aluminum": "非鉄金属", "Copper": "非鉄金属",
    "Other Industrial Metals & Mining": "非鉄金属",
    # 金属製品
    "Metal Fabrication": "金属製品",
    # 機械
    "Farm & Heavy Construction Machinery": "機械",
    "Specialty Industrial Machinery": "機械",
    "Industrial Machinery & Equipment": "機械",
    # 電気機器
    "Electronic Components": "電気機器",
    "Consumer Electronics": "電気機器",
    "Scientific & Technical Instruments": "電気機器",
    "Electrical Equipment & Parts": "電気機器",
    "Semiconductors": "電気機器",
    "Semiconductor Equipment & Materials": "電気機器",
    # 輸送用機器
    "Auto Manufacturers": "輸送用機器",
    # 精密機器
    "Medical Instruments & Supplies": "精密機器",
    "Medical Devices": "精密機器",
    "Diagnostics & Research": "精密機器",
    # その他製品
    "Packaging & Containers": "その他製品",
    "Building Products & Equipment": "その他製品",
    "Business Equipment & Supplies": "その他製品",
    "Leisure": "その他製品",
    # 電気・ガス業
    "Utilities - Regulated Electric": "電気・ガス業",
    "Utilities - Renewable": "電気・ガス業",
    "Utilities - Regulated Gas": "電気・ガス業",
    "Utilities - Diversified": "電気・ガス業",
    "Utilities - Independent Power Producers": "電気・ガス業",
    # 陸運業
    "Railroads": "陸運業", "Trucking": "陸運業",
    # 海運業
    "Marine Shipping": "海運業",
    # 空運業
    "Airlines": "空運業",
    # 倉庫・運輸関連業
    "Integrated Freight & Logistics": "倉庫・運輸関連業",
    "Infrastructure Operations": "倉庫・運輸関連業",
    # 情報・通信業
    "Telecom Services": "情報・通信業",
    "Information Technology Services": "情報・通信業",
    "Software - Application": "情報・通信業",
    "Software - Infrastructure": "情報・通信業",
    "Internet Content & Information": "情報・通信業",
    "Communication Equipment": "情報・通信業",
    "Electronic Gaming & Multimedia": "情報・通信業",
    # 卸売業
    "Conglomerates": "卸売業", "Industrial Distribution": "卸売業",
    # 小売業
    "Specialty Retail": "小売業",
    "Home Improvement Retail": "小売業",
    "Department Stores": "小売業",
    "Discount Stores": "小売業",
    "Grocery Stores": "小売業",
    # 銀行業
    "Banks - Regional": "銀行業", "Banks - Diversified": "銀行業",
    # 証券、商品先物取引業
    "Capital Markets": "証券、商品先物取引業",
    # 保険業
    "Insurance - Property & Casualty": "保険業",
    "Insurance - Life": "保険業",
    "Insurance - Diversified": "保険業",
    "Insurance Brokers": "保険業",
    # その他金融業
    "Credit Services": "その他金融業",
    "Financial Data & Stock Exchanges": "その他金融業",
    "Financial Conglomerates": "その他金融業",
    # 不動産業
    "Real Estate Services": "不動産業",
    "Real Estate - Diversified": "不動産業",
    "Real Estate - Development": "不動産業",
    "REIT - Diversified": "不動産業",
    "REIT - Residential": "不動産業",
    "REIT - Office": "不動産業",
    "REIT - Retail": "不動産業",
    # 建設業
    "Residential Construction": "建設業",
    "Engineering & Construction": "建設業",
    # サービス業
    "Consulting Services": "サービス業",
    "Staffing & Employment Services": "サービス業",
    "Education & Training Services": "サービス業",
    "Security & Protection Services": "サービス業",
    "Waste Management": "サービス業",
    "Rental & Leasing Services": "サービス業",
    # 家具・インテリア（小売寄り）
    "Furnishings, Fixtures & Appliances": "小売業",
}

# yfinance sector をフォールバックに使う（industryが未マッチ時）
SECTOR_EN_TO_JP_FALLBACK = {
    "Consumer Defensive": "食料品",
    "Consumer Staples": "食料品",
    "Healthcare": "医薬品",
    "Utilities": "電気・ガス業",
    "Communication Services": "情報・通信業",
    "Financial Services": "その他金融業",
    "Financials": "その他金融業",
    "Energy": "石油・石炭製品",
    "Basic Materials": "化学",
    "Materials": "化学",
    "Industrials": "機械",
    "Consumer Cyclical": "小売業",
    "Consumer Discretionary": "小売業",
    "Technology": "情報・通信業",
    "Information Technology": "情報・通信業",
    "Real Estate": "不動産業",
}

# 東証33業種分類（楽天証券表示に準拠）→ 景気区分
DEFENSIVE_SECTORS = {
    "食料品", "医薬品", "電気・ガス業", "情報・通信業",
    "陸運業", "倉庫・運輸関連業", "パルプ・紙",
    "サービス業",    # 教育・コンサル・住宅メンテ等
    "その他製品",    # 建材・オフィス家具等
    "金属製品",      # 建築・住宅向け製品
    "その他ETF",     # ETF（分散投資・安定収益）
}
CYCLICAL_SECTORS = {
    "化学", "鉄鋼", "非鉄金属", "機械", "電気機器", "輸送用機器",
    "銀行業", "証券、商品先物取引業", "保険業", "その他金融業",
    "不動産業", "建設業", "海運業", "空運業",
    "石油・石炭製品", "ゴム製品", "ガラス・土石製品",
    "繊維製品", "卸売業", "小売業", "精密機器",
    "水産・農林業", "鉱業",
}

SECTOR_OPTIONS = sorted(DEFENSIVE_SECTORS | CYCLICAL_SECTORS)

# 楽天証券準拠の手動セクター上書き
# Yahoo Finance Japan やyfinanceが誤分類する銘柄をここで正しい業種に固定する
MANUAL_SECTOR_OVERRIDE: dict[str, str] = {
    "2169.T": "サービス業",   # CDS：楽天証券ではサービス業
}

DROP_ALERT_THRESHOLD = -0.20  # 20%下落
TAX_RATE_TOKUTEI = 0.20315  # 特定口座の配当課税率（所得税15.315% + 住民税5%）
IRBANK_MIN_YEARS = 10  # スクリーニングに必要な最低年数


# ─── IRBANKスクレイピング & スクリーニング ──────────────────────

def _parse_irbank_num(s: str) -> float | None:
    """IRBANK の数値テキスト（「1.93兆」「-4550億」「211.69」等）を数値に変換"""
    s = s.strip().replace(",", "").replace("*", "").replace("△", "-")
    if not s or s == "-":
        return None
    try:
        if "兆" in s:
            return float(s.replace("兆", "")) * 1_0000_0000_0000
        if "億" in s:
            return float(s.replace("億", "")) * 1_0000_0000
        if "万" in s:
            return float(s.replace("万", "")) * 1_0000
        return float(s)
    except ValueError:
        return None


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_irbank_data(stock_code: str) -> dict | None:
    """IRBANKから過去の業績・財務・CF・配当データを取得"""
    url = f"https://irbank.net/{stock_code}/results"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table", class_="bar")
    if len(tables) < 4:
        return None

    def parse_table(table) -> dict[str, list]:
        thead = table.find("thead")
        cols = [th.get_text(strip=True) for th in thead.find_all("th")]
        data: dict[str, list] = {c: [] for c in cols}
        for row in table.find("tbody").find_all("tr"):
            tds = row.find_all("td")
            for i, td in enumerate(tds):
                if i < len(cols):
                    text_span = td.find("span", class_="text")
                    val = text_span.get_text(strip=True) if text_span else td.get_text(strip=True)
                    data[cols[i]].append(val)
        return data

    t_perf = parse_table(tables[0])   # 業績
    t_fin = parse_table(tables[1])    # 財務
    t_cf = parse_table(tables[2])     # CF
    t_div = parse_table(tables[3])    # 配当

    return {
        "performance": t_perf,
        "financial": t_fin,
        "cashflow": t_cf,
        "dividend": t_div,
    }


def _is_uptrend(values: list[float], allow_dips: int = 3) -> bool:
    """全体的に右肩上がりかを判定（多少の下落は許容）"""
    if len(values) < 3:
        return False
    dip_count = 0
    for i in range(1, len(values)):
        if values[i] < values[i - 1]:
            dip_count += 1
    # 下落回数が許容範囲内、かつ最後の値が最初の値より大きい
    return dip_count <= allow_dips and values[-1] > values[0]


def _is_dividend_uptrend(values: list[float]) -> bool:
    """配当が無配・減配なく右肩上がりか（記念配当による一時的な増を除く）"""
    if len(values) < 3:
        return False
    for i in range(1, len(values)):
        if values[i] <= 0:
            return False  # 無配
        if values[i] < values[i - 1] * 0.95:
            return False  # 5%超の減配
    return values[-1] > values[0]


def screen_stock(stock_code: str) -> dict | None:
    """1銘柄のIRBANKデータを取得し、8項目スクリーニングを実施"""
    raw = fetch_irbank_data(stock_code)
    if not raw:
        return None

    perf = raw["performance"]
    fin = raw["financial"]
    cf = raw["cashflow"]
    div = raw["dividend"]

    years = perf.get("年度", [])
    if not years:
        return None

    # 予想行と古いデータを除外し、直近10年分を取得
    valid_mask = [not y.endswith("予") for y in years]
    n = len(years)

    def extract(table_data: dict, col_name: str) -> list[float]:
        vals = table_data.get(col_name, [])
        result = []
        for i, v in enumerate(vals):
            if i < n and valid_mask[i]:
                parsed = _parse_irbank_num(v)
                if parsed is not None:
                    result.append(parsed)
        return result[-IRBANK_MIN_YEARS:]

    # 収益 / 営業収益 / 売上 / 売上高 のいずれか
    revenue = []
    for col_candidate in ["収益", "営業収益", "売上高", "売上"]:
        revenue = extract(perf, col_candidate)
        if revenue:
            break

    eps = extract(perf, "EPS")
    op_margin_raw = extract(perf, "営利率")
    # 「株主資本比率」or「自己資本比率」
    equity_ratio_raw = extract(fin, "株主資本比率")
    if not equity_ratio_raw:
        equity_ratio_raw = extract(fin, "自己資本比率")
    op_cf = extract(cf, "営業CF")
    cash = extract(cf, "現金等")

    # 配当
    div_years = div.get("年度", [])
    div_valid = [not y.endswith("予") for y in div_years]
    div_vals_raw = div.get("一株配当", [])
    div_vals = []
    for i, v in enumerate(div_vals_raw):
        if i < len(div_valid) and div_valid[i]:
            parsed = _parse_irbank_num(v)
            if parsed is not None:
                div_vals.append(parsed)
    div_vals = div_vals[-IRBANK_MIN_YEARS:]

    payout_raw = div.get("配当性向", [])
    payout_vals = []
    for i, v in enumerate(payout_raw):
        if i < len(div_valid) and div_valid[i]:
            parsed = _parse_irbank_num(v)
            if parsed is not None:
                payout_vals.append(parsed)
    payout_vals = payout_vals[-IRBANK_MIN_YEARS:]

    # データ不足チェック
    if len(revenue) < 5 or len(eps) < 5:
        return None

    # ─── 8項目判定 ───
    results = {}

    # ① 売上が全体的に右肩上がり
    results["売上成長"] = _is_uptrend(revenue)

    # ② EPSが全体的に右肩上がり
    results["EPS成長"] = _is_uptrend(eps)

    # ③ 営業利益率10%以上（10年平均）
    avg_margin = sum(op_margin_raw) / len(op_margin_raw) if op_margin_raw else 0
    results["営業利益率10%↑"] = avg_margin >= 10

    # ④ 自己資本比率40%以上（10年すべて）
    latest_equity = equity_ratio_raw[-1] if equity_ratio_raw else 0
    results["自己資本比率40%↑"] = all(v >= 40 for v in equity_ratio_raw) if equity_ratio_raw else False

    # ⑤ 営業CFが赤字なし
    results["営業CF黒字"] = all(v > 0 for v in op_cf) if op_cf else False

    # ⑥ 現金が全体的に右肩上がり
    results["現金増加"] = _is_uptrend(cash) if len(cash) >= 3 else False

    # ⑦ 配当が無配・減配なく右肩上がり
    results["連続増配"] = _is_dividend_uptrend(div_vals) if div_vals else False

    # ⑧ 配当性向50%以下（10年平均）
    avg_payout = sum(payout_vals) / len(payout_vals) if payout_vals else 100
    results["配当性向50%↓"] = 0 < avg_payout <= 50

    score = sum(results.values())

    return {
        "criteria": results,
        "score": score,
        "details": {
            "売上(直近)": revenue[-1] if revenue else 0,
            "EPS(直近)": eps[-1] if eps else 0,
            "営業利益率(10年平均)": round(avg_margin, 1),
            "自己資本比率(直近)": round(latest_equity, 1),
            "営業CF(直近)": op_cf[-1] if op_cf else 0,
            "現金等(直近)": cash[-1] if cash else 0,
            "一株配当(直近)": div_vals[-1] if div_vals else 0,
            "配当性向(10年平均)": round(avg_payout, 1),
            "データ年数": len(revenue),
        },
    }


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_high_dividend_candidates(min_yield: float = 3.5, max_pages: int = 8) -> list[tuple[str, float]]:
    """Yahoo Financeから配当利回りランキングを取得し、min_yield%以上の銘柄を返す"""
    all_stocks = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    for page in range(1, max_pages + 1):
        url = f"https://finance.yahoo.co.jp/stocks/ranking/dividendYield?page={page}"
        try:
            r = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table:
                break
            rows = table.find_all("tr")[1:]
            if not rows:
                break
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue
                name_text = cells[0].get_text(strip=True)
                code_match = re.search(r"(\d{4})", name_text)
                if not code_match:
                    continue
                code = code_match.group(1)
                yield_text = cells[4].get_text(strip=True)
                yield_text = yield_text.replace("+", "").replace("-", "").replace("%", "")
                try:
                    div_yield = float(yield_text)
                except ValueError:
                    continue
                if div_yield >= min_yield:
                    all_stocks.append((code, div_yield))
                else:
                    return all_stocks  # 利回り降順なのでこれ以降は不要
            time.sleep(0.3)
        except Exception:
            break
    return all_stocks


def get_economy_type(sector) -> str:
    if not isinstance(sector, str):
        return "―"
    s = normalize_sector(sector.strip())
    if s in DEFENSIVE_SECTORS:
        return "ディフェンシブ"
    if s in CYCLICAL_SECTORS:
        return "景気敏感"
    return "―"


# 旧略称 → 東証33業種正式名称 変換テーブル（保存済みデータの正規化用）
_SECTOR_NORMALIZE = {
    "石油・石炭":   "石油・石炭製品",
    "ガラス・土石": "ガラス・土石製品",
    "電気・ガス":   "電気・ガス業",
    "陸運":         "陸運業",
    "海運":         "海運業",
    "空運":         "空運業",
    "倉庫・運輸":   "倉庫・運輸関連業",
    "情報・通信":   "情報・通信業",
    "卸売":         "卸売業",
    "小売":         "小売業",
    "銀行":         "銀行業",
    "証券・先物":   "証券、商品先物取引業",
    "保険":         "保険業",
    "その他金融":   "その他金融業",
    "不動産":       "不動産業",
    "建設":         "建設業",
    "サービス":     "サービス業",
}

def normalize_sector(s) -> str:
    """略称で保存されている業種名を東証33業種正式名称に変換する"""
    if not isinstance(s, str):
        return ""
    return _SECTOR_NORMALIZE.get(s, s)


# ─── 日本株 業種取得 ──────────────────────────────────────────

# Yahoo Finance Japan の業種名 → 東証業種分類マッピング
# Yahoo Finance Japan 業種名 → 東証33業種正式名称（楽天証券準拠）
YJ_INDUSTRY_TO_SECTOR = {
    "水産・農林業": "水産・農林業",
    "鉱業": "鉱業",
    "建設業": "建設業",
    "食料品": "食料品",
    "繊維製品": "繊維製品",
    "パルプ・紙": "パルプ・紙",
    "化学": "化学",
    "医薬品": "医薬品",
    "石油・石炭製品": "石油・石炭製品",
    "ゴム製品": "ゴム製品",
    "ガラス・土石製品": "ガラス・土石製品",
    "鉄鋼": "鉄鋼",
    "非鉄金属": "非鉄金属",
    "金属製品": "金属製品",
    "機械": "機械",
    "電気機器": "電気機器",
    "輸送用機器": "輸送用機器",
    "精密機器": "精密機器",
    "その他製品": "その他製品",
    "電気・ガス業": "電気・ガス業",
    "陸運業": "陸運業",
    "海運業": "海運業",
    "空運業": "空運業",
    "倉庫・運輸関連業": "倉庫・運輸関連業",
    "情報・通信業": "情報・通信業",
    "卸売業": "卸売業",
    "小売業": "小売業",
    "銀行業": "銀行業",
    "証券、商品先物取引業": "証券、商品先物取引業",
    "保険業": "保険業",
    "その他金融業": "その他金融業",
    "不動産業": "不動産業",
    "サービス業": "サービス業",
}

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_sector_jp(stock_code: str) -> str:
    """Yahoo Finance Japan のプロフィールページから東証33業種を取得。
    ① th/td パターン（BeautifulSoup）
    ② dt/dd パターン（BeautifulSoup）
    ③ ページテキスト regex（業種[分類] の直後に既知業種名）
    """
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        url = f"https://finance.yahoo.co.jp/quote/{stock_code}.T/profile"
        r = requests.get(url, headers=headers, timeout=10)
        r.encoding = "utf-8"
        if r.status_code != 200:
            return ""
        page_text = r.text
        soup = BeautifulSoup(page_text, "html.parser")

        def _map(raw: str) -> str:
            """取得した業種名をTSE33正式名に変換し、旧略称も正規化する"""
            mapped = YJ_INDUSTRY_TO_SECTOR.get(raw, raw)
            return normalize_sector(mapped.strip())

        # ① th → sibling td（"業種分類" / "業種" どちらにも対応）
        for th in soup.find_all("th"):
            if "業種" in th.get_text(strip=True):
                td = th.find_next_sibling("td")
                if td:
                    industry_raw = td.get_text(strip=True)
                    if industry_raw:
                        return _map(industry_raw)

        # ② dt → sibling dd
        for dt in soup.find_all("dt"):
            if "業種" in dt.get_text(strip=True):
                dd = dt.find_next_sibling("dd")
                if dd:
                    industry_raw = dd.get_text(strip=True)
                    if industry_raw:
                        return _map(industry_raw)

        # ③ ページテキスト regex（"業種" の近傍に既知業種名）
        # 長い名前を先にチェックして部分マッチを防ぐ
        known = sorted(YJ_INDUSTRY_TO_SECTOR.keys(), key=len, reverse=True)
        pattern = r'業種[分類]*[\s\S]{0,40}?(' + '|'.join(re.escape(s) for s in known) + ')'
        m = re.search(pattern, page_text)
        if m:
            return _map(m.group(1))

    except Exception:
        pass
    return ""


# ─── 日本語銘柄名取得 ─────────────────────────────────────────

@st.cache_data(ttl=86400)
def fetch_jp_name(ticker: str) -> str:
    """Yahoo Finance Japan → kabutan → yfinance の順に日本語銘柄名を取得"""
    code = ticker.replace(".T", "")

    # ① Yahoo Finance Japan のタイトルから取得
    try:
        url = f"https://finance.yahoo.co.jp/quote/{code}.T"
        resp = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            title = soup.find("title")
            if title:
                t = title.get_text()
                # "トヨタ自動車(株)【7203】..." のパターン
                m = re.match(r"(.+?)[\(（(]株[\)）)]", t)
                if m:
                    return m.group(1).strip()
                # タイトルから【コード】より前の部分
                m2 = re.match(r"(.+?)【", t)
                if m2:
                    return m2.group(1).strip()
                # h1タグから取得を試みる
                h1 = soup.find("h1")
                if h1:
                    name = h1.get_text(strip=True)
                    if name and not name.isdigit():
                        return name
    except Exception:
        pass

    # ② kabutan のページタイトルから取得
    try:
        url2 = f"https://kabutan.jp/stock/?code={code}"
        resp2 = requests.get(url2, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        resp2.encoding = "utf-8"
        if resp2.status_code == 200:
            soup2 = BeautifulSoup(resp2.text, "html.parser")
            title2 = soup2.find("title")
            if title2:
                t2 = title2.get_text()
                m3 = re.match(r"(.+?)[\(（【\[]", t2)
                if m3:
                    name = m3.group(1).strip()
                    if name and not name.isdigit():
                        return name
    except Exception:
        pass

    # ③ yfinance の shortName / longName（英語表記だが最終手段）
    try:
        info = yf.Ticker(f"{code}.T").info
        name = info.get("shortName") or info.get("longName") or ""
        if name:
            return name
    except Exception:
        pass

    return ""


# ─── 企業詳細情報取得（PER/PBR/業種/概要） ────────────────────────

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_company_details(code: str) -> dict:
    """yfinance + 株探 / みんかぶ / IRBANK から PER・PBR・業種名・事業概要を取得"""
    result = {
        "per": None, "pbr": None, "roe": None, "roa": None,
        "industry_jp": "", "overview": "", "founded": "",
    }
    ticker = f"{code}.T"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    # ── yfinance から PER / PBR ──────────────────────────────────
    try:
        info = yf.Ticker(ticker).info
        per = info.get("trailingPE")
        pbr = info.get("priceToBook")
        result["per"] = round(float(per), 1) if per else None
        result["pbr"] = round(float(pbr), 1) if pbr else None
        roe = info.get("returnOnEquity")
        roa = info.get("returnOnAssets")
        result["roe"] = round(float(roe) * 100, 1) if roe else None
        result["roa"] = round(float(roa) * 100, 1) if roa else None
    except Exception:
        pass

    # ── Yahoo Finance Japan /profile から設立年月日・業種を取得 ──
    try:
        prof_resp = requests.get(
            f"https://finance.yahoo.co.jp/quote/{code}.T/profile",
            timeout=10, headers=headers,
        )
        prof_resp.encoding = "utf-8"
        if prof_resp.status_code == 200:
            page_text = prof_resp.text
            # Next.js JSONペイロードから正規表現で設立年月日を取得
            if not result["founded"]:
                m = re.search(
                    r'設立年月日.{0,60}?([0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日)',
                    page_text,
                )
                if m:
                    result["founded"] = m.group(1)
            # 業種も同様に取得
            if not result["industry_jp"]:
                prof_soup = BeautifulSoup(page_text, "html.parser")
                for th in prof_soup.find_all("th"):
                    label = th.get_text(strip=True)
                    td = th.find_next_sibling("td")
                    if not td:
                        continue
                    p = td.find("p")
                    text = (p.get_text(strip=True) if p else td.get_text(strip=True))
                    if label == "業種":
                        result["industry_jp"] = text
                        break
    except Exception:
        pass

    # ── ① 株探（kabutan.jp）：業種名・概要 ──────────────────────
    try:
        resp = requests.get(
            f"https://kabutan.jp/stock/?code={code}",
            timeout=10, headers=headers,
        )
        resp.encoding = "utf-8"
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            block = soup.find("div", class_="company_block")
            if block:
                for th in block.find_all("th"):
                    label = th.get_text(strip=True)
                    td = th.find_next_sibling("td")
                    if not td:
                        continue
                    text = td.get_text(strip=True)
                    if label == "概要" and not result["overview"]:
                        result["overview"] = text
                    elif label == "業種" and not result["industry_jp"]:
                        result["industry_jp"] = td.get_text(strip=True)
            # 業種リンクからも取得を試みる
            if not result["industry_jp"]:
                for a in soup.find_all("a", href=True):
                    if "/stock/meigara/?gyosyu=" in a["href"]:
                        result["industry_jp"] = a.get_text(strip=True)
                        break
    except Exception:
        pass

    # ── ② みんかぶ（minkabu.jp）：概要フォールバック ─────────────
    if not result["overview"]:
        try:
            resp2 = requests.get(
                f"https://minkabu.jp/stock/{code}",
                timeout=10, headers=headers,
            )
            resp2.encoding = "utf-8"
            if resp2.status_code == 200:
                soup2 = BeautifulSoup(resp2.text, "html.parser")
                body = soup2.find("div", id="sh_field_body")
                if body:
                    for div in body.select("div.ly_content_wrapper.size_ss"):
                        t = div.get_text(strip=True)
                        # リンクなし・一定の文字数があるものが概要
                        if t and not div.find("a") and len(t) > 10:
                            result["overview"] = t
                            break
        except Exception:
            pass

    # ── ③ IRBANK：概要フォールバック（EDINETコード経由） ──────────
    if not result["overview"]:
        try:
            # irbank.net/{stock_code} ページ内の会社ページリンクからEDINETコードを取得
            r0 = requests.get(
                f"https://irbank.net/{code}",
                timeout=10, headers=headers,
            )
            r0.encoding = "utf-8"
            if r0.status_code == 200:
                s0 = BeautifulSoup(r0.text, "html.parser")
                edinet_link = None
                for a in s0.find_all("a", href=True):
                    href = a["href"]
                    if re.match(r"^/E\d+$", href):
                        edinet_link = href
                        break
                if edinet_link:
                    r1 = requests.get(
                        f"https://irbank.net{edinet_link}",
                        timeout=10, headers=headers,
                    )
                    r1.encoding = "utf-8"
                    if r1.status_code == 200:
                        s1 = BeautifulSoup(r1.text, "html.parser")
                        msg = s1.find("p", class_="message")
                        if msg:
                            result["overview"] = msg.get_text(strip=True)
        except Exception:
            pass

    return result


# ─── 日本株 予想配当取得 ─────────────────────────────────────────

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_forecast_dividend_yj(stock_code: str) -> float:
    """Yahoo Finance Japan から予想配当(円/株)を取得"""
    url = f"https://finance.yahoo.co.jp/quote/{stock_code}.T"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        for dt in soup.find_all("dt"):
            if "予想配当" in dt.get_text():
                dd = dt.find_next_sibling("dd")
                if dd:
                    val = dd.get_text(strip=True).replace(",", "").replace("円", "")
                    return float(val)
    except Exception:
        pass
    return 0.0


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_annual_dividend_jp(stock_code: str) -> float:
    """IRBANKから最新の一株配当を取得（J-REIT分配金も対応）"""
    raw = fetch_irbank_data(stock_code)
    if not raw:
        return 0.0
    div = raw.get("dividend", {})
    # J-REITは「一口当分配金」「分配金」など列名が異なる場合がある
    for col in ["一株配当", "一口当分配金", "分配金", "1株配当"]:
        div_vals_raw = div.get(col, [])
        if div_vals_raw:
            for v in reversed(div_vals_raw):
                parsed = _parse_irbank_num(v)
                if parsed is not None and parsed > 0:
                    return parsed
    return 0.0


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_dividend_from_history(ticker: str) -> float:
    """yfinanceの配当履歴から直近1年の合計を取得"""
    try:
        divs = yf.Ticker(ticker).dividends
        if divs.empty:
            return 0.0
        one_year_ago = datetime.now() - timedelta(days=365)
        recent = divs[divs.index >= one_year_ago.strftime("%Y-%m-%d")]
        if not recent.empty:
            return float(recent.sum())
    except Exception:
        pass
    return 0.0


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_dividend_kabutan(stock_code: str) -> float:
    """kabutan から予想配当(円/株)を取得"""
    url = f"https://kabutan.jp/stock/?code={stock_code}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        # 配当(予想) または 分配金(予想) を含む行を探す
        for th in soup.find_all(["th", "dt"]):
            text = th.get_text(strip=True)
            if "配当" in text or "分配金" in text:
                td = th.find_next_sibling(["td", "dd"])
                if td:
                    val_text = td.get_text(strip=True).replace(",", "").replace("円", "").replace("-", "").strip()
                    try:
                        val = float(val_text)
                        if val > 0:
                            return val
                    except ValueError:
                        pass
    except Exception:
        pass
    return 0.0


# ─── 配当支払い月取得 ────────────────────────────────────────────

@st.cache_data(ttl=86400)
def fetch_dividend_months(ticker: str) -> list[int]:
    """yfinance の配当履歴（権利落ち日）から直近2年の実際の支払い月を返す。
    日本株(.T): 権利落ち月 + 3ヶ月 = 口座入金月（例: 3月→6月、9月→12月）
    海外株: 権利落ち月をそのまま使用（通常同月〜翌月に入金）
    """
    try:
        divs = yf.Ticker(ticker).dividends
        if divs.empty:
            return []
        two_years_ago = datetime.now() - timedelta(days=730)
        recent = divs[divs.index >= two_years_ago.strftime("%Y-%m-%d")]
        if recent.empty:
            return []
        ex_months = sorted(set(int(d.month) for d in recent.index))
        if ticker.endswith(".T"):
            # 日本株: 権利落ち月 → 支払月（+3ヶ月）
            # (m-1+3)%12+1 で 12月→3月 の繰り上がりも正しく処理
            pay_months = sorted(set(((m - 1 + 3) % 12) + 1 for m in ex_months))
            return pay_months
        return ex_months
    except Exception:
        return []


# ─── yfinance データ取得 ────────────────────────────────────────

@st.cache_data(ttl=3600)
def fetch_stock_info(ticker: str) -> dict:
    """yfinanceから現在株価・配当・セクター・会社名を取得"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        current_price = info.get("currentPrice") or info.get("regularMarketPrice", 0)
        dividend_yield = info.get("dividendYield", 0) or 0
        trailing_dividend = info.get("trailingAnnualDividendRate", 0) or 0
        sector_en = info.get("sector", "")
        industry_en = info.get("industry", "")
        # まず industry で詳細マッチ、なければ sector でフォールバック
        sector_jp = INDUSTRY_TO_JP_SECTOR.get(industry_en, "")
        if not sector_jp:
            sector_jp = SECTOR_EN_TO_JP_FALLBACK.get(sector_en, "")
        short_name = info.get("shortName", "") or ""
        long_name = info.get("longName", "") or ""

        # trailingAnnualDividendRate がない場合（ETF等）、配当履歴から直近1年分を合算
        if not trailing_dividend:
            try:
                divs = stock.dividends
                if not divs.empty:
                    one_year_ago = datetime.now() - timedelta(days=365)
                    recent = divs[divs.index >= one_year_ago.strftime("%Y-%m-%d")]
                    if not recent.empty:
                        trailing_dividend = float(recent.sum())
                        if current_price and not dividend_yield:
                            dividend_yield = trailing_dividend / current_price
            except Exception:
                pass

        quote_type = info.get("quoteType", "")
        # ETF/REITなどセクター情報がない場合、名前から推定
        if not sector_jp:
            name_upper = (long_name + " " + short_name).upper()
            # ETF判定を先に行う（REIT ETFはその他ETFに分類）
            if quote_type == "ETF":
                sector_jp = "その他ETF"
            elif "REIT" in name_upper or "リート" in name_upper:
                sector_jp = "不動産業"

        high_52w = info.get("fiftyTwoWeekHigh", 0) or 0

        return {
            "current_price": current_price,
            "dividend_yield": dividend_yield,
            "annual_dividend_per_share": trailing_dividend,
            "sector_en": sector_en,
            "sector_jp": sector_jp,
            "quote_type": quote_type,
            "short_name": short_name,
            "long_name": long_name,
            "high_52w": high_52w,
        }
    except Exception:
        return {
            "current_price": 0, "dividend_yield": 0,
            "annual_dividend_per_share": 0,
            "sector_en": "", "sector_jp": "",
            "quote_type": "",
            "short_name": "", "long_name": "",
            "high_52w": 0,
        }


# ─── 楽天証券CSV パーサー ──────────────────────────────────────

def _parse_num(s: str) -> float:
    s = str(s).strip().replace(",", "").replace("，", "")
    if not s or s in ("nan", "-", "－"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _detect_account_type(lines: list[str], header_idx: int) -> str:
    """ヘッダー行より前のセクション見出しから口座種別を判定"""
    for i in range(header_idx - 1, max(header_idx - 5, -1), -1):
        line = lines[i].strip().strip('"').replace("■", "")
        if "NISA" in line or "ＮＩＳＡまたはnisa" in line.lower():
            return "NISA"
        if "特定" in line:
            return "特定"
    return "特定"  # デフォルト


def _detect_csv_format(lines: list[str]) -> str:
    """CSVフォーマットを判定: 'all' (全資産) or 'jp' (国内株式)"""
    for line in lines:
        if "銘柄コード・ティッカー" in line:
            return "all"
        if "銘柄コード" in line:
            return "jp"
    return "unknown"


def _parse_all_format(lines: list[str]) -> list[dict]:
    """assetbalance(all) フォーマットをパース。
    種別,銘柄コード・ティッカー,銘柄,口座,保有数量,...の形式。
    国内株式のみ抽出（投資信託はスキップ）。
    """
    header_idx = None
    for i, line in enumerate(lines):
        if "銘柄コード・ティッカー" in line:
            header_idx = i
            break
    if header_idx is None:
        return []

    # ヘッダー行＋データ行を収集
    section_lines = [lines[header_idx]]
    for j in range(header_idx + 1, len(lines)):
        line = lines[j].strip()
        if not line:
            continue
        if line.startswith("■") or line.startswith('"■'):
            break
        section_lines.append(line)

    if len(section_lines) < 2:
        return []

    try:
        df = pd.read_csv(io.StringIO("\n".join(section_lines)), dtype=str)
    except Exception:
        return []

    cols = list(df.columns)
    type_col = next((c for c in cols if "種別" in c), None)
    ticker_col = next((c for c in cols if "銘柄コード" in c), None)
    name_col = next((c for c in cols if c == "銘柄" or "銘柄名" in c), None)
    account_col = next((c for c in cols if "口座" in c), None)
    shares_col = next((c for c in cols if "保有数量" in c), None)
    cost_col = next((c for c in cols if "平均取得価額" in c), None)
    cur_price_col = next((c for c in cols if c.startswith("現在値") and "前日比" not in c and "更新" not in c), None)
    market_val_col = next((c for c in cols if "時価評価額" in c and "円" in c), None)

    if not ticker_col or not shares_col:
        return []

    merged: dict[tuple[str, str], dict] = {}

    for _, row in df.iterrows():
        # 国内株式のみ（投資信託はスキップ）
        if type_col:
            stype = str(row[type_col]).strip().strip('"')
            if "国内株式" not in stype:
                continue

        raw_ticker = str(row[ticker_col]).strip().strip('"')
        if not raw_ticker or raw_ticker == "nan":
            continue

        shares = int(_parse_num(row[shares_col]))
        if shares <= 0:
            continue

        # 口座種別
        account_type = "特定"
        if account_col:
            acct_str = str(row[account_col]).strip().strip('"')
            if "NISA" in acct_str or "ＮＩＳＡまたはnisa" in acct_str.lower():
                account_type = "NISA"
            elif "特定" in acct_str:
                account_type = "特定"

        cost_total = _parse_num(row[cost_col]) * shares if cost_col else 0.0
        csv_price = _parse_num(row[cur_price_col]) if cur_price_col else 0.0
        csv_market_val = _parse_num(row[market_val_col]) if market_val_col else 0.0

        name = ""
        if name_col:
            name = str(row[name_col]).strip().strip('"')
            if name == "nan":
                name = ""

        key = (raw_ticker, account_type)
        if key in merged:
            m = merged[key]
            m["total_cost_amount"] += cost_total
            m["total_shares"] += shares
            m["csv_market_val"] += csv_market_val
        else:
            merged[key] = {
                "name": name,
                "total_shares": shares,
                "total_cost_amount": cost_total,
                "csv_price": csv_price,
                "csv_market_val": csv_market_val,
                "account": account_type,
            }

    return _merged_to_holdings(merged)


def _parse_jp_format(lines: list[str]) -> list[dict]:
    """assetbalance(JP) フォーマットをパース。
    ■特定口座/■NISAセクションごとにヘッダー行が出現する形式。
    """
    header_indices = [
        i for i, line in enumerate(lines)
        if "銘柄コード" in line
    ]
    if not header_indices:
        return []

    section_info: list[tuple[pd.DataFrame, str]] = []
    for idx, h_idx in enumerate(header_indices):
        account_type = _detect_account_type(lines, h_idx)
        if idx + 1 < len(header_indices):
            end_idx = header_indices[idx + 1]
        else:
            end_idx = len(lines)
        section_lines = [lines[h_idx]]
        for j in range(h_idx + 1, end_idx):
            line = lines[j].strip()
            if not line:
                continue
            if line.startswith("■") or line.startswith('"■'):
                continue
            first = line.lstrip('"')
            if first and first[0].isdigit():
                section_lines.append(line)
        if len(section_lines) < 2:
            continue
        try:
            df = pd.read_csv(io.StringIO("\n".join(section_lines)), dtype=str)
            section_info.append((df, account_type))
        except Exception:
            continue

    if not section_info:
        return []

    merged: dict[tuple[str, str], dict] = {}

    for df, account_type in section_info:
        cols = list(df.columns)
        ticker_col = next((c for c in cols if "銘柄コード" in c), None)
        name_col = next((c for c in cols if "銘柄名" in c), None)
        shares_col = next((c for c in cols if "保有数量" in c), None)
        cost_col = next((c for c in cols if "平均取得価額" in c), None)
        cost_total_col = next((c for c in cols if "取得総額" in c), None)
        cur_price_col = next((c for c in cols if c.startswith("現在値") and "前日比" not in c), None)
        market_val_col = next((c for c in cols if "時価評価額" in c), None)

        if not ticker_col or not shares_col:
            continue

        for _, row in df.iterrows():
            raw_ticker = str(row[ticker_col]).strip().strip('"')
            if not raw_ticker or raw_ticker == "nan":
                continue

            shares = int(_parse_num(row[shares_col]))
            if shares <= 0:
                continue

            if cost_total_col:
                cost_total = _parse_num(row[cost_total_col])
            elif cost_col:
                cost_total = _parse_num(row[cost_col]) * shares
            else:
                cost_total = 0.0

            csv_price = _parse_num(row[cur_price_col]) if cur_price_col else 0.0
            csv_market_val = _parse_num(row[market_val_col]) if market_val_col else 0.0

            name = ""
            if name_col:
                name = str(row[name_col]).strip().strip('"')
                if name == "nan":
                    name = ""

            key = (raw_ticker, account_type)
            if key in merged:
                m = merged[key]
                m["total_cost_amount"] += cost_total
                m["total_shares"] += shares
                m["csv_market_val"] += csv_market_val
            else:
                merged[key] = {
                    "name": name,
                    "total_shares": shares,
                    "total_cost_amount": cost_total,
                    "csv_price": csv_price,
                    "csv_market_val": csv_market_val,
                    "account": account_type,
                }

    return _merged_to_holdings(merged)


def _merged_to_holdings(merged: dict) -> list[dict]:
    """merged dict を holdings リストに変換"""
    holdings = []
    for (raw_ticker, _acct), m in merged.items():
        shares = m["total_shares"]
        avg_cost = m["total_cost_amount"] / shares if shares else 0.0

        if raw_ticker.isdigit() and len(raw_ticker) == 4:
            ticker = raw_ticker + ".T"
        else:
            ticker = raw_ticker.upper()

        holdings.append({
            "ticker": ticker,
            "shares": shares,
            "cost": round(avg_cost, 2),
            "sector": "",
            "name": m["name"],
            "csv_price": m.get("csv_price", 0.0),
            "account": m["account"],
        })
    return holdings


def parse_rakuten_csv(text: str) -> list[dict]:
    """楽天証券CSVテキスト（複数フォーマット対応）をパースして holdings リストを返す。
    assetbalance(all) と assetbalance(JP) の両方に対応。
    同一銘柄でも口座種別（特定/NISA）が異なれば別レコードとして保持する。
    """
    text = text.strip().lstrip("\ufeff")
    if not text:
        return []

    lines = text.splitlines()
    fmt = _detect_csv_format(lines)

    if fmt == "all":
        return _parse_all_format(lines)
    else:
        return _parse_jp_format(lines)


# ─── ポートフォリオ構築 ─────────────────────────────────────────

def build_portfolio_df(holdings: list[dict]) -> pd.DataFrame:
    rows = []
    for h in holdings:
        info = fetch_stock_info(h["ticker"])
        shares = h["shares"]
        cost = h["cost"]
        account = h.get("account", "特定")

        # 現在株価: yfinance と CSV の値を比較し、乖離が大きい場合は CSV を優先
        yf_price = info["current_price"]
        csv_price = h.get("csv_price", 0.0)
        if csv_price > 0 and yf_price > 0:
            ratio = yf_price / csv_price
            if ratio > 1.5 or ratio < 0.67:
                current_price = csv_price
            else:
                current_price = yf_price
        elif yf_price > 0:
            current_price = yf_price
        else:
            current_price = csv_price

        # 配当: 手動設定 > YJ予想 > IRBANK予想 > yfinance trailing
        div_overrides = st.session_state.get("div_overrides", {})
        manual_div = div_overrides.get(h["ticker"], 0.0)
        if manual_div > 0:
            annual_div = manual_div
        elif h["ticker"].endswith(".T"):
            # 日本株: IRBANK → yfinance trailing → yfinance履歴 → kabutan の順に試みる
            code = h["ticker"].replace(".T", "")
            irbank_div = fetch_annual_dividend_jp(code)
            if irbank_div > 0:
                annual_div = irbank_div
            elif info["annual_dividend_per_share"] > 0:
                annual_div = info["annual_dividend_per_share"]
            else:
                hist_div = fetch_dividend_from_history(h["ticker"])
                if hist_div > 0:
                    annual_div = hist_div
                else:
                    annual_div = fetch_dividend_kabutan(code)
        else:
            annual_div = info["annual_dividend_per_share"]
            if csv_price > 0 and yf_price > 0 and current_price == csv_price:
                split_ratio = csv_price / yf_price
                annual_div = annual_div * split_ratio

        # セクター取得優先順:
        #   ⓪ MANUAL_SECTOR_OVERRIDE（楽天証券準拠の手動上書き）最優先
        #   日本株(.T): Yahoo Finance Japan（TSE33直接） → yfinance → 名称推定
        #   米国株等:   保存値（正規化済） → yfinance → 名称推定
        saved_sector = normalize_sector(h.get("sector", ""))
        sector = MANUAL_SECTOR_OVERRIDE.get(h["ticker"], "")
        if sector:
            h["sector"] = sector
            # 手動上書きがある場合は後続処理をスキップ
        elif h["ticker"].endswith(".T"):
            # ① Yahoo Finance Japan（東証33業種を直接返す）
            code_s = h["ticker"].replace(".T", "")
            sector = fetch_sector_jp(code_s) or ""
            # ② yfinance フォールバック
            if not sector or sector == "未分類":
                sector = info["sector_jp"] or ""
        else:
            # 海外株: 保存値（正規化後） → yfinance
            sector = saved_sector if saved_sector and saved_sector != "未分類" else ""
            if not sector:
                sector = info["sector_jp"] or ""
        # ③ 銘柄名から推定
        if not sector or sector == "未分類":
            name_check = (
                h.get("name", "") + " " +
                info.get("short_name", "") + " " +
                info.get("long_name", "")
            ).upper()
            if info.get("quote_type", "") == "ETF" or "ETF" in name_check:
                sector = "その他ETF"
            elif "REIT" in name_check or "リート" in name_check or "INFRA" in name_check:
                sector = "不動産業"
            else:
                sector = "未分類"
        h["sector"] = sector

        # 会社名（日本語優先）
        name = h.get("name", "")
        if not name:
            name = fetch_jp_name(h["ticker"]) or info["short_name"] or info["long_name"] or ""
            h["name"] = name

        market_value = current_price * shares
        total_cost = cost * shares
        gain_pct = (current_price - cost) / cost if cost else 0
        annual_div_total = annual_div * shares

        # 税引後配当
        tax_rate = 0.0 if account == "NISA" else TAX_RATE_TOKUTEI
        annual_div_after_tax = annual_div_total * (1 - tax_rate)

        div_yield = (annual_div / current_price) if current_price else 0
        yield_on_cost = (annual_div / cost) if cost else 0

        div_months = fetch_dividend_months(h["ticker"])
        div_months_str = "・".join(f"{m}月" for m in div_months) if div_months else "―"

        rows.append({
            "銘柄": h["ticker"],
            "会社名": name,
            "口座": account,
            "セクター": sector,
            "景気区分": get_economy_type(sector),
            "株数": shares,
            "取得単価": cost,
            "現在株価": current_price,
            "評価額": market_value,
            "取得総額": total_cost,
            "損益率": gain_pct,
            "高値52w": info.get("high_52w", 0),
            "年間配当/株": annual_div,
            "年間配当(税引前)": annual_div_total,
            "年間配当(税引後)": annual_div_after_tax,
            "配当利回り(時価)": div_yield,
            "取得価格利回り": yield_on_cost,
            "配当月": div_months_str,
            "_div_months": div_months,
            "_annual_div_after_tax": annual_div_after_tax,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        total_div = df["年間配当(税引後)"].sum()
        df["配当割合"] = df["年間配当(税引後)"] / total_div if total_div else 0
    return df


# ═══ Streamlit UI ═══════════════════════════════════════════════

st.set_page_config(page_title="高配当株ポートフォリオ管理", layout="wide")

# ─── 認証 (streamlit-authenticator 0.2.x) ──────────────────────
_creds = dict(st.secrets.get("credentials", {}))
_cookie = dict(st.secrets.get("cookie", {
    "name": "portfolio_auth",
    "key": "change_this_secret_key",
    "expiry_days": 30,
}))

authenticator = stauth.Authenticate(
    {"usernames": {k: dict(v) for k, v in _creds.get("usernames", {}).items()}},
    _cookie.get("name", "portfolio_auth"),
    _cookie.get("key", "change_this_secret_key"),
    int(_cookie.get("expiry_days", 30)),
)
authenticator.login(location="main", fields={
    "Form name": "🔐 ログイン",
    "Username": "ユーザー名",
    "Password": "パスワード",
    "Login": "ログイン",
})

if st.session_state.get("authentication_status") is False:
    st.error("ユーザー名またはパスワードが違います")
    st.stop()
elif not st.session_state.get("authentication_status"):
    st.info("ユーザー名とパスワードを入力してください")
    st.stop()

# ログイン済み
st.title("高配当株ポートフォリオ管理ツール")

# --- サイドバー: 銘柄入力 ---
st.sidebar.header("保有銘柄の登録")

# ユーザーが切り替わったらデータをリセットして再ロード
_current_user = st.session_state.get("username", "")
if "holdings" not in st.session_state or st.session_state.get("_loaded_user") != _current_user:
    st.session_state.holdings = []
    st.session_state["_loaded_user"] = _current_user
    cloud_data = load_portfolio_from_github()
    if cloud_data:
        st.session_state.holdings = cloud_data

# ─── サイドバー：クラウド保存 ──────────────────────────────────────
st.sidebar.divider()
st.sidebar.subheader("☁️ クラウド保存")
if st.secrets.get("GITHUB_TOKEN", ""):
    if st.sidebar.button("💾 スマホと共有（GitHubに保存）", use_container_width=True):
        with st.sidebar:
            with st.spinner("保存中..."):
                ok = save_portfolio_to_github(st.session_state.holdings)
        if ok:
            st.sidebar.success("✅ 保存しました！スマホを更新すると反映されます")
        else:
            st.sidebar.error("❌ 保存に失敗しました")
else:
    st.sidebar.info("Streamlit CloudのSecretsに\nGITHUB_TOKENを設定すると\nスマホと共有できます")
st.sidebar.divider()

# ログアウト
st.sidebar.write(f"👤 {st.session_state.get('name', '')} でログイン中")
authenticator.logout("ログアウト", "sidebar")
st.sidebar.divider()

with st.sidebar.form("add_stock", clear_on_submit=True):
    st.subheader("銘柄を追加")
    col1, col2 = st.columns(2)
    new_ticker = col1.text_input("ティッカー", placeholder="8058.T")
    new_shares = col2.number_input("株数", min_value=1, value=100, step=1)
    new_cost = st.number_input("取得単価", min_value=0.01, value=1000.0, step=0.01)
    new_account = st.selectbox("口座", ["特定", "NISA"])
    new_sector = st.selectbox("セクター（空欄で自動取得）", ["自動取得"] + SECTOR_OPTIONS)
    submitted = st.form_submit_button("追加")
    if submitted and new_ticker:
        st.session_state.holdings.append({
            "ticker": new_ticker.upper().strip(),
            "shares": int(new_shares),
            "cost": float(new_cost),
            "sector": "" if new_sector == "自動取得" else new_sector,
            "account": new_account,
            "name": "",
        })
        st.rerun()

# 削除
if st.session_state.holdings:
    with st.sidebar.expander("銘柄を削除"):
        labels = [
            f"{h.get('name') or h['ticker']}（{h.get('account','特定')} {h['shares']}株）"
            for h in st.session_state.holdings
        ]
        del_idx = st.selectbox("削除する銘柄", range(len(labels)), format_func=lambda i: labels[i])
        if st.button("削除"):
            st.session_state.holdings.pop(del_idx)
            st.rerun()

# --- 楽天証券CSV取り込み ---
st.sidebar.divider()
st.sidebar.header("楽天証券CSV取り込み")

with st.sidebar.expander("📋 CSVの取得方法"):
    st.markdown("""
**① 楽天証券にログイン**

**② 保有証券ページを開く**
「資産管理」→「保有証券一覧」

**③ CSVをダウンロードまたはコピー**

**ファイルで取り込む場合：**
- ページ右上の「CSV」ボタンをクリック
- ダウンロードした `.csv` ファイルを「ファイル」タブからアップロード

**貼り付けで取り込む場合：**
- 「国内株式」または「全資産」タブを開く
- Ctrl+A（全選択）→ Ctrl+C（コピー）
- 「貼り付け」タブにCtrl+V で貼り付け

> 対応フォーマット：国内株式・全資産CSVどちらも可
""")

csv_tab1, csv_tab2 = st.sidebar.tabs(["貼り付け", "ファイル"])

with csv_tab1:
    csv_text = st.text_area(
        "CSVデータを貼り付け",
        height=150,
        placeholder="楽天証券からコピーしたCSVデータを貼り付けてください",
        key="csv_paste",
    )
    if st.button("取り込み", key="import_paste"):
        if csv_text.strip():
            parsed = parse_rakuten_csv(csv_text)
            if parsed:
                st.session_state.holdings = parsed
                save_portfolio_to_github(parsed)
                st.success(f"{len(parsed)}銘柄を取り込みました（既存データを更新）")
                st.rerun()
            else:
                st.error("CSVを解析できませんでした。カラム名を確認してください。")

with csv_tab2:
    uploaded = st.file_uploader("CSVファイル", type=["csv", "txt"], key="csv_upload")
    if uploaded is not None:
        raw = uploaded.read()
        csv_content = ""
        for enc in ["cp932", "shift_jis", "utf-8-sig", "utf-8"]:
            try:
                csv_content = raw.decode(enc)
                if "銘柄" in csv_content:
                    break
            except (UnicodeDecodeError, LookupError):
                continue

        if not csv_content:
            csv_content = raw.decode("cp932", errors="replace")

        if st.button("取り込み", key="import_file"):
            parsed = parse_rakuten_csv(csv_content)
            if parsed:
                st.session_state.holdings = parsed
                save_portfolio_to_github(parsed)
                st.success(f"{len(parsed)}銘柄を取り込みました（既存データを更新）")
                st.rerun()
            else:
                st.error("CSVを解析できませんでした。")

# --- セクター手動修正 ---
unclassified = [i for i, h in enumerate(st.session_state.holdings) if h.get("sector") in ("", "未分類")]
if unclassified:
    st.sidebar.divider()
    st.sidebar.warning(f"セクター未確定の銘柄が{len(unclassified)}件あります（yfinance取得後に自動設定されます）")

# --- メイン: ポートフォリオ表示 ---
if not st.session_state.holdings:
    st.info("サイドバーから銘柄を追加するか、楽天証券CSVを取り込んでください。")
    st.stop()

with st.spinner("株価・セクターデータを取得中..."):
    df = build_portfolio_df(st.session_state.holdings)

if df.empty:
    st.error("データを取得できませんでした。")
    st.stop()

# ─── KPI ────────────────────────────────────────────────────────
total_value = df["評価額"].sum()
total_cost = df["取得総額"].sum()
total_div_before = df["年間配当(税引前)"].sum()
total_div_after = df["年間配当(税引後)"].sum()
portfolio_yield = total_div_after / total_cost if total_cost else 0
defensive_value = df.loc[df["景気区分"] == "ディフェンシブ", "評価額"].sum()
defensive_ratio = defensive_value / total_value if total_value else 0

has_jp = any(h["ticker"].endswith(".T") for h in st.session_state.holdings)
cur = "¥" if has_jp else "$"

k1, k2, k3, k4 = st.columns(4)
k1.metric("総評価額", f"{cur}{total_value:,.0f}", f"{(total_value - total_cost) / total_cost:.1%}" if total_cost else "")
k1.caption(f"取得総額 {cur}{total_cost:,.0f}")
k2.metric("年間配当(税引後)", f"{cur}{total_div_after:,.0f}")
k3.metric("税引後利回り", f"{portfolio_yield:.2%}")
k4.metric("ディフェンシブ比率", f"{defensive_ratio:.1%}")

# 口座別 配当サマリー
nisa_div_before = df.loc[df["口座"] == "NISA", "年間配当(税引前)"].sum()
nisa_div_after = df.loc[df["口座"] == "NISA", "年間配当(税引後)"].sum()
tokutei_div_before = df.loc[df["口座"] == "特定", "年間配当(税引前)"].sum()
tokutei_div_after = df.loc[df["口座"] == "特定", "年間配当(税引後)"].sum()
tax_amount = total_div_before - total_div_after

s1, s2, s3 = st.columns(3)
s1.metric("NISA配当(非課税)", f"{cur}{nisa_div_after:,.0f}")
s2.metric("特定口座配当(税引後)", f"{cur}{tokutei_div_after:,.0f}",
          f"税引前 {cur}{tokutei_div_before:,.0f}")
s3.metric("年間税額", f"{cur}{tax_amount:,.0f}")

st.divider()

# ─── ポートフォリオ一覧 ─────────────────────────────────────────
st.subheader("保有銘柄一覧")

display_df = df.drop(columns=["_div_months", "_annual_div_after_tax", "高値52w"], errors="ignore").copy()
fmt = {
    "取得単価": f"{cur}{{:,.2f}}",
    "現在株価": f"{cur}{{:,.2f}}",
    "評価額": f"{cur}{{:,.0f}}",
    "取得総額": f"{cur}{{:,.0f}}",
    "損益率": "{:.1%}",
    "年間配当/株": f"{cur}{{:,.2f}}",
    "年間配当(税引前)": f"{cur}{{:,.0f}}",
    "年間配当(税引後)": f"{cur}{{:,.0f}}",
    "配当利回り(時価)": "{:.2%}",
    "取得価格利回り": "{:.2%}",
    "配当割合": "{:.1%}",
}
def _color_gain(val):
    if val > 0:
        return "color: green"
    elif val < 0:
        return "color: red"
    return ""

def _color_vs_ref(col, ref_col):
    ref = display_df[ref_col]
    return [
        "color: red" if v > r else "color: green" if v < r else ""
        for v, r in zip(col, ref)
    ]

st.dataframe(
    display_df.style
        .format(fmt)
        .map(_color_gain, subset=["損益率"])
        .apply(_color_vs_ref, ref_col="取得単価", subset=["現在株価"])
        .apply(_color_vs_ref, ref_col="取得総額", subset=["評価額"]),
    use_container_width=True,
    hide_index=True,
    column_config={
        "銘柄":             st.column_config.Column(label="コード"),
        "会社名":           st.column_config.Column(label="会社名"),
        "口座":             st.column_config.Column(label="口座"),
        "セクター":         st.column_config.Column(label="セクター"),
        "景気区分":         st.column_config.Column(label="景気"),
        "株数":             st.column_config.Column(label="株数"),
        "取得単価":         st.column_config.Column(label="取得単価"),
        "現在株価":         st.column_config.Column(label="現在株価"),
        "評価額":           st.column_config.Column(label="評価額"),
        "取得総額":         st.column_config.Column(label="取得額"),
        "損益率":           st.column_config.Column(label="損益率"),
        "年間配当/株":      st.column_config.Column(label="配当/株"),
        "年間配当(税引前)": st.column_config.Column(label="配当(税前)"),
        "年間配当(税引後)": st.column_config.Column(label="配当(税後)"),
        "配当利回り(時価)": st.column_config.Column(label="利回り(時価)"),
        "取得価格利回り":   st.column_config.Column(label="利回り(取得)"),
        "配当割合":         st.column_config.Column(label="配当割合"),
        "配当月":           st.column_config.Column(label="配当月"),
    },
)

# ─── 配当金単価 手動設定 ──────────────────────────────────────────
with st.expander("配当金単価を手動設定（yfinanceデータが不正確な場合）"):
    st.caption("IRBANKやYahoo Financeの予想配当金に合わせて修正できます。0のままにするとyfinanceの値を使用します。")
    if "div_overrides" not in st.session_state:
        st.session_state["div_overrides"] = {}

    div_rows = []
    for _, row in df.iterrows():
        ticker = row["銘柄"]
        override = st.session_state["div_overrides"].get(ticker, 0.0)
        div_rows.append({
            "銘柄": ticker,
            "会社名": row["会社名"],
            "yfinance値(円)": round(row["年間配当/株"], 2),
            "手動設定(円)": float(override) if override else 0.0,
        })

    edited = st.data_editor(
        pd.DataFrame(div_rows),
        column_config={
            "銘柄": st.column_config.TextColumn(disabled=True),
            "会社名": st.column_config.TextColumn(disabled=True),
            "yfinance値(円)": st.column_config.NumberColumn(disabled=True, format="%.2f"),
            "手動設定(円)": st.column_config.NumberColumn(min_value=0.0, step=1.0, format="%.0f"),
        },
        use_container_width=True,
        hide_index=True,
        key="div_override_editor",
    )

    if st.button("配当金設定を反映", key="apply_div_overrides"):
        new_overrides = {}
        for _, row in edited.iterrows():
            val = float(row["手動設定(円)"] or 0)
            if val > 0:
                new_overrides[row["銘柄"]] = val
        st.session_state["div_overrides"] = new_overrides
        st.success("設定を反映しました。画面を更新します。")
        st.rerun()

st.divider()

# ─── 買い増し検討チャート ────────────────────────────────────────
with st.expander("📉 買い増し検討チャート（直近高値から -20% 警戒ライン）", expanded=True):
    # 同一銘柄をNISA・特定口座で保有している場合は1本に集約
    _agg = df.groupby("銘柄", sort=False).agg(
        会社名=("会社名", "first"),
        現在株価=("現在株価", "first"),
        高値52w=("高値52w", "first"),
    ).reset_index()

    # 直近52週高値からの下落率（常に0以下）
    def _drawdown(r):
        if r["高値52w"] > 0:
            return min((r["現在株価"] - r["高値52w"]) / r["高値52w"], 0.0)
        return 0.0

    _agg["下落率"] = _agg.apply(_drawdown, axis=1)

    gain_df = _agg.copy()

    # 表示名：7文字で短縮（スマホ対応）
    def _short_name(r):
        name = r["会社名"] or r["銘柄"]
        return name[:7] + "…" if len(name) > 7 else name
    gain_df["表示名"] = gain_df.apply(_short_name, axis=1)
    # 下落率が大きい順（左ほど危険）に並べる
    gain_df = gain_df.sort_values("下落率", ascending=True)

    # 色分け: -20%以下=赤（買い増し要検討）, -10〜-20%=橙, 0〜-10%=黄, 0%=緑
    def _bar_color(v):
        if v <= -0.20:   return "#e74c3c"
        elif v <= -0.10: return "#e67e22"
        elif v < 0:      return "#f1c40f"
        else:            return "#2ecc71"

    gain_df["色"] = gain_df["下落率"].apply(_bar_color)
    gain_df["下落率(%)"] = gain_df["下落率"] * 100

    # 0%付近でもバーが見えるよう最小幅を確保
    x_range = gain_df["下落率(%)"].abs().max() or 10
    MIN_BAR = x_range * 0.015
    gain_df["表示値"] = gain_df["下落率(%)"].apply(
        lambda v: -MIN_BAR if (-MIN_BAR < v <= 0) else v
    )

    fig_gain = go.Figure()

    fig_gain.add_trace(go.Bar(
        x=gain_df["表示値"],
        y=gain_df["表示名"],
        orientation="h",
        marker_color=gain_df["色"].tolist(),
        text=[f"{v:.1f}%" for v in gain_df["下落率(%)"]],
        textposition="outside",
        textfont=dict(size=11),
        hovertemplate=(
            "<b>%{y}</b><br>高値比下落率: %{text}<extra></extra>"
        ),
    ))

    # -20% 買い増し検討ライン
    fig_gain.add_vline(
        x=-20,
        line_width=2, line_dash="dash", line_color="#e74c3c",
        annotation_text="−20%",
        annotation_position="top right",
        annotation_font_color="#e74c3c",
        annotation_font_size=11,
    )
    # 0% 基準ライン（高値＝現在値）
    fig_gain.add_vline(
        x=0, line_width=1, line_dash="dot", line_color="#888",
    )

    fig_gain.update_layout(
        title=dict(text="直近52週高値からの下落率（-20%で買い増し検討）", font=dict(size=13)),
        height=max(160, len(gain_df) * 26 + 50),
        xaxis=dict(
            title="",
            ticksuffix="%",
            tickfont=dict(size=10),
            zeroline=False,
            automargin=True,
            fixedrange=True,
            range=[-55, 8],          # 左端:-55% / 右端:+8% 固定
        ),
        yaxis=dict(
            title="",
            tickfont=dict(size=10),
            fixedrange=True,
            side="right",
        ),
        margin=dict(l=4, r=85, t=36, b=28),
        bargap=0.25,
        showlegend=False,
        plot_bgcolor="#fafafa",
        dragmode=False,
    )
    st.plotly_chart(
        fig_gain,
        use_container_width=True,
        config={
            "scrollZoom": False,
            "doubleClick": False,
            "displayModeBar": False,
        },
    )

    # 凡例（色の意味）
    st.markdown(
        "<div style='font-size:0.78em;display:flex;gap:12px;flex-wrap:wrap;margin-top:-8px;'>"
        "<span>🟢 含み益</span>"
        "<span>🟡 -10%未満</span>"
        "<span>🟠 -10〜-20%</span>"
        "<span>🔴 -20%以下（買い増し検討）</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    # -20%以下の銘柄をコンパクトなカードで表示（集約後のgain_dfを使用）
    alerts = gain_df[gain_df["下落率"] <= DROP_ALERT_THRESHOLD]
    if not alerts.empty:
        st.error(f"⚠️ {len(alerts)}銘柄が直近高値から -20%以下")
        cols = st.columns(min(len(alerts), 3))
        for i, (_, row) in enumerate(alerts.iterrows()):
            with cols[i % min(len(alerts), 3)]:
                st.markdown(
                    f"<div style='background:#fdecea;border:1px solid #e74c3c;"
                    f"border-radius:6px;padding:8px;font-size:0.85em;text-align:center;'>"
                    f"<b>{row['会社名'] or row['銘柄']}</b><br>"
                    f"<span style='color:#e74c3c;font-size:1.1em;font-weight:bold;'>"
                    f"{row['下落率']:.1%}</span><br>"
                    f"<small>52週高値 {cur}{row['高値52w']:,.0f} → 現在 {cur}{row['現在株価']:,.0f}</small>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
    else:
        st.success("✅ 直近高値から20%以上下落している銘柄はありません")

st.divider()

# ─── 分析グラフ ─────────────────────────────────────────────────
st.subheader("分析")
tab1, tab2, tab3, tab4 = st.tabs(["セクター分散", "配当割合", "景気区分", "銘柄スクリーニング"])

with tab1:
    sector_agg = df.groupby("セクター")["評価額"].sum().reset_index()
    sector_agg["割合"] = sector_agg["評価額"] / sector_agg["評価額"].sum()
    sector_agg = sector_agg.sort_values("評価額", ascending=False)
    sector_agg["ラベル"] = sector_agg.apply(
        lambda r: f"{r['セクター']}<br>{r['割合']:.1%}", axis=1
    )

    fig_sector = px.treemap(
        sector_agg,
        path=["ラベル"],
        values="評価額",
        title="セクター別 評価額構成",
        color="評価額",
        color_continuous_scale="Blues",
    )
    fig_sector.update_traces(
        textinfo="label",
        textfont=dict(size=13),
        hovertemplate=(
            "<b>%{label}</b><br>"
            f"評価額: {cur}%{{value:,.0f}}<br>"
            "割合: %{percentRoot:.1%}<extra></extra>"
        ),
    )
    fig_sector.update_layout(
        height=480,
        margin=dict(l=10, r=10, t=40, b=10),
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig_sector, use_container_width=True)

with tab2:
    # 配当割合（会社名+口座で表示）
    div_df = df[df["年間配当(税引後)"] > 0].copy()
    div_df = div_df.sort_values("年間配当(税引後)", ascending=False)
    div_df["表示名"] = div_df.apply(
        lambda r: f"{r['会社名'] or r['銘柄']}", axis=1
    )
    div_total = div_df["年間配当(税引後)"].sum()
    div_df["割合"] = div_df["年間配当(税引後)"] / div_total
    div_df["ラベル"] = div_df.apply(
        lambda r: f"{r['表示名']}<br>{r['割合']:.1%}", axis=1
    )

    # ツリーマップ：面積＝割合、会社名を直接表示
    fig_div = px.treemap(
        div_df,
        path=["ラベル"],
        values="年間配当(税引後)",
        title="銘柄別 年間配当割合（税引後）",
        color="年間配当(税引後)",
        color_continuous_scale="Blues",
    )
    fig_div.update_traces(
        textinfo="label",
        textfont=dict(size=13),
        hovertemplate=(
            "<b>%{label}</b><br>"
            f"年間配当: {cur}%{{value:,.0f}}<br>"
            "割合: %{percentRoot:.1%}<extra></extra>"
        ),
    )
    fig_div.update_layout(
        height=480,
        margin=dict(l=10, r=10, t=40, b=10),
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig_div, use_container_width=True)

    # 口座別 配当比較（棒グラフ）
    acct_div = df.groupby("口座").agg(
        税引前=("年間配当(税引前)", "sum"),
        税引後=("年間配当(税引後)", "sum"),
    ).reset_index()
    acct_melt = acct_div.melt(id_vars="口座", var_name="区分", value_name="金額")
    acct_melt["金額テキスト"] = acct_melt["金額"].apply(
        lambda v: f"{cur}{v:,.0f}"
    )
    fig_acct = px.bar(
        acct_melt,
        x="口座", y="金額", color="区分", barmode="group",
        title="口座別 年間配当（税引前 vs 税引後）",
        text="金額テキスト",
        color_discrete_map={"税引前": "#5b9bd5", "税引後": "#70ad47"},
    )
    fig_acct.update_traces(
        textposition="outside",
        textfont=dict(size=13),
    )
    fig_acct.update_layout(
        yaxis=dict(
            title="",
            tickprefix=cur,
            tickformat=",.0f",
            showgrid=True,
            gridcolor="#eeeeee",
            fixedrange=True,
        ),
        xaxis=dict(
            title="",
            fixedrange=True,
        ),
        plot_bgcolor="white",
        legend_title_text="",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=380,
        margin=dict(t=60, b=20),
        dragmode=False,
    )
    st.plotly_chart(
        fig_acct,
        use_container_width=True,
        config={
            "scrollZoom": False,
            "doubleClick": False,
            "displayModeBar": False,
        },
    )

    # 配当利回り比較
    yield_df = df[df["取得価格利回り"] > 0].copy()
    yield_df["表示名"] = yield_df.apply(
        lambda r: f"{r['会社名'] or r['銘柄']}（{r['口座']}）", axis=1
    )
    yield_df = yield_df.sort_values("取得価格利回り", ascending=True).reset_index(drop=True)
    yield_df["利回りテキスト"] = yield_df["取得価格利回り"].apply(lambda v: f"{v*100:.2f}%")

    fig_yield = go.Figure(go.Bar(
        x=yield_df["取得価格利回り"],
        y=yield_df["表示名"],
        orientation="h",
        text=yield_df["利回りテキスト"],
        textposition="outside",
        textfont=dict(size=12, color="#333333"),
        marker=dict(
            color=yield_df["取得価格利回り"],
            colorscale=[[0, "#a8d5a2"], [1, "#1a7a2e"]],
            showscale=False,
        ),
        hovertemplate="%{y}<br>利回り: %{text}<extra></extra>",
    ))
    # 平均利回り基準線
    avg_yield = yield_df["取得価格利回り"].mean()
    fig_yield.add_vline(
        x=avg_yield, line_dash="dash", line_color="#e67e22", line_width=1.5,
        annotation_text=f"平均 {avg_yield*100:.2f}%",
        annotation_position="top right",
        annotation_font=dict(color="#e67e22", size=11),
    )
    max_yield = yield_df["取得価格利回り"].max()
    fig_yield.update_layout(
        title="銘柄別 取得価格ベース配当利回り",
        xaxis=dict(
            tickformat=".1%",
            range=[0, max_yield * 1.35],
            showgrid=True, gridcolor="#eeeeee",
            fixedrange=True,       # ズーム無効
        ),
        yaxis=dict(
            title="",
            fixedrange=True,       # ズーム無効
        ),
        height=max(250, len(yield_df) * 32 + 80),
        margin=dict(l=10, r=20, t=50, b=20),
        plot_bgcolor="white",
        dragmode=False,            # ドラッグ操作無効
    )
    st.plotly_chart(
        fig_yield,
        use_container_width=True,
        config={
            "scrollZoom": False,       # スクロールズーム無効
            "doubleClick": False,      # ダブルタップリセット無効
            "displayModeBar": False,   # ツールバー非表示
        },
    )

    # 月別配当カレンダー（支払月ベース）
    st.subheader("配当月カレンダー（口座入金月）")
    monthly_div = {m: 0.0 for m in range(1, 13)}
    monthly_stocks: dict[int, list[str]] = {m: [] for m in range(1, 13)}
    for _, row in df.iterrows():
        months = row.get("_div_months", [])
        after_tax = row.get("_annual_div_after_tax", 0)
        name_label = row["会社名"] or row["銘柄"]
        if months and after_tax > 0:
            per_month = after_tax / len(months)
            for m in months:
                monthly_div[m] += per_month
                monthly_stocks[m].append(name_label)

    month_df = pd.DataFrame({
        "月": [f"{m}月" for m in range(1, 13)],
        "配当金(税引後)": [monthly_div[m] for m in range(1, 13)],
        "銘柄": [", ".join(monthly_stocks[m]) if monthly_stocks[m] else "―" for m in range(1, 13)],
    })
    fig_cal = px.bar(
        month_df, x="月", y="配当金(税引後)",
        title="月別 受取配当金（税引後・概算）― 口座入金月ベース",
        text_auto=",.0f",
        hover_data={"銘柄": True},
        color_discrete_sequence=["#3498db"],
    )
    fig_cal.update_layout(
        yaxis=dict(title="円", fixedrange=True),
        xaxis=dict(title="", fixedrange=True),
        dragmode=False,
    )
    st.plotly_chart(
        fig_cal,
        use_container_width=True,
        config={
            "scrollZoom": False,
            "doubleClick": False,
            "displayModeBar": False,
        },
    )

    # 月別明細テーブル
    cal_table = month_df[month_df["配当金(税引後)"] > 0].copy()
    cal_table["配当金(税引後)"] = cal_table["配当金(税引後)"].map("¥{:,.0f}".format)
    st.dataframe(cal_table, use_container_width=True, hide_index=True)

with tab3:
    eco_agg = df.groupby("景気区分")["評価額"].sum().reset_index()
    # 「―」(未分類)を除外
    eco_agg = eco_agg[eco_agg["景気区分"] != "―"]
    total_eco = eco_agg["評価額"].sum()
    eco_agg["割合"] = eco_agg["評価額"] / total_eco if total_eco else 0
    eco_agg["ラベル"] = eco_agg.apply(
        lambda r: f"{r['景気区分']}<br>{r['割合']:.1%}<br>¥{r['評価額']:,.0f}", axis=1
    )
    colors = {"ディフェンシブ": "#2ecc71", "景気敏感": "#e74c3c"}
    eco_agg["色"] = eco_agg["景気区分"].map(colors).fillna("#aaaaaa")
    fig_eco = px.treemap(
        eco_agg, path=["ラベル"], values="評価額",
        title="ディフェンシブ vs 景気敏感",
        color="景気区分",
        color_discrete_map=colors,
    )
    fig_eco.update_traces(
        textinfo="label",
        textfont=dict(size=14),
        hovertemplate="<b>%{label}</b><extra></extra>",
    )
    fig_eco.update_layout(height=300, coloraxis_showscale=False, margin=dict(t=40, l=4, r=4, b=4))
    st.plotly_chart(fig_eco, use_container_width=True)

    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=defensive_ratio * 100,
        title={"text": "ディフェンシブ比率"},
        number={"suffix": "%"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": "#2ecc71"},
            "steps": [
                {"range": [0, 40], "color": "#fadbd8"},
                {"range": [40, 60], "color": "#fdebd0"},
                {"range": [60, 100], "color": "#d5f5e3"},
            ],
            "threshold": {"line": {"color": "black", "width": 2}, "thickness": 0.75, "value": 50},
        },
    ))
    st.plotly_chart(fig_gauge, use_container_width=True)

with tab4:
    st.markdown("""
    **新規投資候補 — IRBANKスクリーニング（過去10年間）**

    Yahoo Finance配当利回りランキングから**3.5%以上**の銘柄を取得し、
    **保有銘柄を除外**した上で8項目をスクリーニングします。

    | # | 条件 |
    |---|------|
    | 1 | 売上が全体的に右肩上がり |
    | 2 | EPSが全体的に右肩上がり |
    | 3 | 営業利益率が10%以上（10年平均） |
    | 4 | 自己資本比率が40%以上（直近） |
    | 5 | 営業CFが赤字なし |
    | 6 | 現金等が全体的に右肩上がり |
    | 7 | 一株配当が無配・減配なく増配傾向 |
    | 8 | 配当性向が50%以下（10年平均） |
    """)

    # 保有銘柄コード一覧（除外用）
    held_codes = set(
        h["ticker"].replace(".T", "")
        for h in st.session_state.holdings
        if h["ticker"].endswith(".T")
    )
    if held_codes:
        st.caption(f"保有中の{len(held_codes)}銘柄は候補から除外されます")

    max_screen = st.slider(
        "スクリーニング対象数（配当利回り上位から）",
        min_value=20, max_value=200, value=80, step=10,
    )

    if st.button("新規投資候補をスクリーニング", type="primary"):
        # Step 1: Yahoo Finance から配当利回り3.5%以上の銘柄を取得
        with st.spinner("Yahoo Finance から高配当銘柄リストを取得中..."):
            candidates = fetch_high_dividend_candidates(min_yield=3.5)

        if not candidates:
            st.error("配当利回りランキングを取得できませんでした。")
        else:
            # 保有銘柄を除外
            candidates = [(code, dy) for code, dy in candidates if code not in held_codes]
            candidates = candidates[:max_screen]

            st.info(f"対象: {len(candidates)}銘柄（保有銘柄除外済）")

            # Step 2: IRBANKスクリーニング
            progress = st.progress(0, text="IRBANKからデータ取得中...")
            results = []
            for i, (code, div_yield) in enumerate(candidates):
                progress.progress(
                    (i + 1) / len(candidates),
                    text=f"IRBANKからデータ取得中... {code} ({i+1}/{len(candidates)})",
                )
                result = screen_stock(code)
                if result:
                    result["code"] = code
                    result["dividend_yield"] = round(div_yield, 2)
                    # Yahoo Finance Japanから日本語名を取得
                    result["name"] = fetch_jp_name(code) or code
                    results.append(result)
                time.sleep(0.5)
            progress.empty()

            if not results:
                st.warning("スクリーニング条件を満たす銘柄がありませんでした。")
            else:
                results.sort(key=lambda x: (-x["score"], -x["dividend_yield"]))
                st.session_state["screen_results"] = results

    # 結果表示
    if "screen_results" in st.session_state:
        results = st.session_state["screen_results"]
        top10 = results[:10]

        st.subheader(f"おすすめ上位{min(10, len(top10))}社（新規投資候補）")

        # 企業詳細情報を一括取得（キャッシュ済みなので高速）
        with st.spinner("企業情報（業種・PER・PBR・概要）を取得中..."):
            company_details = {r["code"]: fetch_company_details(r["code"]) for r in top10}

        criteria_names = [
            "売上成長", "EPS成長", "営業利益率10%↑", "自己資本比率40%↑",
            "営業CF黒字", "現金増加", "連続増配", "配当性向50%↓",
        ]
        summary_rows = []
        for r in top10:
            cd = company_details.get(r["code"], {})
            row = {
                "銘柄コード": r["code"],
                "会社名": r["name"],
                "業種": cd.get("industry_jp", "―"),
                "スコア": f"{r['score']}/8",
                "配当利回り": f"{r.get('dividend_yield', 0):.2f}%",
                "PER": f"{cd['per']:.1f}倍" if cd.get("per") else "―",
                "PBR": f"{cd['pbr']:.2f}倍" if cd.get("pbr") else "―",
            }
            for cn in criteria_names:
                row[cn] = "○" if r["criteria"].get(cn) else "×"
            d = r["details"]
            row["営業利益率"] = f"{d.get('営業利益率(10年平均)') or d.get('営業利益率(3年平均)', 0)}%"
            row["自己資本比率"] = f"{d.get('自己資本比率(直近)', 0)}%"
            row["配当性向"] = f"{d.get('配当性向(10年平均)') or d.get('配当性向(3年平均)', 0)}%"
            row["一株配当"] = f"{d.get('一株配当(直近)', 0):.1f}円"
            summary_rows.append(row)

        # HTML テーブルで表示（テキスト折り返し対応）
        cols_order = [
            "銘柄コード", "会社名", "業種", "スコア", "配当利回り",
            "PER", "PBR",
        ] + criteria_names + ["営業利益率", "自己資本比率", "配当性向", "一株配当"]

        # 表示ヘッダー名（短縮）
        col_labels = {
            "銘柄コード":     "コード",
            "配当利回り":     "利回り",
            "売上成長":       "売上↑",
            "EPS成長":        "EPS↑",
            "営業利益率10%↑": "営利\n10%↑",
            "自己資本比率40%↑":"自己資\n40%↑",
            "営業CF黒字":     "CF\n黒字",
            "現金増加":       "現金↑",
            "連続増配":       "増配",
            "配当性向50%↓":  "性向\n50%↓",
            "営業利益率":     "営利率",
            "自己資本比率":   "自己資本",
            "一株配当":       "配当/株",
        }

        # 列幅（px）。指定なしは auto
        col_widths = {
            "銘柄コード": "52", "スコア": "48", "配当利回り": "52",
            "PER": "52", "PBR": "52",
            "売上成長": "42", "EPS成長": "42", "営業利益率10%↑": "48",
            "自己資本比率40%↑": "52", "営業CF黒字": "42", "現金増加": "42",
            "連続増配": "38", "配当性向50%↓": "48",
            "営業利益率": "52", "自己資本比率": "56", "配当性向": "52", "一株配当": "52",
        }

        def _score_bg(val):
            if "8/8" in val or "7/8" in val:
                return "background:#d5f5e3;"
            if "6/8" in val:
                return "background:#fdebd0;"
            return ""

        def _cell_style(col, val):
            if col == "スコア":
                return _score_bg(val)
            if col in criteria_names:
                if val == "○":
                    return "color:#27ae60;font-weight:bold;"
                if val == "×":
                    return "color:#e74c3c;"
            return ""

        th_base = (
            "background:#f0f2f6;padding:4px 5px;text-align:center;"
            "border:1px solid #ddd;white-space:pre-line;font-size:0.78em;line-height:1.3;"
        )
        td_base = (
            "padding:4px 5px;border:1px solid #ddd;"
            "white-space:normal;word-break:break-all;font-size:0.80em;"
            "vertical-align:middle;text-align:center;"
        )

        def _th(col):
            label = col_labels.get(col, col)
            w = col_widths.get(col, "")
            w_style = f"width:{w}px;min-width:{w}px;" if w else ""
            return f"<th style='{th_base}{w_style}'>{label}</th>"

        html_rows = []
        html_rows.append(
            "<table style='border-collapse:collapse;font-size:0.80em;'>"
            "<thead><tr>"
            + "".join(_th(c) for c in cols_order)
            + "</tr></thead><tbody>"
        )
        nowrap_cols = {"会社名", "業種", "PER", "PBR"}
        for row in summary_rows:
            html_rows.append("<tr>")
            for c in cols_order:
                val = str(row.get(c, ""))
                extra = _cell_style(c, val)
                if c in nowrap_cols:
                    extra += "white-space:nowrap;word-break:normal;"
                html_rows.append(f"<td style='{td_base}{extra}'>{val}</td>")
            html_rows.append("</tr>")
        html_rows.append("</tbody></table>")

        st.markdown("".join(html_rows), unsafe_allow_html=True)

        # セクター別 ROE / ROA 平均を計算（top10 ベース）
        _sec_roe: dict[str, list] = {}
        _sec_roa: dict[str, list] = {}
        for _cd in company_details.values():
            _ind = _cd.get("industry_jp", "")
            if not _ind:
                continue
            if _cd.get("roe") is not None:
                _sec_roe.setdefault(_ind, []).append(_cd["roe"])
            if _cd.get("roa") is not None:
                _sec_roa.setdefault(_ind, []).append(_cd["roa"])
        sec_roe_avg = {k: round(sum(v) / len(v), 1) for k, v in _sec_roe.items()}
        sec_roa_avg = {k: round(sum(v) / len(v), 1) for k, v in _sec_roa.items()}

        # セクター��� 営業利益率 平均（IRBANK データから）
        _sec_op: dict[str, list] = {}
        for _r in top10:
            _ind = company_details.get(_r["code"], {}).get("industry_jp", "")
            if not _ind:
                continue
            _d = _r["details"]
            _op = _d.get("営業利益率(10年平均)") or _d.get("営業利益率(3年平均)")
            if _op is not None:
                _sec_op.setdefault(_ind, []).append(float(_op))
        sec_op_avg = {k: round(sum(v) / len(v), 1) for k, v in _sec_op.items()}

        # 各銘柄の詳細
        for r in top10:
            cd = company_details.get(r["code"], {})
            with st.expander(
                f"{'★' * r['score']}{'☆' * (8 - r['score'])} "
                f"{r['name']} ({r['code']}) — {r['score']}/8 — 利回り{r['dividend_yield']}%"
            ):
                # 概要・強み
                overview = cd.get("overview", "")
                if overview:
                    st.markdown("**📋 概要（事業内容）**")
                    st.write(overview)
                else:
                    st.caption("事業概要を取得できませんでした。")

                st.divider()

                # 財務指標（HTMLグリッドで全文表示）
                d = r["details"]
                per_val = cd.get("per")
                pbr_val = cd.get("pbr")
                industry_val = cd.get("industry_jp", "")

                founded_val = cd.get("founded", "")
                roe_val = cd.get("roe")
                roa_val = cd.get("roa")
                roe_avg = sec_roe_avg.get(industry_val)
                roa_avg = sec_roa_avg.get(industry_val)

                # (ラベル, 値, セクター平均サブテキスト)
                metrics = [
                    ("業種",                  industry_val or "―",                          None),
                    ("設立",                  founded_val or "―",                           None),
                    ("配当利回り",             f"{r.get('dividend_yield', 0):.2f}%",          None),
                    ("PER",                   f"{per_val:.1f}倍" if per_val else "―",        None),
                    ("PBR",                   f"{pbr_val:.2f}倍" if pbr_val else "―",        None),
                    ("ROE",                   f"{roe_val:.1f}%" if roe_val is not None else "―",
                                              f"業種平均 {roe_avg:.1f}%" if roe_avg is not None else None),
                    ("ROA",                   f"{roa_val:.1f}%" if roa_val is not None else "―",
                                              f"業種平均 {roa_avg:.1f}%" if roa_avg is not None else None),
                    ("営業利益率\n(10年平均)", f"{d.get('営業利益率(10年平均)') or d.get('営業利益率(3年平均)', 0)}%",
                                              f"業種平均 {sec_op_avg[industry_val]:.1f}%" if industry_val in sec_op_avg else None),
                    ("自己資本比率\n(直近)",   f"{d.get('自己資本比率(直近)', 0)}%",           None),
                    ("配当性向\n(10年平均)",   f"{d.get('配当性向(10年平均)') or d.get('配当性向(3年平均)', 0)}%", None),
                    ("一株配当\n(直近)",       f"{d.get('一株配当(直近)', 0):.1f}円",          None),
                ]
                card_style = (
                    "display:inline-block;min-width:100px;padding:8px 12px;"
                    "background:#f8f9fa;border:1px solid #e0e0e0;border-radius:6px;"
                    "margin:4px;vertical-align:top;text-align:center;"
                )
                label_style = "font-size:0.75em;color:#555;white-space:pre-line;line-height:1.3;"
                value_style = "font-size:1.05em;font-weight:bold;margin-top:4px;"
                avg_style   = "font-size:0.72em;color:#888;margin-top:3px;"
                cards_html = "<div style='display:flex;flex-wrap:wrap;gap:4px;margin-top:6px;'>"
                for label, value, avg_text in metrics:
                    avg_html = f"<div style='{avg_style}'>{avg_text}</div>" if avg_text else ""
                    cards_html += (
                        f"<div style='{card_style}'>"
                        f"<div style='{label_style}'>{label}</div>"
                        f"<div style='{value_style}'>{value}</div>"
                        f"{avg_html}"
                        f"</div>"
                    )
                cards_html += "</div>"
                st.markdown(cards_html, unsafe_allow_html=True)

        # 全結果も表示（折りたたみ）
        if len(results) > 10:
            with st.expander(f"全{len(results)}銘柄の結果を表示"):
                all_rows = []
                for r in results:
                    row = {
                        "コード": r["code"],
                        "会社名": r["name"],
                        "スコア": r["score"],
                        "配当利回り": f"{r['dividend_yield']}%",
                    }
                    for cn in criteria_names:
                        row[cn] = "○" if r["criteria"].get(cn) else "×"
                    all_rows.append(row)
                st.dataframe(pd.DataFrame(all_rows), use_container_width=True, hide_index=True)
