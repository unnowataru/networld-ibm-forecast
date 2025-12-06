import pandas as pd
import numpy as np


def filter_ibm_manufacturer(quotes_df: pd.DataFrame) -> pd.DataFrame:
    """
    メーカ名が IBM に該当する行だけに絞る。
    業務マニュアルで例示されている表記ゆれをすべて含める想定。

    例:
      - 日本アイ・ビー・エム
      - 日本IBM
      - 日本アイ・ビー・エム株式会社
    """
    df = quotes_df.copy()
    if "メーカ名" not in df.columns:
        raise KeyError("列『メーカ名』が見積データに存在しません。")

    patterns = ["日本アイ・ビー・エム", "日本IBM", "日本アイ・ビー・エム株式会社"]
    mask = df["メーカ名"].isin(patterns)

    # 念のため、表記ゆれで取りこぼしがある場合に備えて contains("IBM") も補助的に追加
    extra_mask = df["メーカ名"].astype(str).str.contains("IBM", na=False)
    df_filtered = df[mask | extra_mask].reset_index(drop=True)

    return df_filtered


def attach_sku(quotes_df: pd.DataFrame) -> pd.DataFrame:
    """
    メーカ型番の先頭7桁を SKU として追加する。
    """
    df = quotes_df.copy()
    if "メーカ型番" not in df.columns:
        raise KeyError("列『メーカ型番』が見積データに存在しません。")

    df["SKU"] = df["メーカ型番"].astype(str).str.slice(0, 7)
    return df


def attach_brand_and_license(
    quotes_df: pd.DataFrame,
    master_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    型番マスタ (PAシート) から ブランド / ライセンス形態 / ライセンスカテゴリー を付与する。
    - JOINキーは SKU（パーツ番号の先頭7桁）とする。
    """
    df = quotes_df.copy()
    master = master_df.copy()

    required_cols = ["パーツ番号", "ブランド", "ライセンス形態"]
    missing = [c for c in required_cols if c not in master.columns]
    if missing:
        raise KeyError(f"型番マスタに必要な列がありません: {missing}")

    # マスタ側にも SKU 列を作成
    master["SKU"] = master["パーツ番号"].astype(str).str.slice(0, 7)

    master_small = master[["SKU", "ブランド", "ライセンス形態"]].drop_duplicates()

    df = df.merge(master_small, on="SKU", how="left")

    # ひとまず ライセンスカテゴリー = ライセンス形態 として扱う
    df["ライセンスカテゴリー"] = df["ライセンス形態"]

    return df


def filter_ibm_software(df: pd.DataFrame) -> pd.DataFrame:
    """
    IBMソフトウェア型番だけに絞り込む。
      - SKU 先頭が D/E/X/Y の行を残す。
    """
    out = df.copy()
    if "SKU" not in out.columns:
        raise KeyError("列『SKU』が存在しません。先に attach_sku() を呼んでください。")

    sku = out["SKU"].astype(str)
    head = sku.str[0]

    is_sw = head.isin(["D", "E", "X", "Y"])
    filtered = out[is_sw].reset_index(drop=True)
    return filtered


def attach_amount_flag(quotes_df: pd.DataFrame) -> pd.DataFrame:
    """
    見積No単位で小計を集計し、200万円UPフラグを明細行に付与する。
    - PDFマニュアルのピボットテーブル + IF 関数のロジックを再現。
    """
    df = quotes_df.copy()

    required_cols = ["見積No", "小計"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"200万円UPフラグ付与に必要な列がありません: {missing}")

    # 見積No単位で小計を合計（IBM見積のHW+SWを合算する前提）
    pivot = df.groupby("見積No", dropna=False)["小計"].sum()

    # 200万円「超」の案件にフラグ（IF(B3>2000000,"★","NG") 相当）
    high_ids = pivot[pivot > 2_000_000].index

    df["200万円UPフラグ"] = df["見積No"].isin(high_ids).map({True: "★", False: "NG"})
    return df


def build_forecast_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    社内向け Forecast シートに近いフォーマットを作成する（詳細版）。

    カラム構成（左から）:
      メーカ名 / 見積作成日 / 顧客名 / 担当営業 / アシスタント名 /
      見積No / 版数 / 件名 / ブランド / メーカ型番 / SKU / ライセンスカテゴリー /
      商品名 / 数量 / 小計 / 見積注意事項 / 納入期日 / 単価 / 原単価 / 粗利額 /
      原価小計 / 粗利小計 / 確度 / 受注予定日 / 受注有無 / エンドユーザー名 /
      時期 / 確度 / 200万円UPかどうかの判断（○/空欄）
    """

    tmp = df.copy()

    # 見積No / 版数 を整数寄りに（.0 を消すため）
    for col in ["見積No", "版数"]:
        if col in tmp.columns:
            tmp[col] = pd.to_numeric(tmp[col], errors="coerce").astype("Int64")

    # 時期: 見積作成日ベースの YYYY-MM
    if "見積作成日" in tmp.columns:
        tmp["時期"] = pd.to_datetime(tmp["見積作成日"], errors="coerce").dt.strftime("%Y-%m")
    else:
        tmp["時期"] = np.nan

    # 2つ目の「確度」用に簡易分類（雑に例示）
    def classify_conf(x):
        if pd.isna(x):
            return ""
        s = str(x)
        if "受注" in s:
            return "High"
        if "概算" in s:
            return "Low"
        return ""

    if "確度" in tmp.columns:
        tmp["確度分類"] = tmp["確度"].apply(classify_conf)
    else:
        tmp["確度分類"] = ""

    # 200万フラグ → ○ / 空欄
    def map_flag(v):
        if v == "★":
            return "○"
        return ""

    if "200万円UPフラグ" in tmp.columns:
        tmp["200万FLAG"] = tmp["200万円UPフラグ"].map(map_flag)
    else:
        tmp["200万FLAG"] = ""

    cols_internal = [
        "メーカ名",
        "見積作成日",
        "顧客名",
        "担当営業",
        "アシスタント名",
        "見積No",
        "版数",
        "件名",
        "ブランド",
        "メーカ型番",
        "SKU",
        "ライセンスカテゴリー",
        "商品名",
        "数量",
        "小計",
        "見積注意事項",
        "納入期日",
        "単価",
        "原単価",
        "粗利額",
        "原価小計",
        "粗利小計",
        "確度",          # 元の確度
        "受注予定日",
        "受注有無",
        "エンドユーザー名",
        "時期",
        "確度分類",      # 2つ目の確度（分類）
        "200万FLAG",
    ]

    missing = [c for c in cols_internal if c not in tmp.columns]
    if missing:
        raise KeyError(f"Forecastテーブル生成に必要な列が足りません: {missing}")

    forecast = tmp[cols_internal].copy()

    # 最終的なヘッダー（業務シートの1行目と同じ並び・名称）
    header = [
        "メーカ名",
        "見積作成日",
        "顧客名",
        "担当営業",
        "アシスタント名",
        "見積No",
        "版数",
        "件名",
        "ブランド",
        "メーカ型番",
        "SKU",
        "ライセンスカテゴリー",
        "商品名",
        "数量",
        "小計",
        "見積注意事項",
        "納入期日",
        "単価",
        "原単価",
        "粗利額",
        "原価小計",
        "粗利小計",
        "確度",
        "受注予定日",
        "受注有無",
        "エンドユーザー名",
        "時期",
        "確度",
        "200万円UPかどうかの判断（実データは200万円以上の案件のみピックアップします）",
    ]

    forecast.columns = header

    return forecast


def build_ibm_vad_forecast(df: pd.DataFrame) -> pd.DataFrame:
    """
    IBM に送付する VAD Forecast 形式（21列）を生成する。

    カラム構成（NW VAD Forecast 20254Q 1125.xlsx と同じ）:
      見積作成日 / 顧客名 / 担当営業 / アシスタント名 / 見積No /
      ブランド / SKU / ライセンスカテゴリ / 商品名 / 数量 / 小計 /
      EU / 案件時期 / 案件確度 / その他コメント / 営業部確認 / PA番号 /
      カテゴリ / チャレンジ / 担当 / PGS
    """
    d = df.copy()

    required_cols = [
        "見積作成日",
        "顧客名",
        "担当営業",
        "アシスタント名",
        "見積No",
        "ブランド",
        "SKU",
        "ライセンスカテゴリー",
        "商品名",
        "数量",
        "小計",
        "エンドユーザー名",
        "200万円UPフラグ",
    ]
    missing = [c for c in required_cols if c not in d.columns]
    if missing:
        raise KeyError(f"VAD Forecast生成に必要な列が足りません: {missing}")

    # 200万円UP案件のみを対象（見積No単位で2,000,000超）
    d = d[d["200万円UPフラグ"] == "★"].copy()

    # EU（エンドユーザー名）
    d["EU_internal"] = d["エンドユーザー名"]

    # まだ持っていない項目はとりあえず空欄で出す
    d["案件時期_internal"] = ""
    d["案件確度_internal"] = ""
    d["その他コメント_internal"] = ""
    d["営業部確認_internal"] = ""
    d["PA番号_internal"] = ""
    d["カテゴリ_internal"] = ""
    d["チャレンジ_internal"] = ""
    d["担当_internal"] = ""
    d["PGS_internal"] = ""

    cols_internal = [
        "見積作成日",
        "顧客名",
        "担当営業",
        "アシスタント名",
        "見積No",
        "ブランド",
        "SKU",
        "ライセンスカテゴリー",
        "商品名",
        "数量",
        "小計",
        "EU_internal",
        "案件時期_internal",
        "案件確度_internal",
        "その他コメント_internal",
        "営業部確認_internal",
        "PA番号_internal",
        "カテゴリ_internal",
        "チャレンジ_internal",
        "担当_internal",
        "PGS_internal",
    ]

    vad_df = d[cols_internal].copy()

    header_vad = [
        "見積作成日",
        "顧客名",
        "担当営業",
        "アシスタント名",
        "見積No",
        "ブランド",
        "SKU",
        "ライセンスカテゴリ",
        "商品名",
        "数量",
        "小計",
        "EU",
        "案件時期",
        "案件確度",
        "その他コメント",
        "営業部確認",
        "PA番号",
        "カテゴリ",
        "チャレンジ",
        "担当",
        "PGS",
    ]

    vad_df.columns = header_vad

    return vad_df
