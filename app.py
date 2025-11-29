import os, io
import pandas as pd
import ibm_boto3
from ibm_boto_core.client import Config

# --- COS 接続 ---
cos = ibm_boto3.client(
    "s3",
    ibm_api_key_id=os.environ["COS_APIKEY"],
    ibm_service_instance_id=os.environ["COS_INSTANCE_CRN"],
    config=Config(signature_version="oauth"),
    endpoint_url=os.environ["COS_ENDPOINT"],
)
bucket = os.environ["COS_BUCKET"]

# --- 入出力キー ---
QUOTE_KEY = os.getenv("INPUT_QUOTE_KEY", "inputs/見積データ.csv")
SKU_KEY = os.getenv("MASTER_SKU_KEY", "masters/型番検索表.xlsx")
OUT_FORECAST = os.getenv("OUT_FORECAST_KEY", "outputs/forecast.xlsx")
OUT_NEEDS = os.getenv("OUT_NEEDS_REVIEW_KEY", "outputs/needs_review.xlsx")

def get_obj(key) -> bytes:
    return cos.get_object(Bucket=bucket, Key=key)["Body"].read()

def put_obj(key, data: bytes, content_type="application/octet-stream"):
    cos.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)

def main():
    # 1) 取得
    quote_csv = get_obj(QUOTE_KEY)
    sku_xlsx = get_obj(SKU_KEY)

    df_quote = pd.read_csv(io.BytesIO(quote_csv), encoding="utf-8")
    df_sku = pd.read_excel(io.BytesIO(sku_xlsx), engine="openpyxl")

    # 2) 前処理（列名の正規化など）
    df_quote.columns = [c.strip() for c in df_quote.columns]
    df_sku.columns = [c.strip() for c in df_sku.columns]

    # 3) マスタ突合（例：型番でJOIN）
    #   ※ 実際の列名はPDF手順に合わせて後で調整
    merged = df_quote.merge(df_sku, how="left", on="型番", suffixes=("", "_m"))

    # 4) needs_review の一次推定（LLM前のヒューリスティック）
    needs = merged[merged["必要列名_例"].isna()].copy()  # TODO: 実データ列に置換
    # 5) 予測/集計（仮ロジック）
    forecast = merged.copy()  # TODO: PDF手順のロジックを実装

    # 6) 出力（Excelブック）
    with pd.ExcelWriter("forecast.xlsx", engine="openpyxl") as w:
        forecast.to_excel(w, index=False, sheet_name="forecast")
    with open("forecast.xlsx","rb") as f:
        put_obj(OUT_FORECAST, f.read(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    if not needs.empty:
        with pd.ExcelWriter("needs_review.xlsx", engine="openpyxl") as w:
            needs.to_excel(w, index=False, sheet_name="needs_review")
        with open("needs_review.xlsx","rb") as f:
            put_obj(OUT_NEEDS, f.read(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    print("DONE:", OUT_FORECAST, "NEEDS:", len(needs))

if __name__ == "__main__":
    main()
