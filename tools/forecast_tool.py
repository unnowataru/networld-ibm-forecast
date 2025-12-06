# tools/forecast_tool.py
# 1行目にあった from __future__ ... は削除しました

from pydantic import BaseModel, Field
# 正しいインポートパス
from ibm_watsonx_orchestrate.agent_builder.tools import tool

from forecast_core.io import (
    load_quotes_from_cos,
    load_part_master_from_cos,
    save_forecast_to_cos,
)
from forecast_core.logic import (
    filter_ibm_manufacturer,
    attach_sku,
    attach_brand_and_license,
    filter_ibm_software,
    attach_amount_flag,
    build_forecast_table,
    build_ibm_vad_forecast,
)

# --- 1. 入力の定義 ---
class GenerateForecastInputs(BaseModel):
    quotes_key: str = Field(
        default="inputs/quotes.csv",
        description="COS上にある見積データのCSVファイルパス"
    )
    part_master_key: str = Field(
        default="inputs/master.xlsx",
        description="COS上にある型番マスタのExcelファイルパス"
    )
    output_prefix: str = Field(
        default="outputs/",
        description="結果を出力するCOS上のフォルダパス"
    )

# --- 2. 出力の定義 ---
class GenerateForecastResult(BaseModel):
    """generate_forecast の戻り値"""
    quotes_key: str
    part_master_key: str
    forecast_key: str
    vad_forecast_key: str
    needs_review_key: str
    rows_total: int
    rows_vad: int
    rows_needs_review: int

# エラー回避のための念押し（定義を確定させる）
GenerateForecastResult.model_rebuild()

# --- 3. ツール本体 ---
@tool(
    name="generate_forecast",
    description="IBMソフトウェアの見積データから、Forecast(予測)テーブル、VAD提出用ファイル、要確認リストを作成し、COSに保存します。"
)
def generate_forecast(inputs: GenerateForecastInputs) -> GenerateForecastResult:
    """
    COS 上の CSV / Excel を読み込んで Forecast を生成し、
    COS に forecast / vad_forecast / needs_review を保存するメイン関数。
    """
    
    # Pydanticモデルから値を取り出す
    quotes_key = inputs.quotes_key
    part_master_key = inputs.part_master_key
    output_prefix = inputs.output_prefix

    # 1. COS から入力ファイルを読み込み
    quotes_df = load_quotes_from_cos(quotes_key)
    master_df = load_part_master_from_cos(part_master_key)

    # 2. 業務ロジック適用
    ibm_df = filter_ibm_manufacturer(quotes_df)
    ibm_df = attach_amount_flag(ibm_df)

    df = attach_sku(ibm_df)
    df = attach_brand_and_license(df, master_df)
    df = filter_ibm_software(df)

    forecast_df = build_forecast_table(df)
    vad_df = build_ibm_vad_forecast(df)
    needs_review_df = df[df["ブランド"].isna() | df["ライセンスカテゴリー"].isna()]

    # 3. COS に保存するキーを決定
    if not output_prefix.endswith("/"):
        output_prefix = output_prefix + "/"

    forecast_key = output_prefix + "forecast.xlsx"
    vad_key = output_prefix + "vad_forecast.xlsx"
    needs_review_key = output_prefix + "needs_review.xlsx"

    # 4. COS に保存
    save_forecast_to_cos(forecast_df, key=forecast_key)
    save_forecast_to_cos(vad_df, key=vad_key)
    save_forecast_to_cos(needs_review_df, key=needs_review_key)

    # 5. 結果を返す
    return GenerateForecastResult(
        quotes_key=quotes_key,
        part_master_key=part_master_key,
        forecast_key=forecast_key,
        vad_forecast_key=vad_key,
        needs_review_key=needs_review_key,
        rows_total=int(len(df)),
        rows_vad=int(len(vad_df)),
        rows_needs_review=int(len(needs_review_df))
    )