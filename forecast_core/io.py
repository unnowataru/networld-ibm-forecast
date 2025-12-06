from __future__ import annotations

from pathlib import Path
import io
import os

import pandas as pd

# --- ローカルファイル用の基本設定 -----------------------------------------

# プロジェクトルート配下の data / output を想定
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"


def load_quotes() -> pd.DataFrame:
    """
    ローカルの data/見積データ.csv を読み込む。
    - 業務側で作成した「見積データ.csv」を想定
    - 文字コードは Windows-932 (cp932) 前提
    """
    path = DATA_DIR / "見積データ.csv"
    if not path.exists():
        raise FileNotFoundError(f"見積データ.csv が見つかりません: {path}")

    return pd.read_csv(path, encoding="cp932")


def load_part_master() -> pd.DataFrame:
    """
    ローカルの data/型番検索表250905.xlsx (PAシート) を読み込む。
    """
    path = DATA_DIR / "型番検索表250905.xlsx"
    if not path.exists():
        raise FileNotFoundError(f"型番検索表250905.xlsx が見つかりません: {path}")

    return pd.read_excel(path, sheet_name="PA")


def save_forecast(df: pd.DataFrame, filename: str = "forecast.xlsx") -> None:
    """
    output/ 以下に forecast 系の Excel を保存する。
    - filename でファイル名を指定（例: "forecast.xlsx", "vad_forecast.xlsx"）
    """
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / filename
    df.to_excel(path, index=False)


def save_needs_review(df: pd.DataFrame, filename: str = "needs_review.xlsx") -> None:
    """
    output/ 以下に needs_review 用の Excel を保存する。
    """
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / filename
    df.to_excel(path, index=False)


# --- IBM Cloud Object Storage (COS) 用の設定 -----------------------------

try:
    import ibm_boto3
    from ibm_botocore.client import Config as IBMConfig

    _HAS_COS = True
except ImportError:
    ibm_boto3 = None  # type: ignore[assignment]
    IBMConfig = None  # type: ignore[assignment]
    _HAS_COS = False


# クラウド環境（Code Engine）向け設定
# 環境変数から読み込みます。設定がない場合は None となります。
COS_CONFIG = {
    "ENDPOINT": os.getenv("COS_ENDPOINT"),
    "API_KEY_ID": os.getenv("COS_HMAC_ACCESS_KEY_ID"),
    "SECRET_ACCESS_KEY": os.getenv("COS_HMAC_SECRET_ACCESS_KEY"),
    "BUCKET_DEFAULT": os.getenv("COS_BUCKET", "bucket-networld-forecast-01")
}


def get_cos_client():
    """
    IBM Cloud Object Storage のクライアントを作成する。
    環境変数 (COS_ENDPOINT 等) が設定されていない場合はエラーとします。
    """
    if not _HAS_COS:
        raise RuntimeError(
            "ibm-cos-sdk がインストールされていません。"
            "COS 連携を使う場合は `pip install ibm-cos-sdk` を実行してください。"
        )

    # 必須変数のチェック
    if not COS_CONFIG["ENDPOINT"] or not COS_CONFIG["API_KEY_ID"] or not COS_CONFIG["SECRET_ACCESS_KEY"]:
        raise RuntimeError(
            "COS の認証情報が環境変数に設定されていません。"
            "クラウド環境(Code Engine等)の環境変数設定で "
            "COS_ENDPOINT, COS_HMAC_ACCESS_KEY_ID, COS_HMAC_SECRET_ACCESS_KEY を指定してください。"
        )

    return ibm_boto3.client(
        service_name="s3",
        aws_access_key_id=COS_CONFIG["API_KEY_ID"],
        aws_secret_access_key=COS_CONFIG["SECRET_ACCESS_KEY"],
        endpoint_url=COS_CONFIG["ENDPOINT"],
        config=IBMConfig(signature_version="s3v4"),
    )


def load_quotes_from_cos(
    key: str,
    bucket: str | None = None,
    encoding: str = "cp932",
) -> pd.DataFrame:
    """
    COS 上の CSV を 1 つ読み込んで、見積データとして扱う。
    """
    cos = get_cos_client()
    bucket_name = bucket or COS_CONFIG["BUCKET_DEFAULT"]

    obj = cos.get_object(Bucket=bucket_name, Key=key)
    raw = obj["Body"].read()
    buf = io.BytesIO(raw)

    return pd.read_csv(buf, encoding=encoding)


def load_part_master_from_cos(
    key: str,
    bucket: str | None = None,
    sheet_name: str = "PA",
) -> pd.DataFrame:
    """
    COS 上の 型番マスタExcel を読み込む。
    """
    cos = get_cos_client()
    bucket_name = bucket or COS_CONFIG["BUCKET_DEFAULT"]

    obj = cos.get_object(Bucket=bucket_name, Key=key)
    raw = obj["Body"].read()
    buf = io.BytesIO(raw)

    return pd.read_excel(buf, sheet_name=sheet_name)


def save_forecast_to_cos(
    df: pd.DataFrame,
    key: str,
    bucket: str | None = None,
) -> None:
    """
    DataFrame を Excel 形式にして COS に保存する。
    """
    cos = get_cos_client()
    bucket_name = bucket or COS_CONFIG["BUCKET_DEFAULT"]

    # DataFrame → Excelバイナリ（メモリ上）
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)

    cos.put_object(Bucket=bucket_name, Key=key, Body=buf.getvalue())