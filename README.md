# networld-ibm-forecast

IBM ソフトウェア見積データから forecast を生成するための API / バッチ実装です。  
主な実装は `FastAPI` による API エンドポイント、`forecast_core/` の業務ロジック、`IBM Cloud Object Storage` 連携で構成されています。

## 何をするリポジトリか

- 見積 CSV を読み込む
- 型番マスタ Excel を読み込む
- IBM ソフトウェア向けの forecast テーブルを生成する
- VAD 提出用ファイルと要確認リストを生成する
- ローカル出力または COS 出力で利用できる

## 主な構成

- `main.py`
  - FastAPI アプリ
  - `/` の health check
  - `/generate_forecast` の実行エンドポイント
- `tools/forecast_tool.py`
  - forecast 生成のメイン関数
- `forecast_core/io.py`
  - ローカルファイル入出力
  - IBM Cloud Object Storage 入出力
- `forecast_core/logic.py`
  - フィルタ、SKU 付与、集計などの業務ロジック
- `Dockerfile`
  - Code Engine / コンテナ実行向け定義

## 前提

- Python 3.x
- `requirements.txt` の依存パッケージ
- 必要に応じて `ibm-watsonx-orchestrate[server]` と `uvicorn`
- COS 連携を使う場合は以下の環境変数
  - `COS_ENDPOINT`
  - `COS_HMAC_ACCESS_KEY_ID`
  - `COS_HMAC_SECRET_ACCESS_KEY`
  - `COS_BUCKET`

## ローカルセットアップ

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install "ibm-watsonx-orchestrate[server]" uvicorn
```

## ローカル実行

```powershell
uvicorn main:app --host 0.0.0.0 --port 8080
```

起動後:

- `GET /` : 生存確認
- `POST /generate_forecast` : forecast 生成

## ローカルファイルモード

`forecast_core/io.py` にはローカルファイル用の実装もあります。  
以下のファイルを `data/` 配下に置く前提です。

- `data/見積データ.csv`
- `data/型番検索表250905.xlsx`

出力は `output/` 配下に保存されます。

## Docker / Code Engine

```powershell
docker build -t networld-ibm-forecast .
docker run --rm -p 8080:8080 networld-ibm-forecast
```

本番では Code Engine 側の環境変数で COS 認証情報を与える想定です。

## テスト / 検証

最低限の静的検証:

```powershell
python -m compileall .
```

health check:

```powershell
Invoke-WebRequest http://localhost:8080/
```

## Secrets と運用

- `.env` 実値はコミットしない
- COS 認証情報は Git に置かない
- 共通方針は `C:\dev\work-env` を参照する
