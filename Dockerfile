# ベースイメージ（軽量なPython 3.12）
FROM python:3.12-slim

# 作業ディレクトリ設定
WORKDIR /app

# 必要なパッケージをインストール
# (gitなどは不要ですが、念のため軽量化意識)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# ライブラリのインストール
# まず requirements.txt だけコピーしてキャッシュを効かせる
COPY requirements.txt .
# ADKのサーバー機能(FastAPI/Uvicorn)に必要なものも追加
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir "ibm-watsonx-orchestrate[server]" uvicorn

# ソースコードをコピー
# pkg フォルダの中身を /app にコピーします
COPY . .

# 環境変数のデフォルト（本番はCode Engineの画面で上書きする）
ENV PYTHONUNBUFFERED=1

# 起動コマンド
CMD ["python", "main.py"]