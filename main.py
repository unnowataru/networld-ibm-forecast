# -*- coding: utf-8 -*-
import os
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ツール関数と型定義をインポート
from tools.forecast_tool import generate_forecast, GenerateForecastInputs, GenerateForecastResult

# アプリの定義
app = FastAPI(
    title="Forecast Tool API",
    description="IBM Code Engine上で動作するForecast作成ツール",
    version="1.0.0",
)

# ルート（生存確認用）
@app.get("/")
def health_check():
    return {"status": "ok", "message": "Forecast Tool API is running"}

# ツールの実行エンドポイント
@app.post("/generate_forecast", response_model=GenerateForecastResult)
def run_forecast(inputs: GenerateForecastInputs):
    try:
        # ツール関数を直接呼び出す
        result = generate_forecast(inputs)
        return result
    except Exception as e:
        # エラー時は500エラーを返す
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Code Engine は PORT 環境変数(デフォルト8080)で待機する
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)