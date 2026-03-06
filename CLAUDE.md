# CLAUDE.md

## 目的

このリポジトリで Claude を呼ぶときの案件固有レビュー観点を定義する。

## 期待役割

- forecast ロジックの責務整理
- API と tools の境界確認
- Docker / FastAPI 実行前提のレビュー

## 調査系との境界

- X 上の反応調査は `x-search` を優先する
- Claude は調査結果を構成やレビュー観点へ接続する

## 確認すべきこと

- `work-env` との整合性
- `.env` と secrets の扱い
- API 入出力と運用 docs の整合
