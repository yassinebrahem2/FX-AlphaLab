"""One-shot script: process existing Bronze GDELT JSONL → Silver Parquet."""

from pathlib import Path

from src.ingestion.preprocessors.gdelt_preprocessor import GDELTPreprocessor

input_dir = Path("data/raw/news/gdelt")
output_dir = Path("data/processed/sentiment")

pp = GDELTPreprocessor(input_dir=input_dir, output_dir=output_dir)
df = pp.preprocess()

print(f"Total rows   : {len(df)}")
print(f"Unique articles: {df['article_id'].nunique()}")
print(f"Pairs        : {sorted(df['pair'].unique())}")
print(f"Labels dist  : {df['sentiment_label'].value_counts().to_dict()}")
print()
print(
    df[["timestamp_utc", "pair", "sentiment_score", "sentiment_label", "url"]].head(6).to_string()
)

paths = pp.export_partitioned(df)

print()
print("Silver partitions written:")
for key, path in sorted(paths.items()):
    print(f"  {key}  ->  {path}")
