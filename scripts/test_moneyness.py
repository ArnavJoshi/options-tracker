from data.yfinance_options import get_top_sp500_options

df = get_top_sp500_options(
    ["AAPL", "NVDA", "TSLA", "MSFT"],
    top_n=20, max_expiries=2, min_volume=100,
    atm_pct=0.01,
    sort_by=["volume", "open_interest"], max_workers=4,
)
print("cols:", list(df.columns))
print("moneyness counts:")
print(df["moneyness"].value_counts().to_string())
print()
print(df[["symbol", "type", "moneyness", "strike", "underlying", "volume", "openInterest"]].to_string(index=False))
print()

df2 = get_top_sp500_options(
    ["AAPL", "NVDA", "TSLA", "MSFT"],
    top_n=10, max_expiries=2, min_volume=100,
    moneyness=["ITM"], atm_pct=0.01,
    sort_by=["volume", "open_interest"], max_workers=4,
)
print("ITM-only rows:", len(df2), "unique:", df2["moneyness"].unique().tolist())

df3 = get_top_sp500_options(
    ["AAPL", "NVDA", "TSLA", "MSFT"],
    top_n=10, max_expiries=2, min_volume=50,
    moneyness=["ATM"], atm_pct=0.02,
    sort_by=["volume"], max_workers=4,
)
print("ATM-only rows:", len(df3), "unique:", df3["moneyness"].unique().tolist())
print(df3[["symbol", "type", "moneyness", "strike", "underlying"]].to_string(index=False))

