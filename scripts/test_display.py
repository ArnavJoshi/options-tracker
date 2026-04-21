"""Smoke-test: news-row merge when 3+ consecutive same-symbol rows appear."""
import pandas as pd

df = pd.DataFrame([
    {"symbol": "AAPL", "top_news": "News-A"},
    {"symbol": "AAPL", "top_news": "News-A"},  # run of 4 AAPL
    {"symbol": "AAPL", "top_news": "News-A"},
    {"symbol": "AAPL", "top_news": "News-A"},
    {"symbol": "NVDA", "top_news": "News-N"},
    {"symbol": "NVDA", "top_news": "News-N"},  # run of 2 NVDA (should stay)
    {"symbol": "MSFT", "top_news": "News-M"},
    {"symbol": "TSLA", "top_news": "News-T"},
    {"symbol": "TSLA", "top_news": "News-T"},
    {"symbol": "TSLA", "top_news": "News-T"},  # run of 3 TSLA
])

run_id = (df["symbol"] != df["symbol"].shift()).cumsum()
run_size = df.groupby(run_id)["symbol"].transform("size")
is_not_first = df["symbol"] == df["symbol"].shift()
df.loc[is_not_first & (run_size > 2), "top_news"] = ""

print(df.to_string(index=False))
print()
print("AAPL non-empty news rows:", (df[df.symbol=='AAPL'].top_news != '').sum(), "(expected 1)")
print("NVDA non-empty news rows:", (df[df.symbol=='NVDA'].top_news != '').sum(), "(expected 2)")
print("TSLA non-empty news rows:", (df[df.symbol=='TSLA'].top_news != '').sum(), "(expected 1)")

# Styler sanity check
def _style(v):
    key = str(v).split(" ")[-1]
    bg = {"ITM": "#1b5e20", "ATM": "#8d6e00", "OTM": "#b71c1c"}.get(key, "")
    return f"background-color: {bg}; color: white;" if bg else ""

sdf = pd.DataFrame({"moneyness": ["🟢 ITM", "🟡 ATM", "🔴 OTM"]})
html = sdf.style.map(_style, subset=["moneyness"]).to_html()
print("styler html len:", len(html), "contains ITM bg:", "#1b5e20" in html,
      "ATM bg:", "#8d6e00" in html, "OTM bg:", "#b71c1c" in html)

