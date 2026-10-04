import pandas as pd
from pathlib import Path

# ============================================================
# PROJECT FOLDER
# ============================================================

PROJECT_FOLDER = Path(__file__).resolve().parent

CSV_FILE = PROJECT_FOLDER / "mobility_streaming_data.csv"

OUTPUT_FILE = PROJECT_FOLDER / "international_expansion_results.csv"


# ============================================================
# LOAD DATA
# ============================================================

print("=" * 75)
print("REAL-TIME MOBILITY - INTERNATIONAL EXPANSION ANALYSIS")
print("=" * 75)

print("\nProject folder:")
print(PROJECT_FOLDER)

print("\nLooking for:")
print(CSV_FILE)


if not CSV_FILE.exists():

    print("\nERROR: CSV file not found!")

    print("\nExpected file:")
    print(CSV_FILE)

    print("\nPlease run generate_data.py first.")

    exit()


df = pd.read_csv(CSV_FILE)


print("\nData successfully loaded.")

print(f"Total records: {len(df):,}")

print(f"Total columns: {len(df.columns)}")


# ============================================================
# BASIC DATA CLEANING
# ============================================================

df["timestamp"] = pd.to_datetime(
    df["timestamp"]
)

df["cancellation"] = pd.to_numeric(
    df["cancellation"],
    errors="coerce"
)

df["revenue"] = pd.to_numeric(
    df["revenue"],
    errors="coerce"
)

df["demand"] = pd.to_numeric(
    df["demand"],
    errors="coerce"
)

df["available_drivers"] = pd.to_numeric(
    df["available_drivers"],
    errors="coerce"
)

df["supply_coverage_pct"] = pd.to_numeric(
    df["supply_coverage_pct"],
    errors="coerce"
)

df["eta_minutes"] = pd.to_numeric(
    df["eta_minutes"],
    errors="coerce"
)

df["utilization_pct"] = pd.to_numeric(
    df["utilization_pct"],
    errors="coerce"
)

df["customer_rating"] = pd.to_numeric(
    df["customer_rating"],
    errors="coerce"
)


# ============================================================
# MARKET-LEVEL ANALYSIS
# ============================================================

market_analysis = df.groupby(
    ["country", "city"]
).agg(

    total_events=(
        "event_id",
        "count"
    ),

    average_demand=(
        "demand",
        "mean"
    ),

    average_available_drivers=(
        "available_drivers",
        "mean"
    ),

    average_supply_coverage=(
        "supply_coverage_pct",
        "mean"
    ),

    average_demand_supply_gap=(
        "demand_supply_gap",
        "mean"
    ),

    average_utilization=(
        "utilization_pct",
        "mean"
    ),

    average_eta=(
        "eta_minutes",
        "mean"
    ),

    cancellation_rate=(
        "cancellation",
        "mean"
    ),

    average_customer_rating=(
        "customer_rating",
        "mean"
    ),

    total_revenue=(
        "revenue",
        "sum"
    )

).reset_index()


# ============================================================
# CONVERT CANCELLATION RATE TO %
# ============================================================

market_analysis["cancellation_rate_pct"] = (
    market_analysis["cancellation_rate"] * 100
)


# ============================================================
# NORMALIZATION FUNCTION
# ============================================================

def min_max_score(series, higher_is_better=True):

    minimum = series.min()

    maximum = series.max()

    if maximum == minimum:

        return pd.Series(
            [100] * len(series),
            index=series.index
        )

    score = (
        (series - minimum)
        / (maximum - minimum)
    ) * 100

    if not higher_is_better:

        score = 100 - score

    return score


# ============================================================
# BUSINESS SCORES
# ============================================================

# 1. Demand Potential
market_analysis["demand_score"] = min_max_score(
    market_analysis["average_demand"],
    higher_is_better=True
)


# 2. Revenue Potential
market_analysis["revenue_score"] = min_max_score(
    market_analysis["total_revenue"],
    higher_is_better=True
)


# 3. Supply Availability
market_analysis["supply_score"] = min_max_score(
    market_analysis["average_supply_coverage"],
    higher_is_better=True
)


# 4. Customer Experience
# Lower ETA = better
eta_score = min_max_score(
    market_analysis["average_eta"],
    higher_is_better=False
)


# Lower cancellation = better
cancellation_score = min_max_score(
    market_analysis["cancellation_rate_pct"],
    higher_is_better=False
)


# Combine ETA + cancellation
market_analysis["customer_experience_score"] = (
    eta_score * 0.50
    +
    cancellation_score * 0.50
)


# 5. Utilization
market_analysis["utilization_score"] = min_max_score(
    market_analysis["average_utilization"],
    higher_is_better=True
)


# ============================================================
# INTERNATIONAL EXPANSION SCORE
# ============================================================

market_analysis["expansion_score"] = (

    market_analysis["demand_score"] * 0.25

    +

    market_analysis["revenue_score"] * 0.20

    +

    market_analysis["supply_score"] * 0.15

    +

    market_analysis["customer_experience_score"] * 0.15

    +

    market_analysis["utilization_score"] * 0.15

    +

    # Additional operational attractiveness
    min_max_score(
        market_analysis["average_customer_rating"],
        higher_is_better=True
    ) * 0.10

)


market_analysis["expansion_score"] = (
    market_analysis["expansion_score"]
    .round(2)
)


# ============================================================
# RECOMMENDATION
# ============================================================

def get_recommendation(score):

    if score >= 75:

        return "HIGH PRIORITY - EXPAND"

    elif score >= 55:

        return "MEDIUM PRIORITY - PILOT"

    else:

        return "LOW PRIORITY - MONITOR"


market_analysis["recommendation"] = (
    market_analysis["expansion_score"]
    .apply(get_recommendation)
)


# ============================================================
# SORT RESULTS
# ============================================================

market_analysis = market_analysis.sort_values(
    "expansion_score",
    ascending=False
)


# ============================================================
# DISPLAY RESULTS
# ============================================================

print("\n")
print("=" * 75)
print("INTERNATIONAL MARKET RANKING")
print("=" * 75)


display_columns = [

    "country",
    "city",
    "average_demand",
    "average_available_drivers",
    "average_supply_coverage",
    "average_eta",
    "cancellation_rate_pct",
    "total_revenue",
    "average_customer_rating",
    "expansion_score",
    "recommendation"

]


result_display = market_analysis[
    display_columns
].copy()


# Round numerical columns

numeric_columns = result_display.select_dtypes(
    include="number"
).columns

result_display[numeric_columns] = (
    result_display[numeric_columns]
    .round(2)
)


print(
    result_display.to_string(
        index=False
    )
)


# ============================================================
# SAVE RESULTS
# ============================================================

market_analysis.to_csv(
    OUTPUT_FILE,
    index=False
)


# ============================================================
# BEST MARKET
# ============================================================

best_market = market_analysis.iloc[0]


print("\n")
print("=" * 75)
print("RECOMMENDED INTERNATIONAL MARKET")
print("=" * 75)


print(
    f"\nCountry: "
    f"{best_market['country']}"
)


print(
    f"City: "
    f"{best_market['city']}"
)


print(
    f"Expansion Score: "
    f"{best_market['expansion_score']:.2f}"
)


print(
    f"Recommendation: "
    f"{best_market['recommendation']}"
)


print(
    f"Average Demand: "
    f"{best_market['average_demand']:.2f}"
)


print(
    f"Average Supply: "
    f"{best_market['average_available_drivers']:.2f}"
)


print(
    f"Supply Coverage: "
    f"{best_market['average_supply_coverage']:.2f}%"
)


print(
    f"Average ETA: "
    f"{best_market['average_eta']:.2f} minutes"
)


print(
    f"Cancellation Rate: "
    f"{best_market['cancellation_rate_pct']:.2f}%"
)


print(
    f"Total Revenue: "
    f"{best_market['total_revenue']:.2f}"
)


print("\n")
print("=" * 75)

print(
    "Analysis completed successfully."
)

print(
    f"Results saved to:\n{OUTPUT_FILE}"
)

print("=" * 75)