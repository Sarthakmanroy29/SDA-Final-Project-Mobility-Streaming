import pandas as pd
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

NUM_RECORDS = 10000

markets = {
    "India": ["Delhi", "Mumbai", "Bangalore"],
    "UAE": ["Dubai", "Abu Dhabi"],
    "Singapore": ["Singapore"],
    "Thailand": ["Bangkok"],
    "Indonesia": ["Jakarta"],
    "Malaysia": ["Kuala Lumpur"]
}

zones = [
    "Downtown",
    "Airport",
    "Business District",
    "Residential",
    "Shopping District",
    "University Area"
]

vehicle_types = [
    "Sedan",
    "SUV",
    "Bike",
    "Auto"
]

ride_types = [
    "Economy",
    "Premium",
    "Bike",
    "XL"
]

event_types = [
    "ride_request",
    "ride_accepted",
    "ride_completed",
    "ride_cancelled"
]

devices = [
    "Android",
    "iOS",
    "Web"
]

# ---------------------------------------------------------
# MARKET CHARACTERISTICS
# These values create realistic differences between markets.
# ---------------------------------------------------------

market_profile = {

    "Delhi": {
        "demand_multiplier": 1.20,
        "supply_multiplier": 1.00,
        "fare_multiplier": 1.00,
        "traffic_multiplier": 1.25
    },

    "Mumbai": {
        "demand_multiplier": 1.15,
        "supply_multiplier": 0.95,
        "fare_multiplier": 1.05,
        "traffic_multiplier": 1.30
    },

    "Bangalore": {
        "demand_multiplier": 1.10,
        "supply_multiplier": 0.90,
        "fare_multiplier": 1.05,
        "traffic_multiplier": 1.35
    },

    "Dubai": {
        "demand_multiplier": 1.35,
        "supply_multiplier": 1.15,
        "fare_multiplier": 1.45,
        "traffic_multiplier": 1.05
    },

    "Abu Dhabi": {
        "demand_multiplier": 1.05,
        "supply_multiplier": 1.10,
        "fare_multiplier": 1.40,
        "traffic_multiplier": 0.95
    },

    "Singapore": {
        "demand_multiplier": 1.30,
        "supply_multiplier": 1.05,
        "fare_multiplier": 1.55,
        "traffic_multiplier": 1.00
    },

    "Bangkok": {
        "demand_multiplier": 1.25,
        "supply_multiplier": 0.85,
        "fare_multiplier": 1.20,
        "traffic_multiplier": 1.20
    },

    "Jakarta": {
        "demand_multiplier": 1.40,
        "supply_multiplier": 0.75,
        "fare_multiplier": 1.15,
        "traffic_multiplier": 1.35
    },

    "Kuala Lumpur": {
        "demand_multiplier": 1.10,
        "supply_multiplier": 0.95,
        "fare_multiplier": 1.25,
        "traffic_multiplier": 1.05
    }
}

# ---------------------------------------------------------
# GENERATE DATA
# ---------------------------------------------------------

data = []

start_time = datetime.now() - timedelta(days=30)

city_list = list(market_profile.keys())

for i in range(NUM_RECORDS):

    city = random.choice(city_list)

    profile = market_profile[city]

    country = next(
        country for country, cities in markets.items()
        if city in cities
    )

    timestamp = start_time + timedelta(
        minutes=random.randint(0, 30 * 24 * 60)
    )

    hour = timestamp.hour

    # Peak-hour demand
    if hour in [8, 9, 10, 17, 18, 19, 20]:
        peak_multiplier = 1.6
    elif hour in [12, 13, 14]:
        peak_multiplier = 1.2
    else:
        peak_multiplier = 0.8

    # Demand
    demand = int(
        random.randint(20, 100)
        * profile["demand_multiplier"]
        * peak_multiplier
    )

    # Supply
    supply = int(
        random.randint(25, 110)
        * profile["supply_multiplier"]
    )

    # Demand / Supply
    supply_coverage = min(
        round((supply / demand) * 100, 2),
        150
    )

    # Ride distance
    distance = round(
        random.uniform(1.5, 25),
        2
    )

    # Fare
    fare = round(
        distance
        * random.uniform(25, 60)
        * profile["fare_multiplier"],
        2
    )

    # ETA
    eta = round(
        random.uniform(3, 15)
        * profile["traffic_multiplier"],
        2
    )

    # Cancellation
    cancellation_probability = 0.05

    if supply < demand:
        cancellation_probability += 0.08

    if eta > 10:
        cancellation_probability += 0.05

    cancelled = (
        random.random() < cancellation_probability
    )

    if cancelled:
        event_type = "ride_cancelled"
    else:
        event_type = random.choice([
            "ride_request",
            "ride_accepted",
            "ride_completed"
        ])

    # Customer rating
    rating = round(
        max(
            2.5,
            min(
                5,
                5 - (eta / 15)
                + random.uniform(-0.3, 0.3)
            )
        ),
        2
    )

    # Utilization
    utilization = round(
        min((demand / max(supply, 1)) * 100, 150),
        2
    )

    record = {

        "event_id": str(uuid.uuid4()),

        "timestamp": timestamp.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),

        "country": country,

        "city": city,

        "zone": random.choice(zones),

        "event_type": event_type,

        "vehicle_type": random.choice(vehicle_types),

        "ride_type": random.choice(ride_types),

        "device_type": random.choice(devices),

        "demand": demand,

        "available_drivers": supply,

        "supply_coverage_pct": supply_coverage,

        "demand_supply_gap": demand - supply,

        "utilization_pct": utilization,

        "distance_km": distance,

        "fare": fare,

        "eta_minutes": eta,

        "cancellation": int(cancelled),

        "customer_rating": rating,

        "revenue": fare if event_type == "ride_completed" else 0,

        "peak_hour": int(peak_multiplier > 1),

    }

    data.append(record)


# ---------------------------------------------------------
# CREATE DATAFRAME
# ---------------------------------------------------------

df = pd.DataFrame(data)

# Sort chronologically
df = df.sort_values("timestamp")

# Save CSV
df.to_csv(
    "mobility_streaming_data.csv",
    index=False
)

# Save JSON
df.to_json(
    "mobility_streaming_data.json",
    orient="records",
    lines=True
)

print("=" * 60)
print("DATA GENERATION COMPLETED")
print("=" * 60)

print(f"Total records: {len(df)}")

print("\nCountries:")
print(df["country"].value_counts())

print("\nCities:")
print(df["city"].value_counts())

print("\nEvent Types:")
print(df["event_type"].value_counts())

print("\nAverage Revenue:")
print(round(df["revenue"].mean(), 2))

print("\nAverage ETA:")
print(round(df["eta_minutes"].mean(), 2))

print("\nAverage Supply Coverage:")
print(round(df["supply_coverage_pct"].mean(), 2))

print("\nFiles created:")
print("mobility_streaming_data.csv")
print("mobility_streaming_data.json")