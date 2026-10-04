from kafka import KafkaProducer
import pandas as pd
import json
import time
from pathlib import Path


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_FOLDER = Path(__file__).resolve().parent

CSV_FILE = PROJECT_FOLDER / "mobility_streaming_data.csv"


# Kafka configuration

KAFKA_SERVER = "localhost:9092"

TOPIC_NAME = "mobility_streaming_events"


# ============================================================
# STREAMING SPEED
# ============================================================

# 0.2 seconds = 5 events/second
STREAM_DELAY = 0.2


# ============================================================
# LOAD DATA
# ============================================================

print("=" * 75)
print("REAL-TIME MOBILITY KAFKA PRODUCER")
print("=" * 75)


print("\nProject folder:")
print(PROJECT_FOLDER)


print("\nLooking for data:")
print(CSV_FILE)


if not CSV_FILE.exists():

    print("\nERROR: mobility_streaming_data.csv not found!")

    print("\nPlease run generate_data.py first.")

    exit()


df = pd.read_csv(CSV_FILE)


print(
    f"\nSuccessfully loaded "
    f"{len(df):,} records."
)


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

print("\nConnecting to Kafka...")


try:

    producer = KafkaProducer(

        bootstrap_servers=[
            KAFKA_SERVER
        ],

        value_serializer=lambda value:
            json.dumps(value).encode("utf-8"),

        acks="all",

        retries=5,

        linger_ms=5

    )


    # Force connection test

    producer.bootstrap_connected()

    print("Kafka connection successful!")


except Exception as e:

    print("\nERROR: Could not connect to Kafka.")

    print("\nMake sure Kafka is running.")

    print("\nKafka server:")
    print(KAFKA_SERVER)

    print("\nError:")
    print(e)

    exit()


# ============================================================
# START STREAMING
# ============================================================

print("\n")
print("=" * 75)

print(
    f"Starting real-time streaming..."
)

print(
    f"Kafka Topic: {TOPIC_NAME}"
)

print(
    f"Streaming Speed: "
    f"{1 / STREAM_DELAY:.1f} events/second"
)

print("=" * 75)


events_sent = 0


try:

    for index, row in df.iterrows():

        # Convert Pandas row to dictionary

        event = row.to_dict()


        # Convert NaN values to None

        event = {

            key: (
                None
                if pd.isna(value)
                else value
            )

            for key, value in event.items()

        }


        # Add streaming metadata

        event["stream_sequence"] = index + 1

        event["stream_source"] = (
            "mobility_simulation"
        )


        # Send event to Kafka

        future = producer.send(

            TOPIC_NAME,

            value=event

        )


        events_sent += 1


        # Print every 100 events

        if events_sent % 100 == 0:

            producer.flush()

            print(
                f"Events streamed: "
                f"{events_sent:,} / "
                f"{len(df):,}"
            )


        # Simulate real-time arrival

        time.sleep(
            STREAM_DELAY
        )


    # Make sure all messages are delivered

    producer.flush()


    print("\n")
    print("=" * 75)

    print(
        "STREAMING COMPLETED SUCCESSFULLY"
    )

    print(
        f"Total events sent: "
        f"{events_sent:,}"
    )

    print(
        f"Kafka topic: "
        f"{TOPIC_NAME}"
    )

    print("=" * 75)


except KeyboardInterrupt:

    print("\n")
    print("=" * 75)

    print(
        "STREAMING STOPPED BY USER"
    )

    print(
        f"Events sent: "
        f"{events_sent:,}"
    )

    print("=" * 75)


except Exception as e:

    print("\n")
    print("ERROR DURING STREAMING:")

    print(e)


finally:

    producer.close()

    print(
        "\nKafka producer closed."
    )