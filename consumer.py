import json
import mysql.connector
from mysql.connector import Error
from kafka import KafkaConsumer
from pathlib import Path
from datetime import datetime


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"

KAFKA_TOPIC = "mobility_streaming_events"

KAFKA_GROUP = "mobility-analytics-consumer"


# ============================================================
# MYSQL CONFIGURATION
# ============================================================

MYSQL_HOST = "localhost"

MYSQL_PORT = 3306

MYSQL_USER = "root"

MYSQL_PASSWORD = "root"

MYSQL_DATABASE = "mobility_analytics"


# ============================================================
# DATABASE CONNECTION
# ============================================================

def create_database():

    print("\nConnecting to MySQL...")

    try:

        connection = mysql.connector.connect(

            host=MYSQL_HOST,

            port=MYSQL_PORT,

            user=MYSQL_USER,

            password=MYSQL_PASSWORD

        )

        if connection.is_connected():

            cursor = connection.cursor()

            cursor.execute(
                f"""
                CREATE DATABASE IF NOT EXISTS
                `{MYSQL_DATABASE}`
                """
            )

            print(
                f"Database '{MYSQL_DATABASE}' "
                f"is ready."
            )

            cursor.close()

            connection.close()

            return True

    except Error as e:

        print("\nMySQL connection error:")

        print(e)

        return False


# ============================================================
# CONNECT TO PROJECT DATABASE
# ============================================================

def connect_database():

    try:

        connection = mysql.connector.connect(

            host=MYSQL_HOST,

            port=MYSQL_PORT,

            user=MYSQL_USER,

            password=MYSQL_PASSWORD,

            database=MYSQL_DATABASE

        )

        print("Connected to MySQL successfully.")

        return connection

    except Error as e:

        print("\nUnable to connect to MySQL:")

        print(e)

        return None


# ============================================================
# CREATE TABLES
# ============================================================

def create_tables(connection):

    cursor = connection.cursor()


    # --------------------------------------------------------
    # RAW STREAMING EVENTS
    # --------------------------------------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS mobility_events (

            event_id VARCHAR(100) PRIMARY KEY,

            timestamp DATETIME,

            country VARCHAR(100),

            city VARCHAR(100),

            zone VARCHAR(100),

            event_type VARCHAR(50),

            vehicle_type VARCHAR(50),

            ride_type VARCHAR(50),

            device_type VARCHAR(50),

            demand INT,

            available_drivers INT,

            supply_coverage_pct DECIMAL(10,2),

            demand_supply_gap INT,

            utilization_pct DECIMAL(10,2),

            distance_km DECIMAL(10,2),

            fare DECIMAL(12,2),

            eta_minutes DECIMAL(10,2),

            cancellation INT,

            customer_rating DECIMAL(4,2),

            revenue DECIMAL(12,2),

            peak_hour INT,

            stream_sequence INT,

            stream_source VARCHAR(100),

            processed_at DATETIME

        )
        """
    )


    # --------------------------------------------------------
    # CITY REAL-TIME SUMMARY
    # --------------------------------------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS city_realtime_metrics (

            id INT AUTO_INCREMENT PRIMARY KEY,

            country VARCHAR(100),

            city VARCHAR(100),

            total_events INT,

            total_demand BIGINT,

            average_demand DECIMAL(12,2),

            average_supply DECIMAL(12,2),

            average_supply_coverage DECIMAL(10,2),

            average_demand_supply_gap DECIMAL(12,2),

            average_utilization DECIMAL(10,2),

            average_eta DECIMAL(10,2),

            cancellation_rate DECIMAL(10,2),

            total_revenue DECIMAL(15,2),

            average_rating DECIMAL(4,2),

            last_updated DATETIME,

            UNIQUE KEY city_country (country, city)

        )
        """
    )


    # --------------------------------------------------------
    # EXPANSION ANALYSIS TABLE
    # --------------------------------------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS expansion_market_metrics (

            id INT AUTO_INCREMENT PRIMARY KEY,

            country VARCHAR(100),

            city VARCHAR(100),

            demand_score DECIMAL(10,2),

            revenue_score DECIMAL(10,2),

            supply_score DECIMAL(10,2),

            customer_experience_score DECIMAL(10,2),

            utilization_score DECIMAL(10,2),

            expansion_score DECIMAL(10,2),

            recommendation VARCHAR(100),

            last_updated DATETIME,

            UNIQUE KEY market_country (country, city)

        )
        """
    )


    # --------------------------------------------------------
    # ALERTS TABLE
    # --------------------------------------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS mobility_alerts (

            id INT AUTO_INCREMENT PRIMARY KEY,

            timestamp DATETIME,

            country VARCHAR(100),

            city VARCHAR(100),

            zone VARCHAR(100),

            alert_type VARCHAR(100),

            alert_message TEXT,

            severity VARCHAR(20)

        )
        """
    )


    connection.commit()

    cursor.close()

    print("All MySQL tables are ready.")


# ============================================================
# PROCESS STREAMING EVENT
# ============================================================

def process_event(event, connection):

    cursor = connection.cursor()


    # --------------------------------------------------------
    # READ EVENT VALUES
    # --------------------------------------------------------

    event_id = event.get("event_id")

    timestamp = event.get("timestamp")

    country = event.get("country")

    city = event.get("city")

    zone = event.get("zone")

    event_type = event.get("event_type")

    vehicle_type = event.get("vehicle_type")

    ride_type = event.get("ride_type")

    device_type = event.get("device_type")

    demand = float(
        event.get("demand", 0)
    )

    available_drivers = float(
        event.get("available_drivers", 0)
    )

    supply_coverage = float(
        event.get("supply_coverage_pct", 0)
    )

    demand_supply_gap = float(
        event.get("demand_supply_gap", 0)
    )

    utilization = float(
        event.get("utilization_pct", 0)
    )

    distance = float(
        event.get("distance_km", 0)
    )

    fare = float(
        event.get("fare", 0)
    )

    eta = float(
        event.get("eta_minutes", 0)
    )

    cancellation = int(
        event.get("cancellation", 0)
    )

    rating = float(
        event.get("customer_rating", 0)
    )

    revenue = float(
        event.get("revenue", 0)
    )

    peak_hour = int(
        event.get("peak_hour", 0)
    )

    stream_sequence = int(
        event.get("stream_sequence", 0)
    )

    stream_source = event.get(
        "stream_source",
        "mobility_simulation"
    )


    # --------------------------------------------------------
    # PROCESS TIMESTAMP
    # --------------------------------------------------------

    try:

        event_timestamp = datetime.strptime(
            timestamp,
            "%Y-%m-%d %H:%M:%S"
        )

    except:

        event_timestamp = datetime.now()


    processed_at = datetime.now()


    # --------------------------------------------------------
    # INSERT RAW EVENT
    # --------------------------------------------------------

    insert_event = """
        INSERT IGNORE INTO mobility_events (

            event_id,
            timestamp,
            country,
            city,
            zone,
            event_type,
            vehicle_type,
            ride_type,
            device_type,
            demand,
            available_drivers,
            supply_coverage_pct,
            demand_supply_gap,
            utilization_pct,
            distance_km,
            fare,
            eta_minutes,
            cancellation,
            customer_rating,
            revenue,
            peak_hour,
            stream_sequence,
            stream_source,
            processed_at

        )

        VALUES (

            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s

        )
    """


    values = (

        event_id,
        event_timestamp,
        country,
        city,
        zone,
        event_type,
        vehicle_type,
        ride_type,
        device_type,
        demand,
        available_drivers,
        supply_coverage,
        demand_supply_gap,
        utilization,
        distance,
        fare,
        eta,
        cancellation,
        rating,
        revenue,
        peak_hour,
        stream_sequence,
        stream_source,
        processed_at

    )


    cursor.execute(
        insert_event,
        values
    )


    # ========================================================
    # REAL-TIME CITY METRICS
    # ========================================================

    update_city_metrics = """
        INSERT INTO city_realtime_metrics (

            country,
            city,
            total_events,
            total_demand,
            average_demand,
            average_supply,
            average_supply_coverage,
            average_demand_supply_gap,
            average_utilization,
            average_eta,
            cancellation_rate,
            total_revenue,
            average_rating,
            last_updated

        )

        VALUES (

            %s, %s, 1, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s

        )

        ON DUPLICATE KEY UPDATE

            total_events =
                total_events + 1,

            total_demand =
                total_demand + VALUES(total_demand),

            average_demand =
                (
                    average_demand *
                    (total_events - 1)
                    +
                    VALUES(average_demand)
                )
                /
                total_events,

            average_supply =
                (
                    average_supply *
                    (total_events - 1)
                    +
                    VALUES(average_supply)
                )
                /
                total_events,

            average_supply_coverage =
                (
                    average_supply_coverage *
                    (total_events - 1)
                    +
                    VALUES(average_supply_coverage)
                )
                /
                total_events,

            average_demand_supply_gap =
                (
                    average_demand_supply_gap *
                    (total_events - 1)
                    +
                    VALUES(average_demand_supply_gap)
                )
                /
                total_events,

            average_utilization =
                (
                    average_utilization *
                    (total_events - 1)
                    +
                    VALUES(average_utilization)
                )
                /
                total_events,

            average_eta =
                (
                    average_eta *
                    (total_events - 1)
                    +
                    VALUES(average_eta)
                )
                /
                total_events,

            cancellation_rate =
                (
                    cancellation_rate *
                    (total_events - 1)
                    +
                    VALUES(cancellation_rate)
                )
                /
                total_events,

            total_revenue =
                total_revenue +
                VALUES(total_revenue),

            average_rating =
                (
                    average_rating *
                    (total_events - 1)
                    +
                    VALUES(average_rating)
                )
                /
                total_events,

            last_updated =
                VALUES(last_updated)
    """


    cancellation_percentage = (
        cancellation * 100
    )


    city_values = (

        country,
        city,
        int(demand),
        demand,
        available_drivers,
        supply_coverage,
        demand_supply_gap,
        utilization,
        eta,
        cancellation_percentage,
        revenue,
        rating,
        processed_at

    )


    cursor.execute(
        update_city_metrics,
        city_values
    )


    # ========================================================
    # REAL-TIME ALERTS
    # ========================================================


    # --------------------------------------------------------
    # SUPPLY SHORTAGE
    # --------------------------------------------------------

    if demand_supply_gap > 20:

        message = (
            f"Demand exceeds available supply "
            f"by {demand_supply_gap:.0f} units "
            f"in {city} - {zone}."
        )


        cursor.execute(
            """
            INSERT INTO mobility_alerts (

                timestamp,
                country,
                city,
                zone,
                alert_type,
                alert_message,
                severity

            )

            VALUES (

                %s, %s, %s, %s,
                %s, %s, %s

            )
            """,

            (

                event_timestamp,
                country,
                city,
                zone,
                "SUPPLY_SHORTAGE",
                message,
                "HIGH"

            )
        )


    # --------------------------------------------------------
    # HIGH ETA
    # --------------------------------------------------------

    if eta > 12:

        message = (
            f"Average ETA reached "
            f"{eta:.1f} minutes "
            f"in {city} - {zone}."
        )


        cursor.execute(
            """
            INSERT INTO mobility_alerts (

                timestamp,
                country,
                city,
                zone,
                alert_type,
                alert_message,
                severity

            )

            VALUES (

                %s, %s, %s, %s,
                %s, %s, %s

            )
            """,

            (

                event_timestamp,
                country,
                city,
                zone,
                "HIGH_ETA",
                message,
                "MEDIUM"

            )
        )


    # --------------------------------------------------------
    # HIGH CANCELLATION
    # --------------------------------------------------------

    if cancellation == 1:

        message = (
            f"Ride cancellation detected "
            f"in {city} - {zone}."
        )


        cursor.execute(
            """
            INSERT INTO mobility_alerts (

                timestamp,
                country,
                city,
                zone,
                alert_type,
                alert_message,
                severity

            )

            VALUES (

                %s, %s, %s, %s,
                %s, %s, %s

            )
            """,

            (

                event_timestamp,
                country,
                city,
                zone,
                "RIDE_CANCELLATION",
                message,
                "MEDIUM"

            )
        )


    connection.commit()

    cursor.close()


# ============================================================
# MAIN PROGRAM
# ============================================================

print("\n")
print("=" * 75)
print("REAL-TIME MOBILITY STREAMING CONSUMER")
print("=" * 75)


# ------------------------------------------------------------
# CREATE DATABASE
# ------------------------------------------------------------

if not create_database():

    print(
        "\nCould not create/connect "
        "to MySQL database."
    )

    exit()


# ------------------------------------------------------------
# CONNECT TO DATABASE
# ------------------------------------------------------------

db_connection = connect_database()


if db_connection is None:

    exit()


# ------------------------------------------------------------
# CREATE TABLES
# ------------------------------------------------------------

create_tables(
    db_connection
)


# ------------------------------------------------------------
# CONNECT TO KAFKA
# ------------------------------------------------------------

print("\nConnecting to Kafka...")


try:

    consumer = KafkaConsumer(

        KAFKA_TOPIC,

        bootstrap_servers=[
            KAFKA_SERVER
        ],

        group_id=KAFKA_GROUP,

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        value_deserializer=lambda value:
            json.loads(
                value.decode("utf-8")
            )

    )


    print(
        "Connected to Kafka successfully."
    )


except Exception as e:

    print(
        "\nKafka connection error:"
    )

    print(e)

    db_connection.close()

    exit()


# ============================================================
# START CONSUMING
# ============================================================

print("\n")
print("=" * 75)

print(
    "STREAM PROCESSING STARTED"
)

print(
    f"Kafka Topic: {KAFKA_TOPIC}"
)

print(
    f"Consumer Group: {KAFKA_GROUP}"
)

print("=" * 75)


events_processed = 0


try:

    for message in consumer:

        event = message.value


        process_event(
            event,
            db_connection
        )


        events_processed += 1


        # ----------------------------------------------------
        # DISPLAY EVERY 10 EVENTS
        # ----------------------------------------------------

        if events_processed % 10 == 0:

            print(
                f"Processed events: "
                f"{events_processed:,} | "
                f"Latest: "
                f"{event.get('city')} | "
                f"Demand: "
                f"{event.get('demand')} | "
                f"Supply: "
                f"{event.get('available_drivers')} | "
                f"Gap: "
                f"{event.get('demand_supply_gap')}"
            )


except KeyboardInterrupt:

    print("\n")
    print(
        "Consumer stopped by user."
    )


except Exception as e:

    print(
        "\nConsumer error:"
    )

    print(e)


finally:

    consumer.close()

    db_connection.close()

    print("\n")
    print("=" * 75)

    print(
        f"Total events processed: "
        f"{events_processed:,}"
    )

    print(
        "Kafka consumer closed."
    )

    print(
        "MySQL connection closed."
    )

    print("=" * 75)