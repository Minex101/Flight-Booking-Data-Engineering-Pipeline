from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

rules = {
    "rule_1" : "booking_id IS NOT NULL",
    "rule_2" : "passenger_id IS NOT NULL"
}

# ----------------------------------------------------------------------------------------
# -- Bookings Table--
@dp.table(name="stage_bookings")
def stage_bookings():
    df = spark.readStream.format("delta") \
                .load("/Volumes/databricksete/bronze/bronzevolume/bookings/data/")
    return df

@dp.view(name="transform_bookings")
def transform_bookings():
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType())) \
           .withColumn("modifiedDate", current_timestamp()) \
           .withColumn("booking_date", to_date(col("booking_date"))) \
           .drop("_rescued_data")
    return df

@dp.table(name="silver_bookings")
@dp.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("transform_bookings")
    return df

# ----------------------------------------------------------------------------------------
# -- Flights Table --
@dp.table(name="trans_flights")
def trans_flights():
    df = spark.readStream.format("delta") \
                .load("/Volumes/databricksete/bronze/bronzevolume/flights/data/")
    df = df.withColumn("modifiedDate", current_timestamp()) \
           .drop("_rescued_data")
    return df

dp.create_streaming_table("silver_flights")

dp.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)

# ----------------------------------------------------------------------------------------
# -- Customers Table --
@dp.table(name="trans_passengers")
def trans_passengers():
    df = spark.readStream.format("delta") \
                .load("/Volumes/databricksete/bronze/bronzevolume/customers/data/")
    df = df.withColumn("modifiedDate", current_timestamp()) \
           .drop("_rescued_data")
    return df

dp.create_streaming_table("silver_passengers")

dp.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)

# ----------------------------------------------------------------------------------------
# -- Airports Data --
@dp.table(name="trans_airports")
def trans_airports():
    df = spark.readStream.format("delta") \
                .load("/Volumes/databricksete/bronze/bronzevolume/airports/data/")
    df = df.withColumn("modifiedDate", current_timestamp()) \
           .drop("_rescued_data")
    return df

dp.create_streaming_table("silver_airports")

dp.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)