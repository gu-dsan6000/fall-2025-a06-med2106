#!/usr/bin/env python3
"""
Problem 1: Daily summaries of key metrics for NYC TLC data

This script downloads 6 months of NYC TLC data (Jan-June 2021),
combines them into a single DataFrame, and calculates daily summaries.
"""

import os
import subprocess
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, to_date, to_timestamp, col, 
    lit,
    avg, count, max as spark_max, min as spark_min,
    expr, ceil, percentile_approx
)
import pandas as pd
import re
from py4j.java_gateway import java_import
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType


# Configure logging with basicConfig
logging.basicConfig(
    level=logging.INFO,  # Set the log level to INFO
    # Define log message format
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session():
    """Create a Spark session optimized for Problem 1."""

    spark = (
        SparkSession.builder
        .appName("Problem1_DailySummaries")

        # Memory Configuration
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # Performance settings for local execution
        .config("spark.master", "local[*]")  # Use all available cores
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Arrow optimization for Pandas conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .getOrCreate()
    )

    logger.info("Spark session created successfully for Problem 1")
    return spark

def get_application_data(spark):
    s3_path = f"s3a://med2106-assignment-spark-cluster-logs/data"

    # Import necessary Java classes
    java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

    # Import Java classes
    java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    java_import(spark._jvm, 'java.net.URI')

    # Get S3A FileSystem
    uri = spark._jvm.URI(s3_path)
    fs = spark._jvm.FileSystem.get(uri, hadoop_conf)

    # List all files recursively
    path = spark._jvm.Path(s3_path)
    file_status_list = fs.listStatus(path)

    app_dirs = [
        status.getPath().toString()
        for status in file_status_list
        if status.isDirectory()
    ]

    return app_dirs


def solve_problem1(spark, data_files):
    """
    Solve Problem 1: Calculate summaries of key metrics.

    Analyze the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all log files. 
    Log level counts
    10 random sample log entries with their levels
    Summary statistics
    """

    logger.info("Starting Problem 1: Summaries of Key Metrics")
    print("\nSolving Problem 1: Summaries of Key Metrics")
    print("=" * 60)

    start_time = time.time()

    # Read all parquet files into a single DataFrame
    logger.info(f"Reading {len(data_files)} application directories into a single DataFrame")
    print("Reading all log data files into a single DataFrame...")

    # First we need an empty spark data frame to store the logs
    # define the schema
    schema = StructType([
        StructField("log", StringType(), True),
        StructField("application_id", StringType(), True)
    ])
    # define the empty dataframe
    log_df = spark.createDataFrame([], schema)

    # Now for each application directory read all the logs
    for app in data_files:
        current_df = spark.read.text(app)
        print(app)
        match = re.search(r'application_\d+_\d+', app)
        application_id = match.group(0) if match else "unknown"
        application_id = application_id.replace("application_","")
        current_df = current_df.withColumn("application_id", lit(application_id))
        # add container number col

        # and append to log_df
        log_df = log_df.union(current_df)

    total_rows = log_df.count()
    logger.info(f"Successfully loaded {total_rows:,} total rows from {len(data_files)} files")
    print(f"✅ Loaded {total_rows:,} total rows from {len(data_files)} files")

    # # Step 1: Derive columns
    # logger.info("Step 1: Deriving columns from single log columns")
    # print("\nStep 1: Deriving date columns from tpep_pickup_datetime...")
    # df_parsed = log_df \
    #             .withColumn("date", regexp_extract("value", r"^(\d{2}/\d{2}/\d{2})", 1)) \
    #             .withColumn("time", regexp_extract("value", r"^\d{2}/\d{2}/\d{2} (\d{2}:\d{2}:\d{2})", 1)) \
    #             .withColumn("log_level", regexp_extract("value", r"\b(INFO|WARN|ERROR|DEBUG|TRACE)\b", 1)) \
    #             .withColumn("log", regexp_extract("value", r"\b(?:INFO|WARN|ERROR|DEBUG|TRACE)\b\s+(.*)", 1))
    # df_parsed = df_parsed \
    #     .withColumn("date", to_date("date", "yy/MM/dd")) \
    #     .withColumn("timestamp", to_timestamp(col("date").cast("string") + " " + col("time"), "yyyy-MM-dd HH:mm:ss"))
        

    # # Step 2: Filter for year 2021
    # logger.info("Step 2: Filtering data for year 2021")
    # print("Step 2: Filtering data for year 2021...")
    # nyc_tlc_2021 = nyc_tlc.filter(nyc_tlc.dt_year == 2021)
    # filtered_rows = nyc_tlc_2021.count()
    # logger.info(f"Filtered dataset to {filtered_rows:,} rows for year 2021")
    # print(f"✅ Filtered to {filtered_rows:,} rows for year 2021")

    # # Step 3: Calculate daily summaries
    # logger.info("Step 3: Calculating daily summaries with aggregations")
    # print("Step 3: Calculating daily summaries...")
    # daily_averages = (nyc_tlc_2021
    #     .groupBy("dt_year", "dt_month", "dt_day")
    #     .agg(
    #         count("*").alias("num_trips"),
    #         avg("trip_distance").alias("mean_trip_distance"),
    #         spark_max("mta_tax").alias("max_mta_tax"),
    #         expr("percentile_approx(fare_amount, 0.95)").alias("q95_fare_amount"),
    #         spark_min("tip_amount").alias("min_tip_amount"),
    #         ceil(avg("passenger_count")).alias("mean_passenger_count")
    #     )
    # )

    # # Step 4: Sort by date in descending order
    # logger.info("Step 4: Sorting results by date in descending order")
    # print("Step 4: Sorting by date (descending)...")
    # daily_averages = daily_averages.orderBy(
    #     "dt_year", "dt_month", "dt_day",
    #     ascending=[False, False, False]
    # )

    # Display the resulting DataFrame
    logger.info("Step 5: Displaying results")
    print("\nStep 5: Displaying results...")
    print("\nTop 5 daily summaries (sorted in descending order):")
    log_df.show(5)

    # # Get total number of days
    # total_days = daily_averages.count()
    # logger.info(f"Calculated summaries for {total_days} days")
    # print(f"\n✅ Calculated summaries for {total_days} days")

    # # Step 6: Convert to Pandas and save
    # logger.info("Step 6: Converting Spark DataFrame to Pandas DataFrame")
    # print("\nStep 6: Converting to Pandas DataFrame...")
    # pandas_df = daily_averages.toPandas()

    # # Step 7: Save to CSV
    # output_file = "daily_averages.csv"
    # logger.info(f"Step 7: Saving results to {output_file}")
    # pandas_df.to_csv(output_file, index=False)
    # print(f"Step 7: ✅ Results saved to {output_file}")

    # # Calculate execution time
    # end_time = time.time()
    # execution_time = end_time - start_time
    # logger.info(f"Problem 1 execution completed in {execution_time:.2f} seconds")

    # # Print summary statistics
    # print("\n" + "=" * 60)
    # print("PROBLEM 1 COMPLETED - Summary Statistics")
    # print("=" * 60)
    # print(f"Total rows processed: {filtered_rows:,}")
    # print(f"Days with data: {total_days}")
    # print(f"Date range: {pandas_df['dt_month'].min():02d}/{pandas_df['dt_day'].min():02d}/2021 to {pandas_df['dt_month'].max():02d}/{pandas_df['dt_day'].max():02d}/2021")
    # print(f"Total trips: {pandas_df['num_trips'].sum():,}")
    # print(f"Average daily trips: {pandas_df['num_trips'].mean():.0f}")
    # print(f"Execution time: {execution_time:.2f} seconds")

    # # Show sample results
    # print("\n" + "=" * 60)
    # print("Sample Results (first 5 rows):")
    # print("=" * 60)
    # print("Note: Full results saved to daily_averages.csv")

    # # Show first 5 rows with proper formatting
    # sample_df = pandas_df.head(5)
    # for _, row in sample_df.iterrows():
    #     print(f"Year: {int(row['dt_year'])}, Month: {int(row['dt_month']):02d}, Day: {int(row['dt_day']):02d}")
    #     print(f"  Trips: {int(row['num_trips']):,}")
    #     print(f"  Avg Distance: {row['mean_trip_distance']:.2f} miles")
    #     print(f"  Max MTA Tax: ${row['max_mta_tax']:.2f}")
    #     print(f"  95th Percentile Fare: ${row['q95_fare_amount']:.2f}")
    #     print(f"  Min Tip: ${row['min_tip_amount']:.2f}")
    #     print(f"  Avg Passengers: {row['mean_passenger_count']:.0f}")
    #     print("-" * 40)

    # return pandas_df


def main():
    """Main function for Problem 1."""

    logger.info("Starting Problem 1: Summaries of Key Metrics")
    print("=" * 70)
    print("PROBLEM 1: Summaries of Key Metrics")
    print("=" * 70)

    overall_start = time.time()

    # Create Spark session
    logger.info("Initializing Spark session")
    spark = create_spark_session()

    # Download 6 months of data (January to June 2021)
    logger.info(f"Preparing to download data")
    app_directories = get_application_data(spark)

    if len(app_directories) == 0:
        logger.error("No data files available. Cannot proceed with analysis")
        print("❌ No data files available. Exiting...")
        spark.stop()
        return 1

    # Solve Problem 1
    try:
        logger.info("Starting Problem 1 analysis with downloaded data files")
        result_df = solve_problem1(spark, app_directories[0:1])
        success = True
        logger.info("Problem 1 analysis completed successfully")
    except Exception as e:
        logger.exception(f"Error occurred while solving Problem 1: {str(e)}")
        print(f"❌ Error solving Problem 1: {str(e)}")
        success = False

    # Clean up
    logger.info("Stopping Spark session")
    spark.stop()

    # Overall timing
    overall_end = time.time()
    total_time = overall_end - overall_start
    logger.info(f"Total execution time: {total_time:.2f} seconds")

    print("\n" + "=" * 70)
    if success:
        print("✅ PROBLEM 1 COMPLETED SUCCESSFULLY!")
        print(f"\nTotal execution time: {total_time:.2f} seconds")
        # print("\nFiles created:")
        # print("  - daily_averages.csv (Problem 1 solution)")
        # print("\nNext steps:")
        # print("  1. Check daily_averages.csv for the complete results")
        # print("  2. Verify the output matches the expected format")
        # print("  3. Submit daily_averages.csv as part of your solution")
    else:
        print("❌ Problem 1 failed - check error messages above")
    print("=" * 70)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())