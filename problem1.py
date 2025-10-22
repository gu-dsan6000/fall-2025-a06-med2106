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
    regexp_extract, col, when, input_file_name,
    to_date, to_timestamp,
    lit, concat_ws, count, rand
)
import pandas as pd
import re
from py4j.java_gateway import java_import
from pyspark.sql.types import StructType, StructField, StringType


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

def get_sub_dirs(spark, s3_path, level = "directory"):

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

    if level == 'directory':
        filtered = [status.getPath().toString() for status in file_status_list if status.isDirectory()]
    elif level == 'file':
        filtered = [status.getPath().toString() for status in file_status_list if status.isFile()]
    elif level == 'all':
        filtered = [status.getPath().toString() for status in file_status_list]
    else:
        raise ValueError("Invalid level argument. Must be 'directory', 'file', or 'all'.")

    return filtered


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
    print(f"Reading {len(data_files)} application directories into a single DataFrame...")

    all_files = []
    for app_dir in data_files:
        container_files = get_sub_dirs(spark, app_dir, level='file')
        filtered_files = [
            f for f in sorted(container_files) if not f.lower().endswith("_000001.log")
        ]
        all_files.extend(filtered_files)

    # Read all files in one go
    log_df = spark.read.text(all_files)

    # make of column for the input file name
    log_df = log_df.withColumn("file_path", input_file_name())

    # Extract application_id and container_id from the path
    log_df = log_df \
                    .withColumn("application_id", regexp_extract("file_path", r"application_(\d+_\d+)", 1)) \
                    .withColumn("container_id", regexp_extract("file_path", r"container_\d+_\d+_(\d+_\d+)", 1))


    total_rows = log_df.count()
    logger.info(f"Successfully loaded {total_rows:,} total rows from {len(data_files)} application")
    print(f"✅ Loaded {total_rows:,} total rows from {len(data_files)} application\n")

    # Step 1: Filter out rows with 'SLF4J'
    logger.info("Step 1: Filtering out rows with 'SLF4J'")
    print("Step 1: Filtering out rows with 'SLF4J'...")
    log_df = log_df.filter(~col("value").startswith("SLF4J"))
    filtered_rows = log_df.count()
    logger.info(f"Filtered dataset to {filtered_rows:,} rows of just logs")
    print(f"✅ Filtered to {filtered_rows:,} rows of just logs\n")

    # Step 2: Derive columns
    logger.info("Step 2: Deriving columns from single main column")
    print("\nStep 2: Deriving columns from single main column...")
    df_parsed = log_df \
                .withColumn("date_str", regexp_extract("value", r"^(\d{2}/\d{2}/\d{2})", 1)) \
                .withColumn("time", regexp_extract("value", r"^\d{2}/\d{2}/\d{2} (\d{2}:\d{2}:\d{2})", 1)) \
                .withColumn("date",when(col("date_str") != "", to_date("date_str", "yy/MM/dd")).otherwise(None)) \
                .withColumn("timestamp",when((col("date").isNotNull()) & (col("time") != ""),
                    to_timestamp(concat_ws(" ", col("date").cast("string"), col("time")),"yyyy-MM-dd HH:mm:ss")).otherwise(None)) \
                .withColumn("log_level", when(regexp_extract("value", r"\b(INFO|WARN|ERROR|DEBUG|TRACE)\b", 1) != "",
                    regexp_extract("value", r"\b(INFO|WARN|ERROR|DEBUG|TRACE)\b", 1)).otherwise(lit("UNK"))) \
                .withColumn("log", regexp_extract("value", r"\b(?:INFO|WARN|ERROR|DEBUG|TRACE)\b\s+(.*)", 1))
        
    log_df = df_parsed.select("date", "timestamp", "log_level", "log", "application_id", "container_id")

    # filter out  empty logs
    log_df = log_df.filter(~col("log_level").startswith("UNK"))

    logger.info("Extracted columns from dataset")
    print("✅ Extracted columns from dataset\n")


    # Step 3: Calculate daily summaries
    logger.info("Step 3: Calculating log level summaries")
    print("Step 3: Calculating log level summaries...")
    log_level_summary = (log_df
        .groupBy("log_level")
        .agg(
            count("*").alias("count"),
        )
    )
    print("\nDisplaying log level summary results...")
    log_level_summary.show(5)

    # Step 4: Display 10 random logs
    logger.info("Step 4: Displaying 10 random logs")
    print("\nStep 4: Displaying 10 random logs...")
    random_rows_df = log_df.orderBy(rand()).limit(10)
    random_rows_df.show(10)

    # Step 5: Convert to Pandas and save
    logger.info("Step 5: Converting Spark DataFrames to Pandas DataFrames")
    print("\nStep 5: Converting to Pandas DataFrames...")
    # pandas_log_df = log_df.toPandas()
    pandas_level_df = log_level_summary.toPandas()
    pandas_sample_df = random_rows_df.toPandas()

    # Step 6: Save to CSV
    output_file_counts = "problem1_counts.csv"
    output_file_sample = "problem1_sample.csv"

    logger.info(f"Step 6: Saving results to {output_file_counts} and {output_file_sample}")
    pandas_level_df.to_csv(output_file_counts, index=False)
    pandas_sample_df.to_csv(output_file_sample, index = False)
    print(f"Step 6: ✅ Results saved to {output_file_counts} and {output_file_sample}")

    # Step 7: Calculating Summary Stats and Saving
    logger.info("Step 7: Calculating Summary Stats")
    print("Step 7: Calculating Summary Stats....")
    print("\n" + "=" * 60)
    print("Summary Statistics")
    print("=" * 60)
    print(f"Total log lines processed: {total_rows}")
    print(f"Total lines with log levels: {filtered_rows}")
    unique_levels = pandas_level_df['log_level'].unique()
    print(f"Unique log levels found: {len(unique_levels)}")
    print("Log level distribution:")
    total_logs = sum(pandas_level_df['count'])
    for i in range(0,len(pandas_level_df)):
        print(f"{pandas_level_df['log_level'][i]}:  {pandas_level_df['count'][i]} ({round((((pandas_level_df['count'][i])/total_logs)*100),2)}%)")

    # Save the summary stats
    output_file_summary = "problem1_summary.txt"
    with open(output_file_summary, "w") as f:
        f.write("============================================================\n")
        f.write("Summary Statistics\n")
        f.write("============================================================\n")
        f.write(f"Total log lines processed: {total_rows}\n")
        f.write(f"Total lines with log levels: {filtered_rows}\n")
        f.write(f"Unique log levels found: {len(unique_levels)}\n")
        f.write("Log level distribution:\n")
        for i in range(0,len(pandas_level_df)):
            f.write(f"{pandas_level_df['log_level'][i]}:  {pandas_level_df['count'][i]} ({round((((pandas_level_df['count'][i])/total_logs)*100),2)}%)\n")

    print(f"\nStep 7: ✅ Results saved to {output_file_summary}\n")


    # Calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Problem 1 execution completed in {execution_time:.2f} seconds")


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

    # Catalogue data from S3
    logger.info(f"Preparing to download data")
    s3_path = f"s3a://med2106-assignment-spark-cluster-logs/data"
    app_directories = get_sub_dirs(spark, s3_path, level='directory')

    if len(app_directories) == 0:
        logger.error("No data files available. Cannot proceed with analysis")
        print("❌ No data files available. Exiting...")
        spark.stop()
        return 1

    # Solve Problem 1
    try:
        logger.info("Starting Problem 1 analysis with s3 bucket")
        solve_problem1(spark, app_directories)
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
    else:
        print("❌ Problem 1 failed - check error messages above")
    print("=" * 70)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())