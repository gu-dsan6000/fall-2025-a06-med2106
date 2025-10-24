#!/usr/bin/env python3
"""
Problem 2: Time Series Analysis
"""

import os
import subprocess
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, when, input_file_name,
    to_date, to_timestamp, min, max, timestamp_diff,
    lit, concat_ws, count, rand
)
import pandas as pd
import re
from py4j.java_gateway import java_import
from pyspark.sql.types import StructType, StructField, StringType
import seaborn as sns
import matplotlib.pyplot as plt


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
    Solve Problem 2: Application and Container Time Series Analysis

    Extract cluster IDs, application IDs, and application start/end times
    Finds unique clusters are in the dataset
    Find number of applications ran on each cluster
    Find which clusters are most heavily used
    Analyse the  timeline of application execution across clusters
    """

    logger.info("Starting Problem 2: Application and Container Time Series Analysis")
    print("\nSolving Problem 2: Time Series Analysis")
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
                    .withColumn('cluster_id', regexp_extract('file_path', r"application_(\d+)_\d+", 1)) \
                    .withColumn("application_id", regexp_extract("file_path", r"application_(\d+_\d+)", 1)) \
                    .withColumn("app_number", regexp_extract("file_path", r"application_\d+_(\d+)", 1)) \
                    .withColumn("container_id", regexp_extract("file_path", r"container_\d+_\d+_\d+_(\d+)", 1))


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
        
    log_df = df_parsed.select("date", "timestamp", "log_level", "log", "cluster_id","application_id", "app_number", "container_id")

    # filter out  empty logs
    log_df = log_df.filter(~col("log_level").startswith("UNK"))

    logger.info("Extracted columns from dataset")
    print("✅ Extracted columns from dataset\n")


    # Step 3: Calculate application summaries
    logger.info("Step 3: Calculating application summaries")
    print("Step 3: Calculating application summaries...")
    app_summary = (log_df
        .groupBy("cluster_id","application_id")
        .agg(
            min("timestamp").alias("start_time"),
            max("timestamp").alias("end_time"),
        )
    )
    # add duration column
    app_summary = app_summary.withColumn("duration_seconds", timestamp_diff("SECOND", col("start_time"), col("end_time")))

    print("\nDisplaying application summary results...")
    app_summary.show(5)

    # Step 4: Calculate cluster summaries
    logger.info("Step 34 Calculating cluster summaries")
    print("Step 4: Calculating cluster summaries...")
    cluster_summary = (app_summary
        .groupBy("cluster_id")
        .agg(
            count("*").alias("num_applications"),
            min("start_time").alias("cluster_first_app"),
            max("end_time").alias("cluster_last_app"),
        )
    )
    # Sort by application numbers descending order
    logger.info("Sorting results by number of applications in descending order")
    print("\nStep 4: Sorting by number of applications (descending)...")
    cluster_summary = cluster_summary.orderBy(
        "num_applications",
        ascending=[False]
    )
    print("\nDisplaying cluster results...")
    cluster_summary.show(5)

    # Step 5: Convert to Pandas and save
    logger.info("Step 5: Converting Spark DataFrames to Pandas DataFrames")
    print("\nStep 5: Converting to Pandas DataFrames...")
    # pandas_log_df = log_df.toPandas()
    pandas_app_df = app_summary.toPandas()
    pandas_cluster_df = cluster_summary.toPandas()
    logger.info("Converted to Pandas DataFrames")
    print(f"Step 5: ✅ Converted to Pandas DataFrames\n")

    # Step 6: Save to CSV
    output_file_app = "problem2_timeline.csv"
    output_file_cluster = "problem2_cluster_summary.csv"

    logger.info(f"Step 6: Saving results to {output_file_app} and {output_file_cluster}")
    pandas_app_df.to_csv(output_file_app, index=False)
    pandas_cluster_df.to_csv(output_file_cluster, index = False)
    print(f"Step 6: ✅ Results saved to {output_file_app} and {output_file_cluster}\n")

    # Step 7: Calculating Summary Stats and Saving
    logger.info("Step 7: Calculating Summary Stats")
    print("Step 7: Calculating Summary Stats....")
    print("\n" + "=" * 60)
    print("Summary Statistics")
    print("=" * 60)
    unique_clusters = len(pandas_cluster_df['cluster_id'].unique())
    print(f"Total unique clusters: {unique_clusters}")
    total_apps = sum(pandas_cluster_df['num_applications'])
    print(f"Total applications: {total_apps}")
    print(f"Average applications per cluster: {round(total_apps/unique_clusters,2)}")
    print("Most heavily used clusters:")
    for i in range(0,3):
        print(f"Cluster {pandas_cluster_df['cluster_id'][i]}:  {pandas_cluster_df['num_applications'][i]} applications")

    # Save the summary stats
    output_file_summary = "problem2_stats.txt"
    with open(output_file_summary, "w") as f:
        f.write("============================================================\n")
        f.write("Summary Statistics\n")
        f.write("============================================================\n")
        f.write(f"Total unique clusters: {unique_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {round(total_apps/unique_clusters,2)}\n")
        f.write("Most heavily used clusters:\n")
        for i in range(0,3):
            f.write(f"Cluster {pandas_cluster_df['cluster_id'][i]}:  {pandas_cluster_df['num_applications'][i]} applications\n")

    print(f"\nStep 7: ✅ Results saved to {output_file_summary}\n")

    # Step 8: Generating Bar Plot
    logger.info("Step 8: Generating Bar Plot")
    print("Step 8: Generating Bar Plot....")
    bar_output_file = "problem2_bar_chart.png"

    # Bar Plot
    plt.figure(figsize=(8,9))
    bar_plot = sns.barplot(pandas_cluster_df, x="cluster_id", y="num_applications", hue="cluster_id")
    bar_plot.bar_label(bar_plot.containers[0], fontsize=10)
    plt.title('Number of Applications per Cluster')
    plt.xlabel('Cluster ID')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Number of Applications')
    plt.tight_layout()
    plt.savefig(bar_output_file)

    print(f"\nStep 8: ✅ Results saved to {bar_output_file}\n")

    # Step 9: Largest Cluster Analysis and Visualization
    logger.info("Step 9: Largest Cluster Analysis and Visualization")
    print("Filtering Pandas Dataframe for largest cluster....")
    largest_cluster_id = pandas_cluster_df['cluster_id'][0]
    largest_cluster_data = pandas_app_df[pandas_app_df['cluster_id'] == largest_cluster_id]

    density_output_file = "problem2_density_plot.png"

    # Density Plot
    plt.figure(figsize=(8,9))
    den_plot = sns.histplot(largest_cluster_data, x = "duration_seconds", kde = True)
    plt.title(f'Job Duration from Largest Cluster - {len(largest_cluster_data)} samples')
    plt.xscale('log')
    plt.ylabel('Count')
    plt.xticks(rotation=45, ha='right')
    plt.xlabel('Job Duration (seconds)')
    plt.tight_layout()
    plt.savefig(density_output_file)

    print(f"\nStep 9: ✅ Results saved to {density_output_file}\n")

    # Calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Problem 2 execution completed in {execution_time:.2f} seconds")


def main():
    """Main function for Problem 2."""

    logger.info("Starting Problem 2: Times Series Analysis")
    print("=" * 70)
    print("PROBLEM 2: Times Series Analysis")
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

    # Solve Problem 2
    try:
        logger.info("Starting Problem 2 analysis with s3 bucket")
        solve_problem1(spark, app_directories[4:16])
        success = True
        logger.info("Problem 2 analysis completed successfully")
    except Exception as e:
        logger.exception(f"Error occurred while solving Problem 2: {str(e)}")
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
        print("✅ PROBLEM 2 COMPLETED SUCCESSFULLY!")
        print(f"\nTotal execution time: {total_time:.2f} seconds")
    else:
        print("❌ Problem 2 failed - check error messages above")
    print("=" * 70)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())