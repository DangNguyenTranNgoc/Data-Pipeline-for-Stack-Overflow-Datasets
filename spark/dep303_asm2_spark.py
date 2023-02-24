#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MONGODB_CLOUD_URI = "mongodb://admin:password@mongo:27017/stackoverflow?authSource=admin"
MONGODB_DATABASE = "stackoverflow"
DATA_DIR = "/usr/local/share/data/"
OUTPUT_DIR = "output"

if __name__ == "__main__":
    # Init Spark session
    spark = SparkSession.builder \
                    .master("local") \
                    .appName("dep303_asm2") \
                    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
                    .config("spark.mongodb.read.connection.uri", MONGODB_CLOUD_URI) \
                    .config("spark.mongodb.write.connection.uri", MONGODB_CLOUD_URI) \
                    .getOrCreate()

    sc = spark.sparkContext
    # Read data from MongoDB
    questions_df = spark.read \
                    .format("mongodb") \
                    .option("uri", MONGODB_CLOUD_URI) \
                    .option("collection", "questions") \
                    .load()

    answers_df = spark.read \
                    .format("mongodb") \
                    .option("uri", MONGODB_CLOUD_URI) \
                    .option("collection", "answers") \
                    .load()

    # Convert data types
    questions_df = questions_df.withColumn("OwnerUserId", F.col("OwnerUserId").cast("integer"))
    questions_df = questions_df.withColumn("CreationDate", F.to_date("CreationDate"))
    questions_df = questions_df.withColumn("ClosedDate", F.to_date("ClosedDate"))

    answers_df = answers_df.withColumn("OwnerUserId", F.col("OwnerUserId").cast("integer"))
    answers_df = answers_df.withColumn("ParentId", F.col("ParentId").cast("integer"))
    answers_df = answers_df.withColumn("CreationDate", F.to_date("CreationDate"))

    # Join data
    spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    spark.sql("USE MY_DB")

    questions_df.write \
                .bucketBy(10, "Id") \
                .format("parquet") \
                .mode("overwrite") \
                .option("path", "{}/tmp_questions".format(DATA_DIR)) \
                .saveAsTable("MY_DB.questions")


    answers_df.write \
            .bucketBy(10, "ParentId") \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", "{}/tmp_answers".format(DATA_DIR)) \
            .saveAsTable("MY_DB.answers")

    tmp_df_1 = spark.read.table("MY_DB.questions")
    tmp_df_2 = spark.read.table("MY_DB.answers")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    df_renamed = tmp_df_1.withColumnRenamed("Id", "QuestionID")
    df_renamed_1 = df_renamed.withColumnRenamed("OwnerUserId", "QuestionerID")
    df_renamed_2 = df_renamed_1.withColumnRenamed("Score", "QuestionScore")
    df_renamed_3 = df_renamed_2.withColumnRenamed("CreationDate", "QuestionCreationDate")

    join_expr = df_renamed_3.QuestionID == tmp_df_2.ParentId
    df_renamed_3.join(tmp_df_2, join_expr, "inner").show(10)

    # Count total answers per question
    result = df_renamed_3.join(tmp_df_2, join_expr, "inner") \
                        .select("QuestionID", "Id") \
                        .groupBy("QuestionID") \
                        .agg(F.count("Id").alias("Number of answers")) \
                        .sort(F.asc("QuestionID"))

    # Save result to csv file
    result.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(os.path.join(DATA_DIR, OUTPUT_DIR))