#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from glob import glob
import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from google_drive_downloader import GoogleDriveDownloader as gdd

DATA_DIR = "/usr/local/share/data"
ANSWERS_FILE_GG_ID = "14iHHE3DCZIkCaOU0f4s7p1pMEAIl6r9c"
QUESTIONS_FILE_GG_ID = "14cV5PfcDwqvhEIWTua2OarSoNnKmRylT"
QUESTIONS_FILE = "QuestionSamples.csv"
ANSWERS_FILE = "AnswerSamples.csv"
OUTPUT_DIR = "output"

MONGODB_CLOUD_URI = "mongodb://admin:password@mongo:27017/stackoverflow?authSource=admin"
MONGODB_DATABASE = "stackoverflow"
MONGODB_USER = "admin"
MONGODB_PASSWORD = "password"

SPARK_FILE = "/usr/local/share/spark/dep303_asm2_spark.py"
SPARK_CONN = "spark_submit_conn"

default_args = {
    "owner": "airflow", 
    "start_date": airflow.utils.dates.days_ago(1)
}

def download_file_from_gg_drive(file_id, dest):
    gdd.download_file_from_google_drive(
        file_id=file_id,
        dest_path=dest
    )


def check_data_files_exist():
    if os.path.exists(os.path.join(DATA_DIR, QUESTIONS_FILE)) \
        and os.path.exists(os.path.join(DATA_DIR, ANSWERS_FILE)):
        return "end"
    return "clear_file"


def gen_mongoimport_cmd(uri, user, password, database, collection, file):
    return 'mongoimport "{}" -u {} -p {} --type csv -d {} -c {} --headerline --drop {}'.format(
        MONGODB_CLOUD_URI,
        MONGODB_USER,
        MONGODB_PASSWORD,
        MONGODB_DATABASE,
        collection,
        file
    )


def gen_mongoimport_ouput_cmd():
    """
    Generate command mongoimport for output file(s) of Spark
    """
    files = glob("{}/*.csv".format(os.path.join(DATA_DIR, OUTPUT_DIR)))
    # When only have 1 file
    if files and len(files) == 1:
        return gen_mongoimport_cmd(
            MONGODB_CLOUD_URI,
            MONGODB_USER,
            MONGODB_PASSWORD,
            MONGODB_DATABASE,
            "ouput",
            os.path.join(DATA_DIR, OUTPUT_DIR, files[0])
        )
    # When have many files, import each file
    elif files and len(files) > 1:
        cmds = [gen_mongoimport_cmd(
            MONGODB_CLOUD_URI,
            MONGODB_USER,
            MONGODB_PASSWORD,
            MONGODB_DATABASE,
            "ouput",
            os.path.join(DATA_DIR, OUTPUT_DIR, f)
        ) for f in files]
        return " && ".join(cmds)
    return ""


with DAG(dag_id="dep303_asm2", 
         default_args=default_args, 
         schedule_interval="@daily") as dag:

    start = DummyOperator(task_id="start")

    end = DummyOperator(task_id="end")

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=check_data_files_exist
    )

    clear_file = BashOperator(
        task_id="clear_file",
        bash_command="rm -f {} {}".format(
            os.path.join(DATA_DIR, QUESTIONS_FILE),
            os.path.join(DATA_DIR, ANSWERS_FILE)
        )
    )

    dowload_answer_file_task = PythonOperator(
        task_id="dowload_answer_file_task",
        python_callable=download_file_from_gg_drive,
        op_kwargs={"file_id": ANSWERS_FILE_GG_ID,
        "dest": os.path.join(DATA_DIR, ANSWERS_FILE)}
    )

    import_answers_cmd = gen_mongoimport_cmd(
        MONGODB_CLOUD_URI,
        MONGODB_USER,
        MONGODB_PASSWORD,
        MONGODB_DATABASE,
        "answers",
        os.path.join(DATA_DIR, ANSWERS_FILE)
    )
    import_answers_mongo = BashOperator(
        task_id="import_answers_mongo",
        bash_command=import_answers_cmd
    )

    dowload_question_file_task = PythonOperator(
        task_id="dowload_question_file_task",
        python_callable=download_file_from_gg_drive,
        op_kwargs={"file_id": QUESTIONS_FILE_GG_ID,
        "dest": os.path.join(DATA_DIR, QUESTIONS_FILE)}
    )

    import_questions_cmd = gen_mongoimport_cmd(
        MONGODB_CLOUD_URI,
        MONGODB_USER,
        MONGODB_PASSWORD,
        MONGODB_DATABASE,
        "questions",
        os.path.join(DATA_DIR, QUESTIONS_FILE)
    )
    import_question_mongo = BashOperator(
        task_id="import_question_mongo",
        bash_command=import_questions_cmd
    )

    spark_process = SparkSubmitOperator(
        task_id="spark_submit_task",
        application =SPARK_FILE,
        conn_id=SPARK_CONN,
        packages="org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
    )
    
    import_output_mongo = BashOperator(
        task_id="import_output_mongo",
        bash_command=gen_mongoimport_ouput_cmd()
    )

    start >> branching >> [clear_file, end]
    clear_file >> [dowload_answer_file_task, dowload_question_file_task]
    dowload_answer_file_task >> import_answers_mongo
    dowload_question_file_task >> import_question_mongo
    [import_answers_mongo, import_question_mongo] >> spark_process >> import_output_mongo >> end