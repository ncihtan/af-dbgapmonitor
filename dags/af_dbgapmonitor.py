from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import io

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 5, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dbgap_new_requestor_report",
    default_args=default_args,
    description="A DAG to report new dbGaP authorized requestors daily",
    schedule_interval="@daily",
)


def get_new_requestors():
    # Download the tab-separated text file
    url = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetAuthorizedRequestDownload.cgi?study_id=phs002371.v5.p1"
    response = requests.get(url)

    df = pd.read_csv(
        io.StringIO(response.text),
        sep="\t",
        encoding="utf-8",
        skiprows=1,
        names=[
            "Requestor_Affiliation",
            "Project",
            "Date of approval",
            "Request status",
            "Public Research Use Statement",
            "Technical Research Use Statement",
        ],
        index_col=None,
    )

    print(df.head())

    print(df.columns)

    # return current_requestors


def send_email_notification(new_requestors):
    # Assuming you have a method to send email notifications
    # Replace placeholders with actual email sending logic
    email_subject = "New dbGaP Authorized Requestors Added"
    email_body = f"New requestors added: {', '.join(new_requestors)}"
    # Your email sending logic here
    print(email_subject)
    print(email_body)


# Task to fetch new requestors
fetch_new_requestors = PythonOperator(
    task_id="fetch_new_requestors",
    python_callable=get_new_requestors,
    dag=dag,
)

# Task to send email notification
send_email = PythonOperator(
    task_id="send_email_notification",
    python_callable=send_email_notification,
    provide_context=True,  # This is needed to pass data between tasks
    dag=dag,
)

# Define task dependencies
fetch_new_requestors >> send_email
