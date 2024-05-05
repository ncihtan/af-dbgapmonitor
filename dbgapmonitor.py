import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import io
import requests
import json
import os

# Download the tab-separated text file
url = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/GetAuthorizedRequestDownload.cgi?study_id=phs002371.v5.p1"
response = requests.get(url)


# Read the CSV file without header and with arbitrary column names
df = (
    pl.read_csv(
        # "dbGaPAuthorizedRequests.phs002371.v1.p1.tab-delimited.txt",
        io.StringIO(response.text),
        separator="\t",
        truncate_ragged_lines=True,
        try_parse_dates=True,
    )
    .rename({"Cloud Service AdministratorData stewardRequestor": "Requestor"})
    .with_columns(pl.col("Date of approval").str.to_date("%b%d, %Y"))
    .sort("Date of approval", descending=True)
)

# Strip extra whitespace from the columns
df = df.with_columns(
    pl.col("Requestor").str.strip_chars(),
    pl.col("Affiliation").str.strip_chars(),
    pl.col("Project").str.strip_chars(),
)


# Filter for those approved in the last month
# Get today's date
today = datetime.today()

# Calculate the date from 30 days ago
last_month = today - timedelta(days=7)
df_recent = df.filter(pl.col("Date of approval") > last_month)

print(df_recent)


def dataframe_to_slack_block_with_md_links(df):
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*New dbGaP Authorized Requestors added in the last 7 days*",
            },
        }
    ]
    for index, row in df.iterrows():
        line = f"{row['Requestor']} from {row['Affiliation']} {row['Request status']} on {row['Date of approval'].strftime('%a %d %B')}\n> {row['Project']}"
        block = {"type": "section", "text": {"type": "mrkdwn", "text": f"{line}"}}
        blocks.append(block)
    return {"blocks": blocks}


def send_message_to_slack_blocks(webhook_url, blocks):
    headers = {"Content-Type": "application/json"}
    data = json.dumps(blocks)
    response = requests.post(webhook_url, headers=headers, data=data)
    if response.status_code != 200:
        raise ValueError(
            f"Request to slack returned an error {response.status_code}, the response is:\n{response.text}"
        )


if df_recent.to_pandas().empty:
    # If no modified entities are found, prepare a simple message for Slack
    slack_message_blocks = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "No new dbGaP Authorized Requestors added in the last 7 days",
                },
            }
        ]
    }
else:
    # If there are modified entities, format the message as before
    slack_message_blocks = dataframe_to_slack_block_with_md_links(df_recent.to_pandas())

# Usage
# Get the webhook URL from a env variable called SLACK_WEBHOOK_URL
webhook_url = os.getenv("SLACK_WEBHOOK_URL")
send_message_to_slack_blocks(webhook_url, slack_message_blocks)
