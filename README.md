# Monitor authorized requestors for the HTAN dbGap study

This project contains a Python script called `dbgapmonitor.py` that monitors changes in the authorized requestor for the HTAN dbGaP study, accession [phs002371.v5.p1](https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=phs002371.v3.p1)

## Prerequisites

Before running the script, make sure you have the following installed:

- Python 3.x
- Required Python packages (specified in `requirements.txt`)

## Installation

1. Clone the repository:

    ```shell
    git clone https://github.com/your-username/af-dbgapmonitor.git
    ```

2. Navigate to the project directory:

    ```shell
    cd af-dbgapmonitor
    ```

3. Install the required packages:

    ```shell
    pip install -r requirements.txt
    ```

4. Setup a Slack [incomming webhook](https://api.slack.com/messaging/webhooks) that posts to your target channe. Your webhook URL will look something like this:
    ```shell
    https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
    ```

## Usage

To run the `dbgapmonitor.py` script, use the following command:

```shell
export SLACK_WEBHOOK_URL='<your-webhook-url>'
python dbgapmonitor.py
```

This repoistory also contains a workflow that runs weekly at 0800 GMT. The webhook url is set througn a Github repository secret.
