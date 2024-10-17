from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from google.cloud import bigquery
import time
import subprocess
import re

def log_etl_run(task_instance, status, row_count=0, error_message=None, test_passes=0, test_failures=0, total_tests=0, test_summary=""):
    client = bigquery.Client()

    # Get current time
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Prepare the log entry with task metadata
    log_entry = {
        "task_name": task_instance.task_id,  # Dynamically get the task name from Airflow
        "run_id": task_instance.run_id,  # Airflow run ID for tracking the specific execution
        "start_time": task_instance.start_date.strftime("%Y-%m-%d %H:%M:%S"),  # Start time of the task
        "end_time": end_time,  # End time (when this function is called)
        "status": status,  # Pass 'success' or 'failure' based on task outcome
        "row_count": row_count,  # Pass row count if applicable (default is 0)
        "test_passes": test_passes,  # Store number of passed tests
        "test_failures": test_failures,  # Store number of failed tests
        "total_tests": total_tests,  # Store total number of tests run
        "test_summary": test_summary,  # Summary of the test run
        "error_message": error_message or "",  # Capture error message, default to empty string if none
        "loaded_date": end_time  # Log the time this entry was written to BigQuery
    }

    table_id = "football-data-pipeline.etl_audit_logs.etl_audit_logs"

    # Insert into BigQuery
    errors = client.insert_rows_json(table_id, [log_entry])
    if errors:
        print(f"Failed to log ETL run: {errors}")
        
def slack_alert(context):
    task_id = context['task_instance'].task_id
    error_message = str(context.get('exception', 'Unknown error'))
    timestamp = context.get('ts', 'N/A')  # Use 'N/A' if 'ts' is missing
    print(f"Slack alert triggered for task {task_id}. Error: {error_message}")
    
    alert = SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_webhook',
        message=f"Task {task_id} failed at {timestamp}. Error: {error_message}"
    )
    return alert.execute(context=context)

def failure_callback_with_slack(context):
    # Log the failure in BigQuery
    log_etl_run(
        context['task_instance'], 
        status='failure', 
        error_message=str(context.get('exception', 'Unknown error'))  # Capture error message
    )
    
    # Send Slack alert
    slack_alert(context)
    
def run_dbt_test_and_log_results(task_instance, dbt_command):
    # Run the dbt test command and capture output
    process = subprocess.Popen(dbt_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    stdout_str = stdout.decode('utf-8')
    stderr_str = stderr.decode('utf-8')
    exit_code = process.returncode  # Capture the exit code

    # Log both stdout and stderr to Airflow logs
    print("dbt test output (stdout):")
    print(stdout_str)  # Print the test results in Airflow logs

    print("dbt test errors (stderr):")
    print(stderr_str)  # Log any errors from dbt test in Airflow logs

    # Extract test pass/fail information from the stdout
    pass_match = re.search(r"PASS=(\d+)", stdout_str)
    fail_match = re.search(r"ERROR=(\d+)", stdout_str)  # Modify to capture FAIL
    total_match = re.search(r"TOTAL=(\d+)", stdout_str)

    test_passes = int(pass_match.group(1)) if pass_match else 0
    test_failures = int(fail_match.group(1)) if fail_match else 0
    total_tests = int(total_match.group(1)) if total_match else 0

    # Collect a clean summary from the dbt test output
    clean_summary = []
    for line in stdout_str.splitlines():
        if 'PASS' in line or 'ERROR' in line or 'WARN' in line or 'FAIL' in line or 'SKIP' in line:
            clean_summary.append(line)
        if 'FAIL' in line:
            clean_summary.append(line)

    test_summary = "\n".join(clean_summary)

    # Determine test status based on exit code and failures
    status = 'success' if exit_code == 0 and test_failures == 0 else 'failure'

    # Log the result into BigQuery - this will happen regardless of pass/fail
    log_etl_run(
        task_instance, 
        status=status, 
        test_passes=test_passes, 
        test_failures=test_failures,
        total_tests=total_tests,
        test_summary=test_summary
    )

    # If the test failed or the dbt command exited with a non-zero code, trigger Slack alert and raise an exception
    if exit_code != 0 or test_failures > 0:
        failure_callback_with_slack({'task_instance': task_instance})
        raise Exception(f"dbt test failed: {test_summary}")  # Raise an exception to fail the Airflow task

# Define default arguments
default_args = {
    'owner': 'Ste Hardy',
    'retries': 1, 
    'retry_delay': timedelta(minutes=1)
}


# Define the DAG
with DAG(
    dag_id='football_data_dbt_pipeline_dag',
    default_args=default_args,
    description='Orchestrate dbt models with Airflow',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),  
    catchup=False,
) as dag:

    # Task: Competition data ingest
    competition_data_ingest = BashOperator(
        task_id='competition_data_ingest',
        bash_command='python /opt/airflow/src/ingest/competition_data_ingest.py',
    on_success_callback=lambda context: log_etl_run(
        context['task_instance'], 
        status='success'
    ),
    on_failure_callback=failure_callback_with_slack
    )

    # Task: Run dbt silver Models
    run_dbt_silver = BashOperator(
        task_id='run_dbt_silver',
        bash_command='cd /opt/airflow/src/transform && /home/airflow/.local/bin/dbt run --models silver --exclude gold --profiles-dir /opt/airflow/config/dbt --no-partial-parse --debug',
    on_success_callback=lambda context: log_etl_run(
        context['task_instance'], 
        status='success'
    ),
    on_failure_callback=failure_callback_with_slack
    )
    
    # Task: Run silver dbt Tests
    run_silver_dbt_tests = PythonOperator(
        task_id='run_silver_dbt_tests',
        python_callable=lambda task_instance: run_dbt_test_and_log_results(
            task_instance, 'cd /opt/airflow/src/transform && /home/airflow/.local/bin/dbt test --models silver --exclude gold --profiles-dir /opt/airflow/config/dbt --no-partial-parse --debug'
        ),
        on_failure_callback=failure_callback_with_slack
    )
    
    # Task: Run dbt Snapshots
    run_dbt_snapshot = BashOperator(
        task_id='run_dbt_snapshot',
        bash_command='cd /opt/airflow/src/transform && /home/airflow/.local/bin/dbt snapshot --profiles-dir /opt/airflow/config/dbt --debug',
    on_success_callback=lambda context: log_etl_run(
        context['task_instance'], 
        status='success'
    ),
    on_failure_callback=failure_callback_with_slack
    )

    # Task: Run dbt gold Models
    run_dbt_gold = BashOperator(
        task_id='run_dbt_gold',
        bash_command='cd /opt/airflow/src/transform && /home/airflow/.local/bin/dbt run --models gold --profiles-dir /opt/airflow/config/dbt --no-partial-parse --debug',
    on_success_callback=lambda context: log_etl_run(
        context['task_instance'], 
        status='success'
    ),
    on_failure_callback=failure_callback_with_slack
    )

    # Task: Run gold dbt Tests
    run_gold_dbt_tests = PythonOperator(
        task_id='run_gold_dbt_tests',
        python_callable=lambda task_instance: run_dbt_test_and_log_results(
            task_instance, 'cd /opt/airflow/src/transform && /home/airflow/.local/bin/dbt test --models gold --profiles-dir /opt/airflow/config/dbt --no-partial-parse --debug'
        ),
        on_failure_callback=failure_callback_with_slack
    )

    # Define task dependencies
    competition_data_ingest >> run_dbt_silver >> run_silver_dbt_tests >> run_dbt_snapshot >> run_dbt_gold >> run_gold_dbt_tests
