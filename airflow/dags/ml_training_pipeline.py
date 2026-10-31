import boto3
import time
import requests
import base64
import os
import paramiko
import io
from datetime import datetime, timedelta
from airflow.sdk import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2TerminateInstanceOperator,
)
from airflow.task.trigger_rule import TriggerRule

# ------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------

# GitHub Config
GITHUB_REPO = os.getenv("GITHUB_REPO") 
# User changed env var name to GITHUB_PAT, updating here:
GITHUB_TOKEN = os.getenv("GITHUB_PAT") 
BRANCH_NAME = "main"

# AWS Config
KEY_PAIR_NAME = os.getenv("KEY_PAIR_NAME") 
AMI_ID = os.getenv("AMI_ID", "ami-00ac45f3035ff009e") 
SECURITY_GROUP_ID = os.getenv("SECURITY_GROUP_ID")
INSTANCE_TYPE = os.getenv("INSTANCE_TYPE", "t3.small")

# Connection (Direct Env Vars)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION_NAME = os.getenv("AWS_DEFAULT_REGION", "eu-west-3")
ARTIFACT_STORE_URI = os.getenv("ARTIFACT_STORE_URI")

# MLFlow Config
REPORT_TRAINING = os.getenv("REPORT_TRAINING", "mlflow")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "mon_conseiller_financier")

# overrides
NB_EPOCHS = os.getenv("NB_EPOCHS", 1)
STEPS = os.getenv("STEPS", "train,score_perplexity,copy_s3")

# User Data (Install Docker + MLflow)
USER_DATA_SCRIPT = """#!/bin/bash
# 1. Redirect all output to a dedicated log file for easy debugging
exec > /var/log/user-data-debug.log 2>&1
set -x # Print every command before executing

echo "--> STARTING SETUP"

# 2. Prevent apt-get from asking questions
export DEBIAN_FRONTEND=noninteractive

# 3. Install System Dependencies
echo "--> Installing Apt Packages..."
apt-get update
apt-get install -y docker.io git python3-pip python3-venv

# 4. Configure Docker
echo "--> Configuring Docker..."
systemctl start docker
systemctl enable docker
usermod -aG docker ubuntu
# Ensure socket is ready before chmod
while [ ! -S /var/run/docker.sock ]; do echo "Waiting for Docker Socket..."; sleep 1; done
chmod 666 /var/run/docker.sock

# 5. Install MLflow (The dangerous part)
echo "--> Installing Python Libs..."
# BYPASS UBUNTU PROTECTIONS
sudo -u ubuntu pip3 install mlflow boto3 --break-system-packages

# This makes 'mlflow' callable from anywhere, even non-interactive shells.
ln -s /home/ubuntu/.local/bin/mlflow /usr/bin/mlflow

# 6. Signal Success
echo "--> SETUP COMPLETE"
touch /tmp/airflow_ready
"""

DAG_ID = 'github_ec2_ml_training'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id=DAG_ID,
    schedule=None,  # <--- FIX: Changed from schedule_interval to schedule
    default_args=default_args,
    catchup=False,
    tags=['github', 'ec2', 'ml-training'],
) as dag:

    # ------------------------------------------------------------------
    # TASK 1: Poll GitHub Actions Status
    # ------------------------------------------------------------------
    @task
    def wait_for_github_ci():
        if not GITHUB_TOKEN or not GITHUB_REPO:
            raise ValueError("Missing GITHUB_PAT or GITHUB_REPO env vars")

        api_url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs"
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        params = {"branch": BRANCH_NAME, "status": "completed", "per_page": 1}

        print(f"📡 Polling GitHub Actions for {GITHUB_REPO}...")

        # Poll for 10 minutes (20 checks * 30 seconds)
        for _ in range(20): 
            try:
                response = requests.get(api_url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                if not data['workflow_runs']:
                    print("No workflow runs found yet.")
                    time.sleep(30)
                    continue

                latest_run = data['workflow_runs'][0]
                conclusion = latest_run['conclusion']
                
                print(f"🔎 Latest Run Conclusion: {conclusion}")

                if conclusion == 'success':
                    print("✅ CI Passed!")
                    return True
                elif conclusion == 'failure':
                    raise Exception("❌ Latest CI run failed!")
                
            except Exception as e:
                print(f"Polling error: {e}")
                time.sleep(30)

        raise TimeoutError("Timed out waiting for GitHub CI.")

    # ------------------------------------------------------------------
    # TASK 2: Create EC2 Instance
    # ------------------------------------------------------------------
    create_ec2 = EC2CreateInstanceOperator(
        task_id="create_ec2_instance",
        region_name=REGION_NAME, 
        image_id=AMI_ID,
        max_count=1,
        min_count=1,
        config={
            "InstanceType": INSTANCE_TYPE,
            "KeyName": KEY_PAIR_NAME,
            "SecurityGroupIds": [SECURITY_GROUP_ID] if SECURITY_GROUP_ID else [],
            "UserData": base64.b64encode(USER_DATA_SCRIPT.encode("ascii")).decode("ascii"),
            "TagSpecifications": [{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': 'Airflow-Trainer'}]}]
        },
        wait_for_completion=True
    )

    # ------------------------------------------------------------------
    # TASK 3: Check EC2 Status Checks (2/2)
    # ------------------------------------------------------------------
    @task
    def check_ec2_status(instance_ids):
        instance_id = instance_ids[0]
        # Boto3 client auto-reads credentials from os.environ
        client = boto3.client('ec2', region_name=REGION_NAME)
        
        print(f"⏳ Waiting for Instance {instance_id} to pass status checks...")
        waiter = client.get_waiter('instance_status_ok')
        waiter.wait(InstanceIds=[instance_id])
        
        print(f"✅ Instance {instance_id} is ready.")
        return instance_id

    # ------------------------------------------------------------------
    # TASK 4: Get Public IP
    # ------------------------------------------------------------------
    @task
    def get_public_ip(instance_id):
        ec2 = boto3.resource('ec2', region_name=REGION_NAME)
        instance = ec2.Instance(instance_id)
        return instance.public_ip_address

    # ------------------------------------------------------------------
    # TASK 5: Run Training (SSH with Memory Key)
    # ------------------------------------------------------------------
    @task
    def run_training_via_ssh(public_ip):
        print(f"🔌 Connecting to {public_ip}...")
        
        key_content = os.getenv("SSH_PRIVATE_KEY_CONTENT")
        if not key_content:
            raise ValueError("SSH_PRIVATE_KEY_CONTENT is missing!")

        #ssh_key = paramiko.RSAKey.from_private_key(io.StringIO(key_content))
        ssh_key = paramiko.RSAKey.from_private_key_file(
            "/opt/airflow/airflow-training-key.pem"
        )

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            # 1. Connect
            connected = False
            for i in range(10):
                try:
                    ssh.connect(public_ip, username='ubuntu', pkey=ssh_key, timeout=10)
                    connected = True
                    break
                except Exception as e:
                    print(f"Waiting for SSH... ({e})")
                    time.sleep(10)

            if not connected:
                raise ConnectionError(f"❌ Impossible de joindre {public_ip}:22 après 10 tentatives. Vérifiez le Security Group AWS et le routage VPC.")

            # 2. Wait for MLflow installation to be usable
            print("⏳ Waiting for MLflow installation to be usable...")
            
            # We loop until python can successfully import mlflow.
            # This prevents the "No module named mlflow" error.
            wait_cmd = """
            for i in {1..60}; do
                if python3 -c "import mlflow" > /dev/null 2>&1; then
                    echo "✅ Python found MLflow!"
                    exit 0
                fi
                echo "Waiting for library..."
                sleep 5
            done
            echo "❌ Timed out waiting for MLflow lib"
            exit 1
            """
            
            stdin, stdout, stderr = ssh.exec_command(wait_cmd)
            if stdout.channel.recv_exit_status() != 0:
                print(stderr.read().decode())
                raise Exception("Environment Setup Failed")
                
            # ============================================================
            # 🔍 DEBUG STEP: DUMP STARTUP LOGS
            # ============================================================
            # DEBUG STEP: Read our custom log file
            print("\n🔍 READING USER DATA DEBUG LOG:")
            # Note: checking /var/log/user-data-debug.log instead of cloud-init-output.log
            stdin, stdout, stderr = ssh.exec_command("cat /var/log/user-data-debug.log")
            print(stdout.read().decode())

            # 3. Run Training
            cmd = f"""
                export REPORT_TRAINING={REPORT_TRAINING}
                export MLFLOW_TRACKING_URI={MLFLOW_TRACKING_URI}
                export MLFLOW_EXPERIMENT_NAME={MLFLOW_EXPERIMENT_NAME}
                export AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}
                export AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}
                export AWS_DEFAULT_REGION={REGION_NAME}
                export ARTIFACT_STORE_URI={ARTIFACT_STORE_URI}
                export NB_EPOCHS={NB_EPOCHS}
                export STEPS={STEPS}
                
                echo "🔍 VERIFYING MLFLOW INSTALLATION:"
                which mlflow
                
                echo "🚀 Starting MLflow Run..."
                python3 -m mlflow run https://github.com/{GITHUB_REPO} \
                    --version {BRANCH_NAME} \
                    --entry-point src/main.py \
                    --build-image
            """
            
            stdin, stdout, stderr = ssh.exec_command(cmd)
            
            for line in iter(stdout.readline, ""):
                print(line, end="")
            
            if stdout.channel.recv_exit_status() != 0:
                print("\n❌ REMOTE SCRIPT FAILED. LOGS FROM USER_DATA:")
                # Dump the setup log to see why pip failed
                _, log_out, _ = ssh.exec_command("cat /var/log/user-data.log")
                print(log_out.read().decode())
                
                print("\n❌ STDERR:")
                print(stderr.read().decode())
                raise Exception("Training Failed")
                
        finally:
            ssh.close()
    # ------------------------------------------------------------------
    # TASK 6: Cleanup
    # ------------------------------------------------------------------
    terminate_ec2 = EC2TerminateInstanceOperator(
        task_id="terminate_ec2_instance",
        region_name=REGION_NAME,
        instance_ids="{{ task_instance.xcom_pull(task_ids='create_ec2_instance')[0] }}",
        trigger_rule=TriggerRule.ALL_DONE, # Run even if training failed
    )

    # ------------------------------------------------------------------
    # FLOW DEFINITION
    # ------------------------------------------------------------------

    # 1. Instantiate the TaskFlow tasks
    github_signal = wait_for_github_ci()

    # 2. Pass data from Standard Operator -> TaskFlow Task
    # CRITICAL FIX: Use 'create_ec2.output' to get the XComArg (Instance IDs)
    # The operator returns a list, so check_ec2_status will receive that list.
    validated_instance_id = check_ec2_status(create_ec2.output)

    # 3. Pass data between TaskFlow tasks
    public_ip = get_public_ip(validated_instance_id)
    training_output = run_training_via_ssh(public_ip)

    # 4. Define the Execution Order (The Graph)
    # We explicitly chain them to ensure they appear in the UI
    github_signal >> create_ec2

    # Note: The data dependencies above (passing variables) technically imply order,
    # but adding explicit bitshift (>>) guarantees the graph renders correctly.
    create_ec2 >> validated_instance_id >> public_ip >> training_output >> terminate_ec2