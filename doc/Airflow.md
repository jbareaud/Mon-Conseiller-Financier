## Setting up Airflow 3.3.2 with Podman

### Download the Airflow docker compose yaml 

```commandline
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.3.2/docker-compose.yaml'
```

### Patch the compose file to add NGROK

In the Airflow docker compose yaml file, add the following service definition :

```
  ngrok:
    image: ngrok/ngrok:latest
    restart: always
    network_mode: host
    command:
      - "http"
      - "8080"
    environment:
      NGROK_AUTHTOKEN: ${NGROK_AUTHTOKEN}
    depends_on:
      airflow-apiserver:
        condition: service_healthy

```

### Create the local directories

```commandline
mkdir -p ./logs ./plugins ./config
sudo chmod -R 777 ./logs ./data ./config
```

### Setting up AIRFLOW_UID

```commandline
echo "AIRFLOW_UID=50000" > .env
```

### Configuration

Enrich the .env file with the following keys :

```
AIRFLOW_UID=50000
#AIRFLOW_UID=$(id -u)  # On Linux/Mac, this sets the user ID
# GitHub Configuration
GITHUB_REPO=your-username/your-repo-name
GITHUB_PAT=your_github_personal_access_token  # From Step 1b
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=eu-west-3  # or your preferred region
KEY_PAIR_NAME=your-ec2-key-pair-name  # From Step 1a (e.g., "airflow-training-key")
SECURITY_GROUP_ID=sg-xxxxxxxxx  # Security group allowing SSH (port 22)
# MLflow Configuration
REPORT_TRAINING="mlflow"
MLFLOW_TRACKING_URI=http://your-mlflow-server:5000
MLFLOW_EXPERIMENT_NAME=california_housing
# SSH Private Key (for connecting to EC2)
# From Step 1a: Open the downloaded .pem file and paste its entire contents here
# Make sure to include the BEGIN and END lines!
SSH_PRIVATE_KEY_CONTENT="-----BEGIN RSA PRIVATE KEY-----
...
-----END RSA PRIVATE KEY-----"
# Ngrok Configuration (optional - for exposing Airflow UI publicly)
# Get your auth token from https://dashboard.ngrok.com/get-started/your-authtoken
NGROK_AUTHTOKEN=your_ngrok_authtoken_here
# Misc
AMI_ID=ami-00ac45f3035ff009e
INSTANCE_TYPE=t3.small
```

### Managing Podman permissions (**DOT IT FOR EVERY SESSION !**)

```commandline
systemctl --user enable --now podman.socket
export DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock"
```

### init airflow.cfg with default values

```commandline
podman compose run airflow-cli airflow config list
```

### init container database

```commandline
podman compose up airflow-init
```

### Lauching the app

```commandline
podman compose up -d
```

Airflow if available at http://localhost:8080 with credentials airflow/airflow

Once Airflow is up and ready, launch ngrok :

```commandline
podman compose up -d ngrok
```

Ngrok interface (with public url) should be accessible at 'http://localhost:4040'
