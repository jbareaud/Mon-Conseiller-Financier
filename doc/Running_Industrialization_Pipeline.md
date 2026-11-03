## Running the Industrialization Pipeline

### 1. AWS

1. Create a dedicated S3 storage bucket
2. Create a user with full S3 and EC2 access rights; export ssh key.
3. On EC2, check Security Group and allow ssh access (we need it for Airflow->EC2 communication). 

### 2. Configure Github Actions

see [Github_Action.md](Github_Action.md) for the detail of environment variables to setup on Githu. 

### 3. Setup MLFlow on Hugging Face

We are using this space : https://huggingface.co/spaces/Ialdaboth/cf-demo-mlflow-server

To setup a copy : 
1. duplicate the space
2. Configure the environment variables :
   - ARTIFACT_STORE_URI : S3 bucket uri
   - AWS_ACCESS_KEY_ID : AWS access key
   - AWS_SECRET_ACCESS_KEY : AWS secret access key
   - BACKEND_STORE_URI : postgres SAL database (we use NeonDB)
   - PORT : port used by the app

### 4. Setup and run Airflow docker image locally 

See [Airflow.md](Airflow.md) for the process.

### 5.Run the process 

Triggered by `Continuous Training Pipeline` action on Github.

## Misc Documentation 

* [Training_script_overrides.md](doc/Training_script_overrides.md) : env variables overrides for the training script
* [Misc.md](doc/Misc.md) : misc env variables