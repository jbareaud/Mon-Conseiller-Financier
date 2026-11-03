# Mon Conseiller Financier

## Installing and running locally

### Installation

```commandline
uv venv --python 3.13.2
source .venv/bin/activate
uv pip install -r requirements.txt
```
### Env. variables

Set env variable `STEPS` to a comma-separated list of required steps to execute :
- `dataset` : fetch dataset from HF
- `train` : train model
- `score_perplexity` : score perplexity of trained model
- `score_with_llm` : score trained model using LLM-as-judge
- `copy_s3` : copy trained model to S3

### Run

Running the training script locally :
```commandline
uv run ./src/main.py
```

Running tests :
```commandline
uv run pytest
```

## Industrialization

See documentation for the full process :

TODO

## Structure 

- **.github/workflows/** contains Github Actions
- **airflow/** contains Airflow Docker compose and DAG
- **configs/**
  - **configs/training_conf.yaml** default configuration used by Python training script; can be overriden by env variables 
- **dataset/**
- **doc/**
- **docker/** Docker file for training image
- **scripts/trigger_airflow.py** triggered by github action to send image hash to Airflow
- **src/** Python project
  - **src/aws/upload_adapter.py** copy lora adapter to S3 
  - **src/dataset/fetch_dataset.py** fetching dataset function
  - **src/training/trainer.py** training function
  - **src/validation/judge.py** score model using LLM-as-judge
  - **src/validation/perplexity.py** score model perplexity
  - **src/main.py** main entrypoint
- **tests/** 

## Architecture

![Schema](M06-MLOps.png)
