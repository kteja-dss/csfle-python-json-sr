# Confluent Kafka Producer and Consumer with CSFLE in python

Steps to setup the environment

```
python3 -m venv csfle_env
source csfle_env/bin/activate
pip install confluent_kafka
pip install -r requirements/requirements.txt
```

Steps to run the demo

- Create a new .env file in the root project
- Configure the environment variables in the .env file with the appropriate values. Sample reference can be found in .env.sample file. 
- Run producer and consumer scripts in the separate terminals
- Verify the name field is encrypted by looking at the record in the cloud console

When you're finished with the venv:

```
deactivate
```
