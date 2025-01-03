# Confluent Kafka Producer and Consumer with CSFLE in python

Steps to setup the environment

```
python3 -m venv venv_examples
source venv_examples/bin/activate
pip install confluent_kafka
pip install -r requirements/requirements-examples.txt
```

Steps to run the demo

- Configure the environment variables with the appropriate values
- Run producer and consumer scripts in the separate terminals
- Verify the name field is encrypted by looking at the record in the cloud console

When you're finished with the venv:

```
deactivate
```
