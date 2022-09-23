1)Firstly you need to run the mosquitto protocol to run  all the MQTT stuff using this command:
  mosquitto -v

2)Then run the mqtt_kafka_producer.py file using this command:
  python3 mqtt_kafka_producer.py

3)Then run the mqtt_kafka_bridge.py file using this command:
  python3 mqtt_kafka_bridge.py

4)Then run the python3 kafka_consumer_uploading_S3.py file either in the Data processing as a job or vs code of the ovh   cloud :-
  a) To run this file as a job on the ovh cloud :
    >Login into the ovh account and then
    >Go  to the public cloud tab.
    >Go to the Data Processing category.
    >Click on the run new job. 
    >Fill the details.
    >Finally submit your job.
  b) To run this file on vs code of ovh cloud:
    >Follow the first two line of the above step.
    >Then Go to the AI notebook category
    >Afer clicking on the AI notebook you will get a notebook created by us.
    >Launch the Vs code from here and now u have your vs code on the ovh cloud 
    >Run the file here using the command:
      python3 kafka_consumer_uploading_S3.py

 5)Then run the Access_ovh_upload_lakefs.py file using the command:
    python3 Access_ovh_upload_lakefs.py      