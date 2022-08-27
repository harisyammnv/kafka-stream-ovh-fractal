PROJECTID =9a99eb2b742e4a7caebd1b6903685d97
JOBNAME=test-spark-app
REGION =GRA
SPARK-VERSION =3.3.0
CLASS= main-class
PACKAGES =org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0,com.amazonaws:aws-java-sdk:1.12.275,org.apache.hadoop:hadoop-aws:3.3.0
./ovh-spark-submit --projectid $PROJECTID  --packages $PACKAGES --class $CLASS --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 4G --num-executors 1 swift://testPythonScript/s/s3.py