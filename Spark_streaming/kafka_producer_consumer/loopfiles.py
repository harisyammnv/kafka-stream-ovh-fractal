import os
path_of_the_directory = '/home/snehasuman/kafka-stream-ovh-fractal/Spark_streaming/pyspark_structured_streaming/part4.3/output'
ext = ('.pdf','.parquet')
for files in os.listdir(path_of_the_directory):
    if files.endswith(ext):
        print(files)  
    else:
        continue