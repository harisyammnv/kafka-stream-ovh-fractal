import sys
import numpy as np
from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
    Parallel Square
    """
    spark = SparkSession\
        .builder\
        .appName("PythonSquared")\
        .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    nums = spark.sparkContext.parallelize(np.arange(1, n, 1))
    squared = nums.map(lambda x: x*x).collect()
    for num in squared:
        print('%i ' % (num))
    spark.stop()