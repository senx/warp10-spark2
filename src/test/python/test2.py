from py4j.java_gateway import java_import
from pyspark import SparkContext

sc = SparkContext()
java_import(sc._gateway.jvm,"io.warp10.spark.PySparkLauncher")

func = sc._gateway.jvm.PySparkLauncher()
params = {'inFile': 'file:///tmp/test.in', 'outFile': 'file:///tmp/test.out'}
print func.testPySparkLauncher(params)