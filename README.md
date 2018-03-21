# warp10-spark2

spark-submit --conf 'spark.driver.extraJavaOptions=-Dwarp.timeunits=us' --conf 'spark.executor.extraJavaOptions=-Dwarp.timeunits=us' --jars spark-all-0.5.jar test.py 

