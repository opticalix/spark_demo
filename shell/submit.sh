./bin/spark-submit --class com.opticalix.Main --master local /hadoop/local/jars/spark_demo.jar
./bin/spark-submit --class com.opticalix.Main --master spark://localhost:6066 --deploy-mode cluster /hadoop/local/jars/spark_demo.jar