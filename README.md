Spark Word count and word reverse jobs in Java

1. Clone this repository
2. Package it with mvn package
3. Copy the spark-java-examples-1.0-SNAPSHOT-jar-with-dependencies.jar to spark installed location[Assume that its /usr/local/spark]
4. Copy input.txt[src/main/resources] to Hadoop[ Assume that its /user/input.txt]
5. Run with the below command
 spark-submit --class com.spark.WordCount --master local spark-java-examples-1.0-SNAPSHOT-jar-with-dependencies.jar /user/input.txt /user/count
 spark-submit --class com.spark.WordReverse --master local spark-java-examples-1.0-SNAPSHOT-jar-with-dependencies.jar /user/input.txt /user/reverse
 
 6. Check the /user/count and  /user/reverse folders and make sure that the output files are created