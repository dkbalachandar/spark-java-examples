package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) throws Exception {

        String inFile = args[0];
        String outFile = args[1];
        //Create a Spark context with the app name
        SparkConf conf = new SparkConf().setAppName("SPARK-WORDCOUNT");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //Load the input data
        JavaRDD<String> input = sc.textFile(inFile);
        //Split into words with the regex
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) {
                return Arrays.asList(x.split("\\s+"));
            }
        });
        //Map it with word and count
        JavaPairRDD<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2(s, 1);
                    }
                });
        //Reduce it by Key
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        counts.saveAsTextFile(outFile);
    }
}
