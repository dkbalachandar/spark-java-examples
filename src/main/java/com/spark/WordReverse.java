package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class WordReverse {

    public static void main(String[] args) throws Exception {

        String inFile = args[0];
        String outFile = args[1];
        //Create a Spark context with the app name
        SparkConf conf = new SparkConf().setAppName("SPARK-WORDREVERSE");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //Load the input data
        JavaRDD<String> input = sc.textFile(inFile);
        //Split into words with the regex
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) {
                return Arrays.asList(x.split("\\s+"));
            }
        });
        //Reverse the content
        JavaRDD<String> reverse = words.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return new StringBuilder(s).reverse().toString();
            }
        });
        reverse.saveAsTextFile(outFile);
    }
}
