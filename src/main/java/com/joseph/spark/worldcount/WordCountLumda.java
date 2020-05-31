package com.joseph.spark.worldcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountLumda {

    public static void main(String[] args) {

        String sourceFile = "hdfs://joseph-hdfs/spark/study/wc-source.txt";
        String targetFile = "hdfs://joseph-hdfs/spark/study/wc-target";

        // 本地模式
        // SparkConf conf = new SparkConf().setAppName("WordCountLumda").setMaster("local[*]");

        SparkConf conf = new SparkConf().setAppName("WordCountLumda");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(sourceFile);
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((m, n) -> m + n);
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());
        result.saveAsTextFile(targetFile);

        jsc.stop();
    }



}
