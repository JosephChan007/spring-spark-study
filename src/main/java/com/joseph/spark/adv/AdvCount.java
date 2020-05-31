package com.joseph.spark.adv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class AdvCount {

    public static void solution1() {
        String sourceFile = "hdfs://joseph-hdfs/spark/study/adv-source.txt";
        String targetFile = "hdfs://joseph-hdfs/spark/study/adv-target";

        SparkConf conf = new SparkConf().setAppName("AdvCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(sourceFile);
        JavaPairRDD<Tuple2<String, String>, Integer> pairRDD = lines.map(line -> {
            String[] words = line.split(" ");
            Tuple2<String, String> t = new Tuple2<>(words[1], words[4]);
            return new Tuple2<>(t, 1);
        }).mapToPair(t -> new Tuple2<>(new Tuple2<>(t._1._1, t._1._2), t._2));

        JavaPairRDD<Tuple2<String, String>, Integer> reducedRDD = pairRDD.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<String, Iterable<Tuple2<Tuple2<String, String>, Integer>>> groupByRDD = reducedRDD.groupBy(t -> t._1._1);
        JavaPairRDD<String, List<Tuple3<String, String, Integer>>> valuesRDD = groupByRDD.mapValues(it -> {
            List<Tuple2<Tuple2<String, String>, Integer>> list = new ArrayList<>();
            it.forEach(t -> list.add(t));
            Collections.sort(list, (t1, t2) -> t2._2 - t1._2);
            List<Tuple2<Tuple2<String, String>, Integer>> subList = list.subList(0, list.size() >= 2 ? 2 : list.size());
            return subList.stream().map(t -> new Tuple3<>(t._1._1, t._1._2, t._2)).collect(Collectors.toList());
        });

        JavaRDD<Tuple3<String, String, Integer>> flatMapRDD = valuesRDD.flatMap(t -> t._2().iterator());
        flatMapRDD.saveAsTextFile(targetFile);

        jsc.stop();
    }

    public static void solution2() {
        String sourceFile = "hdfs://joseph-hdfs/spark/study/adv-source.txt";
        String targetFile = "hdfs://joseph-hdfs/spark/study/adv-target";

        SparkConf conf = new SparkConf().setAppName("AdvCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(sourceFile);
        JavaPairRDD<Tuple2<String, String>, Integer> pairRDD = lines.map(line -> {
            String[] words = line.split(" ");
            Tuple2<String, String> t = new Tuple2<>(words[1], words[4]);
            return new Tuple2<>(t, 1);
        }).mapToPair(t -> new Tuple2<>(new Tuple2<>(t._1._1, t._1._2), t._2));

        JavaPairRDD<Tuple2<String, String>, Integer> reducedRDD = pairRDD.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<String, Tuple2<String, Integer>> pairRDD1 = reducedRDD.mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2)));
        JavaPairRDD<String, Iterable<Tuple2<String, Tuple2<String, Integer>>>> groupByRDD = pairRDD1.groupBy(t -> t._1);
        JavaPairRDD<String, List<Tuple2<String, Integer>>> valuesRDD = groupByRDD.mapValues(it -> {
            List<Tuple2<String, Tuple2<String, Integer>>> list = new ArrayList<>();
            it.forEach(t -> list.add(t));
            Collections.sort(list, (t1, t2) -> t2._2._2 - t1._2._2);
            List<Tuple2<String, Tuple2<String, Integer>>> subList = list.subList(0, list.size() >= 2 ? 2 : list.size());
            return subList.stream().map(e -> new Tuple2<>(e._2._1, e._2._2)).collect(Collectors.toList());
        });

        valuesRDD.saveAsTextFile(targetFile);

        jsc.stop();
    }


    public static void main(String[] args) {
        AdvCount.solution2();
    }


}
