package com.joseph.spark.worldcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WorldCount {


    public static void main(String[] args) {

        String sourceFile = "hdfs://hdfs-host3/spark/study/wc-source.txt";
        String targetFile = "hdfs://hdfs-host3/spark/study/wc-target";

        SparkConf conf = new SparkConf().setAppName("WorldCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(sourceFile);

        // 切分压平
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        // 各单词次数赋值
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String world) throws Exception {
                return new Tuple2<>(world, 1);
            }
        });

        // 按单词聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 调换顺序：key是数量，value是单词
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) throws Exception {
                return tp.swap();
            }
        });

        // 按单词总数排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);

        // 调换顺序：key是单词，value是数量
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tp) throws Exception {
                return tp.swap();
            }
        });

        // 将结果写入文件
        result.saveAsTextFile(targetFile);

        // 释放资源
        jsc.stop();
    }

}
