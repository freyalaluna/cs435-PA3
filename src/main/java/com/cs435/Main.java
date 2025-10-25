package com.cs435;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static final int NUM_ITERATIONS = 25;
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Java Spark Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> linkList = sc.textFile("src/main/resources/links.txt");
        JavaRDD<String> titleList = sc.textFile("src/main/resources/titles.txt");
        JavaPairRDD<Long, String> indexedTitles = titleList.zipWithIndex().mapToPair(
                                  x-> new Tuple2<>(x._2()+1,x._1()+1));

        JavaPairRDD<String, String> links = linkList.mapToPair(s->new Tuple2<>(s.split(":")[0],s.split(":")[1]));
        LongAccumulator numTitles = sc.sc().longAccumulator();

        //Is this safe to accumulate in parallel?
        titleList.foreach(x -> numTitles.add(1));

        JavaPairRDD<String, Double> ranks = links.mapValues(v->1.0/numTitles.value());

        for(int i = 0; i < NUM_ITERATIONS; i++){
            JavaPairRDD<String, Tuple2<String, Double>> linkRankJoin = links.join(ranks);
            //I get lost here....
            //JavaPairRDD<String, Double> tempRank = linkRankJoin.values().flatMapToPair();
        }
    }
}