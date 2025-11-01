package com.cs435;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;


public class PageRankTaxation {
    private static final int NUM_ITERATIONS = 25;
    private static final double BETA = 0.85; //follow links prob
    private static final double TAXATION = 1 - BETA; //teleportion prob
 
    public static void calculatePR(JavaSparkContext sc, String outputPath){
        //PageRank with taxation

        JavaRDD<String> linkList = sc.textFile("hdfs://wasp.cs.colostate.edu:30121/PA3/resources/links.txt");
        JavaRDD<String> titleList = sc.textFile("hdfs://wasp.cs.colostate.edu:30121/PA3/resources/titles.txt");
        JavaPairRDD<Long, String> indexedTitles = titleList.zipWithIndex().mapToPair(
                                  x-> new Tuple2<>(x._2()+1,x._1()));

        //^^ same as PR Ideal

        //Parse links + including dead-end pages
        
        
        long totalPages = indexedTitles.count();

    }

        //local test
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("PageRank Ideal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        try{
            calculatePR(sc, "/PA3/outTAX");
        } finally {
            sc.close();
        }
    }
    
}
