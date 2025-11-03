package com.cs435;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;


public class PageRankTaxation {
    private static final int NUM_ITERATIONS = 25;
    private static final double BETA = 0.85; //follow links
 
    public static void calculatePR(JavaSparkContext sc, String outputPath){
        //PageRank with taxation

        JavaRDD<String> linkList = sc.textFile("hdfs://wasp.cs.colostate.edu:30121/PA3/resources/links.txt");
        JavaRDD<String> titleList = sc.textFile("hdfs://wasp.cs.colostate.edu:30121/PA3/resources/titles.txt");
        JavaPairRDD<Long, String> indexedTitles = titleList.zipWithIndex().mapToPair(
                                  x-> new Tuple2<>(x._2()+1,x._1()));

        JavaPairRDD<String, String> links = linkList.mapToPair(s->new Tuple2<>(s.split(":")[0].trim(),s.split(":")[1].trim()));
        long totalPages = indexedTitles.count();
        double teleportProb = (1 - BETA);

        JavaPairRDD<String, Double> ranks = indexedTitles.mapToPair(title -> new Tuple2<>(title._1.toString(), 1.0/totalPages));

        for(int i = 0; i < NUM_ITERATIONS; i++){
            JavaPairRDD<String, Tuple2<String, Double>> linkRankJoin = links.join(ranks);

            JavaPairRDD<String, Double> tempRank = linkRankJoin.values().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Double>,String,Double>() {
                @Override
                public Iterator<Tuple2<String, Double>> call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                    List<String> urls = List.of(stringDoubleTuple2._1().split(" "));
                    List<Tuple2<String, Double>> newRanks = Lists.newArrayList();
                    for(String url : urls){
                        newRanks.add(new Tuple2<>(url,stringDoubleTuple2._2()/urls.size()));
                    }

                    return newRanks.iterator();
                }
            });
            ranks = tempRank.reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            })
            .mapValues(new Function<Double, Double>() {
                @Override
                public Double call(Double pageRank) {
                    return BETA*pageRank + teleportProb/totalPages;
                }
            });

            List<Tuple2<Double,String>> topTenRanks = ranks.mapToPair(x->x.swap()).sortByKey(false).take(10);

            JavaPairRDD<Long, Double> topTenLinks = sc.parallelizePairs(topTenRanks).mapToPair(x->new Tuple2<Long, Double>(Long.parseLong(x._2()), x._1()));

            JavaPairRDD<String, Double> topTenRDD = topTenLinks
                                            .join(indexedTitles).values().mapToPair(x->x.swap());

            topTenRDD.saveAsTextFile(outputPath);
        }
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
