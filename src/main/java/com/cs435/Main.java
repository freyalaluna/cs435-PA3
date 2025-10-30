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

public class Main {
    public static final int NUM_ITERATIONS = 25;
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Java Spark Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> linkList = sc.textFile("hdfs://wasp.cs.colostate.edu:30121/PA3/resources/links.txt");
        JavaRDD<String> titleList = sc.textFile("hdfs://wasp.cs.colostate.edu:30121/PA3/resources/titles.txt");
        JavaPairRDD<Long, String> indexedTitles = titleList.zipWithIndex().mapToPair(
                                  x-> new Tuple2<>(x._2()+1,x._1()));

        JavaPairRDD<String, String> links = linkList.mapToPair(s->new Tuple2<>(s.split(":")[0].trim(),s.split(":")[1].trim()));
        long totalPages = links.count();

        JavaPairRDD<String, Double> ranks = links.mapValues(v->1.0/totalPages);

        for(int i = 0; i < NUM_ITERATIONS; i++){
            JavaPairRDD<String, Tuple2<String, Double>> linkRankJoin = links.join(ranks);
            //For each pair <neighbor, rank> from the above <node, <neighbors,rank>>, split neighbors, recalc each pair such that newRank = rank/number of neighbors
            JavaPairRDD<String, Double> tempRank = linkRankJoin.values().flatMapToPair(
                    new PairFlatMapFunction<Tuple2<String, Double>, String, Double>() {
                        @Override
                        public Iterator<Tuple2<String, Double>> call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                            List<String> urls = List.of(stringDoubleTuple2._1().split(" "));
                            List<Tuple2<String, Double>> newRanks = Lists.newArrayList();
                            for(String url : urls){
                                newRanks.add(new Tuple2<>(url,stringDoubleTuple2._2()/urls.size()));
                            }

                            return newRanks.iterator();
                        }

                    }
            );

            //Update each link's rank with the sum of each occurrence within tempRank
            ranks = tempRank.reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            });
        }

        //Find the links with the top 10 PageRanks
        List<Tuple2<Double,String>> topTenRanks = ranks.mapToPair(x->x.swap()).sortByKey(false).take(10);

        //Key type conversion for joining
        JavaPairRDD<Long, Double> topTenLinks = sc.parallelizePairs(topTenRanks).mapToPair(x->new Tuple2<Long, Double>(Long.parseLong(x._2()), x._1()));

        //Replace link index with the actual title
        JavaPairRDD<String, Double> topTenRDD = topTenLinks
                                                .join(indexedTitles).values().mapToPair(x->x.swap());

        topTenRDD.saveAsTextFile("hdfs://wasp.cs.colostate.edu:30121/PA3/out/");
    }
}