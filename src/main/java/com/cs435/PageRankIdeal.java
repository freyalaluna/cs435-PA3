package com.cs435;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;


public class PageRankIdeal {

    private static final int NUM_ITERATIONS = 25;

    public static void calculatePR(JavaSparkContext sc, String outputPath) {

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
            System.out.println("Completed iteration " + (i + 1));
        }

        //Find the links with the top 10 PageRanks
        List<Tuple2<Double,String>> topTenRanks = ranks.mapToPair(x->x.swap()).sortByKey(false).take(10);

        JavaPairRDD<Long, Double> topTenLinks = sc.parallelizePairs(topTenRanks).mapToPair(x->new Tuple2<Long, Double>(Long.parseLong(x._2()), x._1()));

        JavaPairRDD<String, Double> topTenRDD = topTenLinks
                                        .join(indexedTitles).values().mapToPair(x->x.swap());

        topTenRDD.saveAsTextFile(outputPath);
                                            
    }

    //local test
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("PageRank Ideal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        try{
            calculatePR(sc, "/PA3/out");
        } finally {
            sc.close();
        }
    }
    
}
