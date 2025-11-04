package com.cs435;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.*;
import org.apache.spark.broadcast.Broadcast;

public class WikipediaBomb {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Wikipedia Bomb Creator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String titlesPath = "hdfs://wasp.cs.colostate.edu:30121/PA3/resources/titles.txt";
        String linksPath  = "hdfs://wasp.cs.colostate.edu:30121/PA3/resources/links.txt";
        String outputPath = "links_bombed";

        JavaRDD<String> titles = sc.textFile(titlesPath);
        JavaPairRDD<Long, String> indexedTitles = titles.zipWithIndex()
                .mapToPair(x -> new Tuple2<>(x._2() + 1, x._1()));

        List<Tuple2<Long, String>> titleList = indexedTitles.collect();
        String rmnpID = "";
        Set<String> surfIDs = new HashSet<>();

        for (Tuple2<Long, String> entry : titleList) {
            String titleLower = entry._2().toLowerCase();
            if (titleLower.contains("battle_of_china")) {
                rmnpID = entry._1().toString();
            System.out.println("Matched target: " + entry._2() + "  ID=" + entry._1());
            }
            if (titleLower.contains("surf")) {
                surfIDs.add(entry._1().toString());
            }
        }

        if (rmnpID.isEmpty()) {
            System.err.println("Error: cant fing in titles.txt");
            System.exit(2);
        }

        System.out.println("RMNP ID = " + rmnpID);
        System.out.println("Found " + surfIDs.size() + " 'surf' pages.");

        final Broadcast<String> bcRMNP = sc.broadcast(rmnpID);
        final Broadcast<HashSet<String>> bcSurf = sc.broadcast(new HashSet<>(surfIDs));

        JavaRDD<String> lines = sc.textFile(linksPath);

        JavaRDD<String> bombed = lines.map(line -> {
            if (!line.contains(":")) return line;
            String[] parts = line.split(":", 2);
            String from = parts[0].trim();
            String toList = parts.length > 1 ? parts[1].trim() : "";

            Set<String> targets = new LinkedHashSet<>();
            if (!toList.isEmpty()) {
                for (String t : toList.split(" ")) {
                    if (!t.trim().isEmpty()) targets.add(t.trim());
                }
            }

            if (bcSurf.value().contains(from)) {
                targets.add(bcRMNP.value());
            }

            if (!bcSurf.value().contains(from) && Math.random() < 0.005) {
                targets.add(bcRMNP.value());
            }

            StringJoiner sj = new StringJoiner(" ");
            for (String t : targets) sj.add(t);
            return from + ": " + sj.toString();
        });

        bombed.saveAsTextFile(outputPath);
        System.out.println("Wikipedia Bomb graph saved to: " + outputPath);

        sc.close();
    }
}
