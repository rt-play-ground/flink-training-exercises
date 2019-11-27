package com.ververica.flinktraining.exercises.datastream_java.scores;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class AggregateScores {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        List<Score> scoreList = Arrays.asList(
                new Score("sachin", 100),
                new Score("rahul", 110),
                new Score("david", 80),
                new Score("george", 20),
                new Score("raj", 120),
                new Score("gayle", 150),
                new Score("baaz", 80),

                new Score("sachin", 70),
                new Score("rahul", 130),
                new Score("david", 40),
                new Score("george", 60),
                new Score("raj", 75),
                new Score("gayle", 15),
                new Score("baaz", 35)
        );

        long start = System.currentTimeMillis();
        env.fromCollection(scoreList)
                .filter(new ScoreFilter())
                .flatMap(new ScoresAggregator())
                .groupBy(0)
                .sum(1)
                .filter(new SumFilter())
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return "\n" + stringIntegerTuple2.f0.toUpperCase() + " is the Greatest Batsman with " + stringIntegerTuple2.f1 + " runs this season.";
                    }
                })
                .print();
        long end = System.currentTimeMillis();

        System.out.println("Latency = " + (end - start) + " with parallelism of " + env.getParallelism());
    }

    public static class ScoresAggregator implements FlatMapFunction<Score, Tuple2<String, Integer>> {

        @Override
        public void flatMap(Score score, Collector<Tuple2<String, Integer>> collector) throws Exception {
            collector.collect(new Tuple2<String, Integer>(score.name, score.score));
        }

    }

    private static class ScoreFilter implements org.apache.flink.api.common.functions.FilterFunction<Score> {
        @Override
        public boolean filter(Score score) throws Exception {
            return score.score >= 100;
        }
    }

    private static class SumFilter implements org.apache.flink.api.common.functions.FilterFunction<Tuple2<String, Integer>> {
        @Override
        public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            return stringIntegerTuple2.f1 > 200;
        }
    }

}

