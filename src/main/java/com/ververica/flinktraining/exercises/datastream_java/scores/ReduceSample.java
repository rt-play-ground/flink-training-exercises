package com.ververica.flinktraining.exercises.datastream_java.scores;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class ReduceSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataSet<Long> numbers = env.generateSequence(1, 100);

        long start = System.currentTimeMillis();
        numbers.reduce(new NumberReducer()).print();
        long end = System.currentTimeMillis();

        System.out.println("Latency = " + (end - start) + " with parallelism of " + env.getParallelism());
    }

    private static class NumberReducer implements org.apache.flink.api.common.functions.ReduceFunction<Long> {
        @Override
        public Long reduce(Long t2, Long t1) throws Exception {
            System.out.println("Adding " + t1 + " and " + t2 + ", returning " + (t1 + t2));
            return t1 + t2;
        }
    }
}
