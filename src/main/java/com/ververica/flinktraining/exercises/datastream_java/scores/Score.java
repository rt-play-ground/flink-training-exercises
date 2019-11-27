package com.ververica.flinktraining.exercises.datastream_java.scores;

import java.io.Serializable;

public class Score implements Serializable {

    public int score;
    public String name;

    public Score(String name, int score) {
        this.name = name;
        this.score = score;
    }

    @Override
    public String toString() {
        return "Name : " + name + ", score : " + score;
    }

}
