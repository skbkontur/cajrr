package ru.kontur.cajrr.api;

import org.codehaus.jackson.annotate.JsonProperty;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;

/**
 * Counts metrics with percentages
 */
public class Meter {

    private final String name;
    private Duration duration = java.time.Duration.ZERO;
    private double percent;
    private int completed;
    private int total;
    private Instant started;
    private Instant lastSuccess;
    private Duration average = java.time.Duration.ZERO;
    private Duration estimate = java.time.Duration.ZERO;
    private Duration elapsed = java.time.Duration.ZERO;

    public Meter(String name) {
        this.name = name;
        this.started = Instant.now();
    }

    private void calculatePercent() {
        percent = completed * 100 / total;
    }

    private void calculateEstimate() {
        int left = total - completed;
        estimate = average.multipliedBy(left);
    }

    private void calculateAverage() {
        average = duration.dividedBy(completed);
    }


    private void increaseDuration(Duration elapsed) {
        duration = duration.plus(elapsed);
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public void setCompleted(int completed) {
        this.completed = completed;
    }

    public void mark() {
        this.completed ++;
        Instant now = Instant.now();
        if (null==lastSuccess) {
            elapsed = Duration.between(started, now);
        } else {
            elapsed = Duration.between(lastSuccess, now);
        }
        increaseDuration(elapsed);
        lastSuccess = now;
        calculateAverage();
        calculateEstimate();
        calculatePercent();
    }

    @JsonProperty
    public Duration getElapsed() {
        return elapsed;
    }

    @JsonProperty
    public Duration getDuration() {
        return duration;
    }

    @JsonProperty
    public Duration getAverage() {
        return average;
    }

    @JsonProperty
    public Duration getEstimate() {
        return estimate;
    }

    public double getPercent() {
        return percent;
    }
}
