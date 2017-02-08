package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Table implements Comparable<Table>{
    private final int MAX_SLICES = 50;
    private final int MIN_SLICING_SIZE = 1000000;

    @JsonProperty
    public String name;

    @JsonProperty
    public long size;

    private int slices = 1;
    private double weight = 1;

    public Table(String sName, long size) {
        this.name = sName;
        this.size = size;
    }

    @JsonProperty
    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {

        this.weight = weight;
        if (this.size > MIN_SLICING_SIZE) {
            this.slices = (int) (MAX_SLICES * weight);
            if (this.slices == 0) {
                this.slices = 1;
            }
        }
    }

    public int compareTo(Table t)
    {
        return t.size - size > 0 ? 1 : -1;
    }

    @JsonProperty
    public long getSlices() {
        return this.slices;
    }


}
