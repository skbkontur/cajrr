package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Table implements Comparable<Table>{
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

    public void setWeight(double weight, int minSlicingSize, int maxSlices) {

        this.weight = weight;
        if (this.size > minSlicingSize) {
            this.slices = (int) (maxSlices * weight);
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
    public int getSlices() {
        return this.slices;
    }


}
