package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Table implements Comparable<Table>{
    @JsonProperty
    public String name;

    @JsonProperty
    public long size;

    @JsonProperty
    public int slices;

    @JsonProperty
    public double weight;

    public Table(String sName, long size) {
        this.name = sName;
        this.size = size;
    }

    public void setWeight(double weight, int minSlicingSize, int maxSlices) {
        this.weight = weight;
        this.slices = calculateSlicesCount(minSlicingSize, maxSlices);
    }

    private int calculateSlicesCount(int minSlicingSize, int maxSlices) {
        int slices = 1;
        if (this.size > minSlicingSize) {
            slices = (int) (maxSlices * weight);
            if (slices == 0) {
                slices = 1;
            }
        }
        return slices;
    }

    public int compareTo(Table t)
    {
        return t.size - size > 0 ? 1 : -1;
    }

}
