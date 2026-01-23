package com.surveys.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class BatchResponse<T> {
    @JsonProperty("results")
    private List<T> results;
    
    @JsonProperty("totalProcessed")
    private int totalProcessed;

    public BatchResponse() {
    }

    public BatchResponse(List<T> results, int totalProcessed) {
        this.results = results;
        this.totalProcessed = totalProcessed;
    }

    public List<T> getResults() {
        return results;
    }

    public void setResults(List<T> results) {
        this.results = results;
    }

    public int getTotalProcessed() {
        return totalProcessed;
    }

    public void setTotalProcessed(int totalProcessed) {
        this.totalProcessed = totalProcessed;
    }
}
