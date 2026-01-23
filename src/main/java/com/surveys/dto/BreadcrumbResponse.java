package com.surveys.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BreadcrumbResponse {
    @JsonProperty("surveySessionId")
    private String surveySessionId;
    
    @JsonProperty("coordinates")
    private Object coordinates;

    public BreadcrumbResponse() {
    }

    public BreadcrumbResponse(String surveySessionId, Object coordinates) {
        this.surveySessionId = surveySessionId;
        this.coordinates = coordinates;
    }

    public String getSurveySessionId() {
        return surveySessionId;
    }

    public void setSurveySessionId(String surveySessionId) {
        this.surveySessionId = surveySessionId;
    }

    public Object getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(Object coordinates) {
        this.coordinates = coordinates;
    }
}

