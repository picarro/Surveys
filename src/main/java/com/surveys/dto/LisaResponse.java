package com.surveys.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LisaResponse {
    @JsonProperty("surveySessionId")
    private String surveySessionId;
    
    @JsonProperty("geometry")
    private Object geometry;

    public LisaResponse() {
    }

    public LisaResponse(String surveySessionId, Object geometry) {
        this.surveySessionId = surveySessionId;
        this.geometry = geometry;
    }

    public String getSurveySessionId() {
        return surveySessionId;
    }

    public void setSurveySessionId(String surveySessionId) {
        this.surveySessionId = surveySessionId;
    }

    public Object getGeometry() {
        return geometry;
    }

    public void setGeometry(Object geometry) {
        this.geometry = geometry;
    }
}

