package com.surveys.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FovResponse {
    @JsonProperty("surveySessionId")
    private String surveySessionId;
    
    @JsonProperty("speciesName")
    private String speciesName;
    
    @JsonProperty("deviceId")
    private String deviceId;
    
    @JsonProperty("coordinates")
    private Object coordinates;

    public FovResponse() {
    }

    public FovResponse(String surveySessionId, String speciesName, String deviceId, Object coordinates) {
        this.surveySessionId = surveySessionId;
        this.speciesName = speciesName;
        this.deviceId = deviceId;
        this.coordinates = coordinates;
    }

    public String getSurveySessionId() {
        return surveySessionId;
    }

    public void setSurveySessionId(String surveySessionId) {
        this.surveySessionId = surveySessionId;
    }

    public String getSpeciesName() {
        return speciesName;
    }

    public void setSpeciesName(String speciesName) {
        this.speciesName = speciesName;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Object getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(Object coordinates) {
        this.coordinates = coordinates;
    }
}

