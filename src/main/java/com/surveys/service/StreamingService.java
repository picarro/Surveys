package com.surveys.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.surveys.dto.BreadcrumbResponse;
import com.surveys.dto.ErrorResponse;
import com.surveys.dto.FovResponse;
import com.surveys.dto.LisaResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.List;

@Service
public class StreamingService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public void streamFovData(String surveySessionId, int limit, SseEmitter emitter) throws IOException {
        final int batchSize = 1000;
        int offset = 0;
        int totalRows = 0;

        try {
            while (totalRows < limit) {
                int remainingLimit = limit - totalRows;
                int currentBatchSize = Math.min(batchSize, remainingLimit);

                if (currentBatchSize <= 0) {
                    break;
                }

                String query = """
                    SELECT
                        "surveySessionId",
                        "speciesName",
                        "deviceId",
                        ST_AsGeoJSON(coordinates)::json AS coordinates
                    FROM public.layer_fov_copy
                    WHERE "surveySessionId" = ?
                    ORDER BY ctid
                    LIMIT ? OFFSET ?
                    """;

                List<FovResponse> results = jdbcTemplate.query(
                    query,
                    new ArgumentPreparedStatementSetter(new Object[]{surveySessionId, currentBatchSize, offset}),
                    this::mapFovRow
                );

                if (results.isEmpty() || totalRows >= limit) {
                    break;
                }

                for (FovResponse row : results) {
                    emitter.send(SseEmitter.event()
                        .data(objectMapper.writeValueAsString(row)));
                    totalRows++;
                }

                offset += results.size();

                if (totalRows >= limit || results.size() < batchSize) {
                    break;
                }
            }
            emitter.complete();
        } catch (Exception e) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("Error streaming FOV data", e.getMessage())
                    )));
            } catch (IOException ioException) {
                // Ignore if emitter is already closed
            }
            emitter.completeWithError(e);
        }
    }

    public void streamBreadcrumbData(String surveySessionId, int limit, int batchSize, SseEmitter emitter) throws IOException {
        int offset = 0;
        int totalRows = 0;

        try {
            while (totalRows < limit) {
                int remainingLimit = limit - totalRows;
                int currentBatchSize = Math.min(batchSize, remainingLimit);

                if (currentBatchSize <= 0) {
                    break;
                }

                String query = """
                    SELECT
                        "surveySessionId",
                        ST_AsGeoJSON(coordinates)::json AS coordinates
                    FROM public.layer_breadcrumb
                    WHERE "surveySessionId" = ?
                    ORDER BY ctid
                    LIMIT ? OFFSET ?
                    """;

                List<BreadcrumbResponse> results = jdbcTemplate.query(
                    query,
                    new ArgumentPreparedStatementSetter(new Object[]{surveySessionId, currentBatchSize, offset}),
                    this::mapBreadcrumbRow
                );

                if (results.isEmpty() || totalRows >= limit) {
                    break;
                }

                for (BreadcrumbResponse row : results) {
                    emitter.send(SseEmitter.event()
                        .data(objectMapper.writeValueAsString(row)));
                    totalRows++;
                }

                offset += results.size();

                if (totalRows >= limit || results.size() < batchSize) {
                    break;
                }
            }
            emitter.complete();
        } catch (Exception e) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("Error streaming breadcrumb data", e.getMessage())
                    )));
            } catch (IOException ioException) {
                // Ignore if emitter is already closed
            }
            emitter.completeWithError(e);
        }
    }

    public void streamLisaData(String surveySessionId, int limit, int batchSize, SseEmitter emitter) throws IOException {
        int offset = 0;
        int totalRows = 0;

        try {
            while (totalRows < limit) {
                int remainingLimit = limit - totalRows;
                int currentBatchSize = Math.min(batchSize, remainingLimit);

                if (currentBatchSize <= 0) {
                    break;
                }

                String query = """
                    SELECT
                        "surveySessionId",
                        ST_AsGeoJSON(coordinates)::json AS coordinates
                    FROM public.layer_peak
                    WHERE "surveySessionId" = ?
                    ORDER BY ctid
                    LIMIT ? OFFSET ?
                    """;

                List<LisaResponse> results = jdbcTemplate.query(
                    query,
                    new ArgumentPreparedStatementSetter(new Object[]{surveySessionId, currentBatchSize, offset}),
                    this::mapLisaRow
                );

                if (results.isEmpty() || totalRows >= limit) {
                    break;
                }

                for (LisaResponse row : results) {
                    emitter.send(SseEmitter.event()
                        .data(objectMapper.writeValueAsString(row)));
                    totalRows++;
                }

                offset += results.size();

                if (totalRows >= limit || results.size() < batchSize) {
                    break;
                }
            }
            emitter.complete();
        } catch (Exception e) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("Error streaming LISA data", e.getMessage())
                    )));
            } catch (IOException ioException) {
                // Ignore if emitter is already closed
            }
            emitter.completeWithError(e);
        }
    }

    private FovResponse mapFovRow(ResultSet rs, int rowNum) throws SQLException {
        FovResponse response = new FovResponse();
        response.setSurveySessionId(rs.getString("surveySessionId"));
        response.setSpeciesName(rs.getString("speciesName"));
        response.setDeviceId(rs.getString("deviceId"));
        
        // Handle JSON coordinates - PostgreSQL returns JSON as string or PGobject
        Object coordinates = parseCoordinates(rs, "coordinates");
        response.setCoordinates(coordinates);
        
        return response;
    }

    private BreadcrumbResponse mapBreadcrumbRow(ResultSet rs, int rowNum) throws SQLException {
        BreadcrumbResponse response = new BreadcrumbResponse();
        response.setSurveySessionId(rs.getString("surveySessionId"));
        
        // Handle JSON coordinates
        Object coordinates = parseCoordinates(rs, "coordinates");
        response.setCoordinates(coordinates);
        
        return response;
    }

    private LisaResponse mapLisaRow(ResultSet rs, int rowNum) throws SQLException {
        LisaResponse response = new LisaResponse();
        response.setSurveySessionId(rs.getString("surveySessionId"));
        
        // Handle JSON coordinates
        Object coordinates = parseCoordinates(rs, "coordinates");
        response.setCoordinates(coordinates);
        
        return response;
    }

    /**
     * Parses PostgreSQL JSON/JSONB object into a Map for proper JSON serialization.
     * Handles PGobject by extracting the JSON string and parsing it.
     */
    private Object parseCoordinates(ResultSet rs, String columnName) throws SQLException {
        try {
            // Try to get as string first (works for JSON columns)
            String jsonString = rs.getString(columnName);
            if (jsonString == null || jsonString.trim().isEmpty()) {
                return null;
            }
            // Parse JSON string into Map for proper serialization
            return objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            // Fallback: try to get as object and handle PGobject via reflection
            try {
                Object obj = rs.getObject(columnName);
                if (obj == null) {
                    return null;
                }
                
                // Check if it's a PGobject using reflection (to avoid import issues)
                if (obj.getClass().getName().equals("org.postgresql.util.PGobject")) {
                    String jsonValue = (String) obj.getClass().getMethod("getValue").invoke(obj);
                    if (jsonValue == null || jsonValue.trim().isEmpty()) {
                        return null;
                    }
                    return objectMapper.readValue(jsonValue, new TypeReference<Map<String, Object>>() {});
                } else if (obj instanceof String) {
                    return objectMapper.readValue((String) obj, new TypeReference<Map<String, Object>>() {});
                } else if (obj instanceof Map) {
                    return obj;
                } else {
                    // Try to convert to string and parse
                    return objectMapper.readValue(obj.toString(), new TypeReference<Map<String, Object>>() {});
                }
            } catch (Exception ex) {
                // If all parsing fails, return null
                return null;
            }
        }
    }
}

