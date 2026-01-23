package com.surveys.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.surveys.dto.BreadcrumbResponse;
import com.surveys.dto.ErrorResponse;
import com.surveys.dto.FovResponse;
import com.surveys.dto.LisaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(StreamingService.class);

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
                        ST_AsGeoJSON(geom)::json AS geometry
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
                        ST_AsGeoJSON(geom)::json AS geometry
                    FROM public.layer_peak_copy
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
        
        // Handle JSON geometry - PostgreSQL returns JSON as string or PGobject
        Object geometry = parseCoordinates(rs, "geometry");
        response.setGeometry(geometry);
        
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
        
        // Handle JSON geometry
        Object geometry = parseCoordinates(rs, "geometry");
        response.setGeometry(geometry);
        
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

    /**
     * Retrieves MVT (Mapbox Vector Tile) data for FOV layer.
     * 
     * @param z Zoom level
     * @param x Tile X coordinate
     * @param y Tile Y coordinate
     * @param surveySessionId Survey session ID
     * @return Binary MVT tile data
     */
    public byte[] getFovMvtTile(int z, int x, int y, String surveySessionId) {
        String query = """
            SELECT ST_AsMVT(tile, 'fov_layer', 4096, 'geom')
            FROM (
              SELECT
               "surveySessionId",
                ST_AsMVTGeom(
                  ST_Transform(geom, 3857),
                  ST_TileEnvelope(?, ?, ?),
                  4096,
                  256,
                  true
                ) AS geom
              FROM public.layer_fov_copy
              WHERE ST_Transform(geom, 3857) && ST_TileEnvelope(?, ?, ?)
              AND "surveySessionId" = ?
            ) tile;
            """;

        Object[] queryParams = new Object[]{z, x, y, z, x, y, surveySessionId};
        
        // Log the query template and parameters for debugging
        logger.debug("Executing MVT query template:\n{}", query);
        logger.debug("Query parameters: [z={}, x={}, y={}, z={}, x={}, y={}, surveySessionId='{}']", 
            z, x, y, z, x, y, surveySessionId);
        
        // Log formatted query with parameters (for easier debugging)
        String formattedQuery = String.format(
            "SELECT ST_AsMVT(tile, 'fov_layer', 4096, 'geom') " +
            "FROM (SELECT \"surveySessionId\", ST_AsMVTGeom(ST_Transform(geom, 3857), ST_TileEnvelope(%d, %d, %d), 4096, 256, true) AS geom " +
            "FROM public.layer_fov_copy WHERE ST_Transform(geom, 3857) && ST_TileEnvelope(%d, %d, %d) AND \"surveySessionId\" = '%s') tile;",
            z, x, y, z, x, y, surveySessionId
        );
        logger.debug("Formatted query (for reference):\n{}", formattedQuery);

        long queryStartTime = System.currentTimeMillis();
        try {
            List<byte[]> results = jdbcTemplate.query(
                query,
                new ArgumentPreparedStatementSetter(queryParams),
                (rs, rowNum) -> {
                    // ST_AsMVT returns bytea (byte array)
                    byte[] tileData = rs.getBytes(1);
                    logger.debug("Retrieved MVT tile data, size: {} bytes", tileData != null ? tileData.length : 0);
                    return tileData;
                }
            );
            
            long queryExecutionTime = System.currentTimeMillis() - queryStartTime;
            
            if (results == null || results.isEmpty()) {
                logger.debug("MVT query returned no results - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms", 
                    z, x, y, surveySessionId, queryExecutionTime);
                return null;
            }
            
            byte[] tileData = results.get(0);
            logger.debug("MVT query completed successfully - z: {}, x: {}, y: {}, surveySessionId: {}, tileSize: {} bytes, executionTime: {}ms", 
                z, x, y, surveySessionId, tileData != null ? tileData.length : 0, queryExecutionTime);
            
            return tileData;
        } catch (Exception e) {
            long queryExecutionTime = System.currentTimeMillis() - queryStartTime;
            logger.error("Database error retrieving MVT tile - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms, error: {}", 
                z, x, y, surveySessionId, queryExecutionTime, e.getMessage(), e);
            throw new RuntimeException("Error retrieving MVT tile", e);
        }
    }

    /**
     * Retrieves MVT (Mapbox Vector Tile) data for LISA layer.
     * 
     * @param z Zoom level
     * @param x Tile X coordinate
     * @param y Tile Y coordinate
     * @param surveySessionId Survey session ID
     * @return Binary MVT tile data
     */
    public byte[] getLisaMvtTile(int z, int x, int y, String surveySessionId) {
        String query = """
            SELECT ST_AsMVT(tile, 'lisa_layer', 4096, 'geom')
            FROM (
              SELECT
               "surveySessionId",
                ST_AsMVTGeom(
                  ST_Transform(geom, 3857),
                  ST_TileEnvelope(?, ?, ?),
                  4096,
                  256,
                  true
                ) AS geom
              FROM public.layer_peak
              WHERE ST_Transform(geom, 3857) && ST_TileEnvelope(?, ?, ?)
              AND "surveySessionId" = ?
            ) tile;
            """;

        Object[] queryParams = new Object[]{z, x, y, z, x, y, surveySessionId};
        
        // Log the query template and parameters for debugging
        logger.debug("Executing MVT query template for LISA:\n{}", query);
        logger.debug("Query parameters: [z={}, x={}, y={}, z={}, x={}, y={}, surveySessionId='{}']", 
            z, x, y, z, x, y, surveySessionId);
        
        // Log formatted query with parameters (for easier debugging)
        String formattedQuery = String.format(
            "SELECT ST_AsMVT(tile, 'lisa_layer', 4096, 'geom') " +
            "FROM (SELECT \"surveySessionId\", ST_AsMVTGeom(ST_Transform(geom, 3857), ST_TileEnvelope(%d, %d, %d), 4096, 256, true) AS geom " +
            "FROM public.layer_peak WHERE ST_Transform(geom, 3857) && ST_TileEnvelope(%d, %d, %d) AND \"surveySessionId\" = '%s') tile;",
            z, x, y, z, x, y, surveySessionId
        );
        logger.debug("Formatted query (for reference):\n{}", formattedQuery);

        long queryStartTime = System.currentTimeMillis();
        try {
            List<byte[]> results = jdbcTemplate.query(
                query,
                new ArgumentPreparedStatementSetter(queryParams),
                (rs, rowNum) -> {
                    // ST_AsMVT returns bytea (byte array)
                    byte[] tileData = rs.getBytes(1);
                    logger.debug("Retrieved MVT tile data for LISA, size: {} bytes", tileData != null ? tileData.length : 0);
                    return tileData;
                }
            );
            
            long queryExecutionTime = System.currentTimeMillis() - queryStartTime;
            
            if (results == null || results.isEmpty()) {
                logger.debug("MVT query returned no results for LISA - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms", 
                    z, x, y, surveySessionId, queryExecutionTime);
                return null;
            }
            
            byte[] tileData = results.get(0);
            logger.debug("MVT query completed successfully for LISA - z: {}, x: {}, y: {}, surveySessionId: {}, tileSize: {} bytes, executionTime: {}ms", 
                z, x, y, surveySessionId, tileData != null ? tileData.length : 0, queryExecutionTime);
            
            return tileData;
        } catch (Exception e) {
            long queryExecutionTime = System.currentTimeMillis() - queryStartTime;
            logger.error("Database error retrieving MVT tile for LISA - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms, error: {}", 
                z, x, y, surveySessionId, queryExecutionTime, e.getMessage(), e);
            throw new RuntimeException("Error retrieving MVT tile for LISA", e);
        }
    }

    /**
     * Retrieves MVT (Mapbox Vector Tile) data for Breadcrumb layer.
     * 
     * @param z Zoom level
     * @param x Tile X coordinate
     * @param y Tile Y coordinate
     * @param surveySessionId Survey session ID
     * @return Binary MVT tile data
     */
    public byte[] getBreadcrumbMvtTile(int z, int x, int y, String surveySessionId) {
        String query = """
            SELECT ST_AsMVT(tile, 'breadcrumb_layer', 4096, 'geom')
            FROM (
              SELECT
               "surveySessionId",
                ST_AsMVTGeom(
                  ST_Transform(coordinate, 3857),
                  ST_TileEnvelope(?, ?, ?),
                  4096,
                  256,
                  true
                ) AS geom
              FROM public.layer_breadcrumb
              WHERE ST_Transform(coordinate, 3857) && ST_TileEnvelope(?, ?, ?)
              AND "surveySessionId" = ?
            ) tile;
            """;

        Object[] queryParams = new Object[]{z, x, y, z, x, y, surveySessionId};
        
        // Log the query template and parameters for debugging
        logger.debug("Executing MVT query template for Breadcrumb:\n{}", query);
        logger.debug("Query parameters: [z={}, x={}, y={}, z={}, x={}, y={}, surveySessionId='{}']", 
            z, x, y, z, x, y, surveySessionId);
        
        // Log formatted query with parameters (for easier debugging)
        String formattedQuery = String.format(
            "SELECT ST_AsMVT(tile, 'breadcrumb_layer', 4096, 'geom') " +
            "FROM (SELECT \"surveySessionId\", ST_AsMVTGeom(ST_Transform(coordinate, 3857), ST_TileEnvelope(%d, %d, %d), 4096, 256, true) AS geom " +
            "FROM public.layer_breadcrumb WHERE ST_Transform(coordinate, 3857) && ST_TileEnvelope(%d, %d, %d) AND \"surveySessionId\" = '%s') tile;",
            z, x, y, z, x, y, surveySessionId
        );
        logger.debug("Formatted query (for reference):\n{}", formattedQuery);

        long queryStartTime = System.currentTimeMillis();
        try {
            List<byte[]> results = jdbcTemplate.query(
                query,
                new ArgumentPreparedStatementSetter(queryParams),
                (rs, rowNum) -> {
                    // ST_AsMVT returns bytea (byte array)
                    byte[] tileData = rs.getBytes(1);
                    logger.debug("Retrieved MVT tile data for Breadcrumb, size: {} bytes", tileData != null ? tileData.length : 0);
                    return tileData;
                }
            );
            
            long queryExecutionTime = System.currentTimeMillis() - queryStartTime;
            
            if (results == null || results.isEmpty()) {
                logger.debug("MVT query returned no results for Breadcrumb - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms", 
                    z, x, y, surveySessionId, queryExecutionTime);
                return null;
            }
            
            byte[] tileData = results.get(0);
            logger.debug("MVT query completed successfully for Breadcrumb - z: {}, x: {}, y: {}, surveySessionId: {}, tileSize: {} bytes, executionTime: {}ms", 
                z, x, y, surveySessionId, tileData != null ? tileData.length : 0, queryExecutionTime);
            
            return tileData;
        } catch (Exception e) {
            long queryExecutionTime = System.currentTimeMillis() - queryStartTime;
            logger.error("Database error retrieving MVT tile for Breadcrumb - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms, error: {}", 
                z, x, y, surveySessionId, queryExecutionTime, e.getMessage(), e);
            throw new RuntimeException("Error retrieving MVT tile for Breadcrumb", e);
        }
    }
}

