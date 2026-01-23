package com.surveys.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.surveys.dto.ErrorResponse;
import com.surveys.dto.HealthResponse;
import com.surveys.service.StreamingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
public class SurveysController {

    private static final Logger logger = LoggerFactory.getLogger(SurveysController.class);

    @Autowired
    private StreamingService streamingService;

    @Autowired
    private ObjectMapper objectMapper;

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @GetMapping("/health")
    public ResponseEntity<HealthResponse> health() {
        HealthResponse response = new HealthResponse("ok", Instant.now().toString());
        return ResponseEntity.ok(response);
    }

    @GetMapping(value = "/api/fov", produces = "text/event-stream")
    public SseEmitter getFov(
            @RequestParam(required = false) String surveySessionId,
            @RequestParam(required = false, defaultValue = "100000") int limit,
            @RequestParam(required = false, defaultValue = "100") int batchSize) {
        
        SseEmitter emitter = new SseEmitter(3600000L); // 1 hour timeout
        
        // Validate required parameter
        if (surveySessionId == null || surveySessionId.trim().isEmpty()) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("surveySessionId is required", null)
                    )));
                emitter.complete();
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
            return emitter;
        }

        // Validate numeric parameters
        if (limit < 1) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("limit must be a positive integer", null)
                    )));
                emitter.complete();
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
            return emitter;
        }

        if (batchSize < 1) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("batchSize must be a positive integer", null)
                    )));
                emitter.complete();
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
            return emitter;
        }

        // Execute streaming in a separate thread
        executorService.execute(() -> {
            try {
                streamingService.streamFovData(surveySessionId, limit, batchSize, emitter);
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });

        return emitter;
    }

    @GetMapping(value = "/api/breadcrumb", produces = "text/event-stream")
    public SseEmitter getBreadcrumb(
            @RequestParam(required = false) String surveySessionId,
            @RequestParam(required = false, defaultValue = "100000") int limit,
            @RequestParam(required = false, defaultValue = "100") int batchSize) {
        
        SseEmitter emitter = new SseEmitter(3600000L); // 1 hour timeout
        
        // Validate required parameter
        if (surveySessionId == null || surveySessionId.trim().isEmpty()) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("surveySessionId is required", null)
                    )));
                emitter.complete();
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
            return emitter;
        }

        // Validate numeric parameters
        if (limit < 1) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("limit must be a positive integer", null)
                    )));
                emitter.complete();
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
            return emitter;
        }

        if (batchSize < 1) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("batchSize must be a positive integer", null)
                    )));
                emitter.complete();
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
            return emitter;
        }

        // Execute streaming in a separate thread
        executorService.execute(() -> {
            try {
                streamingService.streamBreadcrumbData(surveySessionId, limit, batchSize, emitter);
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });

        return emitter;
    }

    @GetMapping(value = "/api/lisa", produces = "text/event-stream")
    public SseEmitter getLisa(
            @RequestParam(required = false) String surveySessionId,
            @RequestParam(required = false, defaultValue = "100000") int limit,
            @RequestParam(required = false, defaultValue = "100") int batchSize) {
        
        SseEmitter emitter = new SseEmitter(3600000L); // 1 hour timeout
        
        // Validate required parameter
        if (surveySessionId == null || surveySessionId.trim().isEmpty()) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("surveySessionId is required", null)
                    )));
                emitter.complete();
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
            return emitter;
        }

        // Validate numeric parameters
        if (limit < 1) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("limit must be a positive integer", null)
                    )));
                emitter.complete();
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
            return emitter;
        }

        if (batchSize < 1) {
            try {
                emitter.send(SseEmitter.event()
                    .name("error")
                    .data(objectMapper.writeValueAsString(
                        new ErrorResponse("batchSize must be a positive integer", null)
                    )));
                emitter.complete();
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
            return emitter;
        }

        // Execute streaming in a separate thread
        executorService.execute(() -> {
            try {
                streamingService.streamLisaData(surveySessionId, limit, batchSize, emitter);
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });

        return emitter;
    }

    @GetMapping(value = "/api/mvt/fov/{z}/{x}/{y}", produces = "application/vnd.mapbox-vector-tile")
    public ResponseEntity<byte[]> getFovMvtTile(
            @PathVariable int z,
            @PathVariable int x,
            @PathVariable int y,
            @RequestParam(required = false) String surveySessionId) {
        
        logger.info("Received MVT tile request - z: {}, x: {}, y: {}, surveySessionId: {}", z, x, y, surveySessionId);
        
        // Validate required parameter
        if (surveySessionId == null || surveySessionId.trim().isEmpty()) {
            logger.warn("MVT tile request rejected - missing surveySessionId parameter");
            return ResponseEntity.badRequest().build();
        }

        // Validate tile coordinates
        if (z < 0 || z > 20) {
            logger.warn("MVT tile request rejected - invalid zoom level: {} (must be 0-20)", z);
            return ResponseEntity.badRequest().build();
        }

        int maxTile = (int) Math.pow(2, z);
        if (x < 0 || x >= maxTile || y < 0 || y >= maxTile) {
            logger.warn("MVT tile request rejected - invalid tile coordinates: x={}, y={} (max: {})", x, y, maxTile - 1);
            return ResponseEntity.badRequest().build();
        }

        long startTime = System.currentTimeMillis();
        try {
            logger.debug("Executing MVT tile query for z={}, x={}, y={}, surveySessionId={}", z, x, y, surveySessionId);
            byte[] tileData = streamingService.getFovMvtTile(z, x, y, surveySessionId);
            long executionTime = System.currentTimeMillis() - startTime;
            
            if (tileData == null || tileData.length == 0) {
                logger.info("MVT tile query returned empty result - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms", 
                    z, x, y, surveySessionId, executionTime);
                return ResponseEntity.noContent().build();
            }
            
            logger.info("MVT tile query successful - z: {}, x: {}, y: {}, surveySessionId: {}, tileSize: {} bytes, executionTime: {}ms", 
                z, x, y, surveySessionId, tileData.length, executionTime);
            
            return ResponseEntity.ok()
                    .header("Content-Type", "application/vnd.mapbox-vector-tile")
                    .body(tileData);
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            logger.error("Error retrieving MVT tile - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms, error: {}", 
                z, x, y, surveySessionId, executionTime, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping(value = "/api/mvt/lisa/{z}/{x}/{y}", produces = "application/vnd.mapbox-vector-tile")
    public ResponseEntity<byte[]> getLisaMvtTile(
            @PathVariable int z,
            @PathVariable int x,
            @PathVariable int y,
            @RequestParam(required = false) String surveySessionId) {
        
        logger.info("Received MVT tile request for LISA - z: {}, x: {}, y: {}, surveySessionId: {}", z, x, y, surveySessionId);
        
        // Validate required parameter
        if (surveySessionId == null || surveySessionId.trim().isEmpty()) {
            logger.warn("MVT tile request rejected - missing surveySessionId parameter");
            return ResponseEntity.badRequest().build();
        }

        // Validate tile coordinates
        if (z < 0 || z > 20) {
            logger.warn("MVT tile request rejected - invalid zoom level: {} (must be 0-20)", z);
            return ResponseEntity.badRequest().build();
        }

        int maxTile = (int) Math.pow(2, z);
        if (x < 0 || x >= maxTile || y < 0 || y >= maxTile) {
            logger.warn("MVT tile request rejected - invalid tile coordinates: x={}, y={} (max: {})", x, y, maxTile - 1);
            return ResponseEntity.badRequest().build();
        }

        long startTime = System.currentTimeMillis();
        try {
            logger.debug("Executing MVT tile query for LISA - z={}, x={}, y={}, surveySessionId={}", z, x, y, surveySessionId);
            byte[] tileData = streamingService.getLisaMvtTile(z, x, y, surveySessionId);
            long executionTime = System.currentTimeMillis() - startTime;
            
            if (tileData == null || tileData.length == 0) {
                logger.info("MVT tile query returned empty result for LISA - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms", 
                    z, x, y, surveySessionId, executionTime);
                return ResponseEntity.noContent().build();
            }
            
            logger.info("MVT tile query successful for LISA - z: {}, x: {}, y: {}, surveySessionId: {}, tileSize: {} bytes, executionTime: {}ms", 
                z, x, y, surveySessionId, tileData.length, executionTime);
            
            return ResponseEntity.ok()
                    .header("Content-Type", "application/vnd.mapbox-vector-tile")
                    .body(tileData);
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            logger.error("Error retrieving MVT tile for LISA - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms, error: {}", 
                z, x, y, surveySessionId, executionTime, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping(value = "/api/mvt/breadcrumb/{z}/{x}/{y}", produces = "application/vnd.mapbox-vector-tile")
    public ResponseEntity<byte[]> getBreadcrumbMvtTile(
            @PathVariable int z,
            @PathVariable int x,
            @PathVariable int y,
            @RequestParam(required = false) String surveySessionId) {
        
        logger.info("Received MVT tile request for Breadcrumb - z: {}, x: {}, y: {}, surveySessionId: {}", z, x, y, surveySessionId);
        
        // Validate required parameter
        if (surveySessionId == null || surveySessionId.trim().isEmpty()) {
            logger.warn("MVT tile request rejected - missing surveySessionId parameter");
            return ResponseEntity.badRequest().build();
        }

        // Validate tile coordinates
        if (z < 0 || z > 20) {
            logger.warn("MVT tile request rejected - invalid zoom level: {} (must be 0-20)", z);
            return ResponseEntity.badRequest().build();
        }

        int maxTile = (int) Math.pow(2, z);
        if (x < 0 || x >= maxTile || y < 0 || y >= maxTile) {
            logger.warn("MVT tile request rejected - invalid tile coordinates: x={}, y={} (max: {})", x, y, maxTile - 1);
            return ResponseEntity.badRequest().build();
        }

        long startTime = System.currentTimeMillis();
        try {
            logger.debug("Executing MVT tile query for Breadcrumb - z={}, x={}, y={}, surveySessionId={}", z, x, y, surveySessionId);
            byte[] tileData = streamingService.getBreadcrumbMvtTile(z, x, y, surveySessionId);
            long executionTime = System.currentTimeMillis() - startTime;
            
            if (tileData == null || tileData.length == 0) {
                logger.info("MVT tile query returned empty result for Breadcrumb - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms", 
                    z, x, y, surveySessionId, executionTime);
                return ResponseEntity.noContent().build();
            }
            
            logger.info("MVT tile query successful for Breadcrumb - z: {}, x: {}, y: {}, surveySessionId: {}, tileSize: {} bytes, executionTime: {}ms", 
                z, x, y, surveySessionId, tileData.length, executionTime);
            
            return ResponseEntity.ok()
                    .header("Content-Type", "application/vnd.mapbox-vector-tile")
                    .body(tileData);
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            logger.error("Error retrieving MVT tile for Breadcrumb - z: {}, x: {}, y: {}, surveySessionId: {}, executionTime: {}ms, error: {}", 
                z, x, y, surveySessionId, executionTime, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping(value = "/", produces = "text/html")
    public ResponseEntity<String> getViewer() {
        try {
            // Try to read from the project root
            String htmlContent = new String(Files.readAllBytes(Paths.get("view-mvt-tiles.html")));
            return ResponseEntity.ok()
                    .header("Content-Type", "text/html")
                    .body(htmlContent);
        } catch (IOException e) {
            logger.error("Error reading HTML file", e);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("<html><body><h1>Viewer HTML file not found</h1><p>Please ensure view-mvt-tiles.html is in the project root directory.</p></body></html>");
        }
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleException(Exception e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("Internal server error", e.getMessage()));
    }
}

