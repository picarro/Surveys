package com.surveys.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.surveys.dto.ErrorResponse;
import com.surveys.dto.HealthResponse;
import com.surveys.service.StreamingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
public class SurveysController {

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
            @RequestParam(required = false, defaultValue = "100000") int limit) {
        
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

        // Execute streaming in a separate thread
        executorService.execute(() -> {
            try {
                streamingService.streamFovData(surveySessionId, limit, emitter);
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

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleException(Exception e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("Internal server error", e.getMessage()));
    }
}

