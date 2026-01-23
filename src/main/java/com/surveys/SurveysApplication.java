package com.surveys;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SurveysApplication {

    public static void main(String[] args) {
        SpringApplication.run(SurveysApplication.class, args);
        System.out.println("Server is running on port " + System.getenv().getOrDefault("PORT", "3000"));
        System.out.println("Streaming APIs available at:");
        System.out.println("  - http://localhost:" + System.getenv().getOrDefault("PORT", "3000") + "/api/fov");
        System.out.println("  - http://localhost:" + System.getenv().getOrDefault("PORT", "3000") + "/api/breadcrumb");
        System.out.println("  - http://localhost:" + System.getenv().getOrDefault("PORT", "3000") + "/api/lisa");
    }
}

