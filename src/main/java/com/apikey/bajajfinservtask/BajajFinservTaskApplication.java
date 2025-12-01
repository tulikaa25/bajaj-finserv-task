package com.apikey.bajajfinservtask;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDate;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class BajajFinservTaskApplication {

    private static final Logger log = LoggerFactory.getLogger(BajajFinservTaskApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(BajajFinservTaskApplication.class, args);
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder.build();
    }

    @Bean
    public ApplicationRunner applicationRunner(RestTemplate restTemplate, ObjectMapper objectMapper) {
        return new ApplicationRunner() {
            @Override
            public void run(ApplicationArguments args) throws Exception {
                log.info("Application started. Executing startup logic...");

                // 1. Send initial POST request to generateWebhook
                String generateWebhookUrl = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";
                Map<String, String> requestBody = new HashMap<>();
                requestBody.put("name", "Tulika Basu");
                requestBody.put("regNo", "22BCE11161");
                requestBody.put("email", "tulikabasu2022@vitbhopal.ac.in");

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<Map<String, String>> request = new HttpEntity<>(requestBody, headers);

                try {
                    log.info("Sending POST request to {} with body: {}", generateWebhookUrl, requestBody);
                    ResponseEntity<String> response = restTemplate.postForEntity(generateWebhookUrl, request, String.class);
                    log.info("Received response from generateWebhook: {}", response.getBody());

                    if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                        JsonNode jsonResponse = objectMapper.readTree(response.getBody());
                        String webhookUrl = jsonResponse.has("webhook") ? jsonResponse.get("webhook").asText() : null;
                        String accessToken = jsonResponse.has("accessToken") ? jsonResponse.get("accessToken").asText() : null;

                        if (webhookUrl != null && accessToken != null) {
                            log.info("Webhook URL: {}", webhookUrl);
                            log.info("Access Token: {}", accessToken);

                            // 2. SQL Problem Solution (QUESTION 1)
                            String finalSqlQuery = "WITH EmployeeSalaries AS (SELECT p.EMP_ID, SUM(p.AMOUNT) AS SALARY FROM PAYMENTS p WHERE DAY(p.PAYMENT_TIME) != 1 GROUP BY p.EMP_ID), RankedEmployeeSalaries AS (SELECT d.DEPARTMENT_NAME, es.SALARY, CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) AS EMPLOYEE_NAME, TIMESTAMPDIFF(YEAR, e.DOB, CURDATE()) AS AGE, ROW_NUMBER() OVER (PARTITION BY d.DEPARTMENT_ID ORDER BY es.SALARY DESC) as rn FROM EmployeeSalaries es JOIN EMPLOYEE e ON es.EMP_ID = e.EMP_ID JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID) SELECT DEPARTMENT_NAME, SALARY, EMPLOYEE_NAME, AGE FROM RankedEmployeeSalaries WHERE rn = 1;";

                            // 3. Send SQL solution to the received webhook
                            HttpHeaders webhookHeaders = new HttpHeaders();
                            webhookHeaders.setContentType(MediaType.APPLICATION_JSON);
                            webhookHeaders.set("Authorization", accessToken);
                            Map<String, String> webhookRequestBody = new HashMap<>();
                            webhookRequestBody.put("finalQuery", finalSqlQuery);

                            HttpEntity<Map<String, String>> webhookRequest = new HttpEntity<>(webhookRequestBody, webhookHeaders);

                            log.info("Sending final SQL query to webhook: {} with Access Token: {}", webhookUrl, accessToken);
                           ResponseEntity<String> webhookResponse = restTemplate.postForEntity(webhookUrl, webhookRequest, String.class);
//                            String correctWebhook = "https://bfhldevapigw.healthrx.co.in/hiring/testWebhook";
//                            ResponseEntity<String> webhookResponse = restTemplate.postForEntity(correctWebhook, webhookRequest, String.class);

                            log.info("Received response from webhook: {}", webhookResponse.getBody());

                        } else {
                            log.error("Webhook URL or Access Token not found in the response.");
                        }
                    } else {
                        log.error("Failed to generate webhook. Status code: {}, Body: {}", response.getStatusCode(), response.getBody());
                    }

                } catch (Exception e) {
                    log.error("An error occurred during webhook generation or SQL submission: {}", e.getMessage(), e);
                } finally {
                    log.info("Application startup logic completed. Exiting application.");
                    // This is important to ensure the application exits after running the CommandLineRunner
                    System.exit(0); // Use System.exit(0) for a clean exit
                }
            }
        };
    }
}