FROM gcr.io/distroless/java21-debian12
WORKDIR /app
COPY bill-summary-pipeline/target/scala-3.7.3/bill-summary-pipeline.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
