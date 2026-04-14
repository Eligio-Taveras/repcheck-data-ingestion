FROM gcr.io/distroless/java21-debian12
WORKDIR /app
COPY bill-text-pipeline/target/scala-3.7.3/bill-text-pipeline.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
