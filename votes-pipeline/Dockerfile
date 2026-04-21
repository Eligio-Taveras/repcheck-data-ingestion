FROM gcr.io/distroless/java21-debian12
WORKDIR /app
COPY votes-pipeline/target/scala-3.7.3/votes-pipeline.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
