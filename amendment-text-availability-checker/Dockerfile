FROM gcr.io/distroless/java21-debian12
WORKDIR /app
COPY amendment-text-availability-checker/target/scala-3.7.3/amendment-text-availability-checker.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
