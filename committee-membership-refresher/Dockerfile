FROM gcr.io/distroless/java21-debian12
WORKDIR /app
COPY committee-membership-refresher/target/scala-3.7.3/committee-membership-refresher.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
