FROM gcr.io/distroless/java21-debian12
WORKDIR /app
COPY lis-mapping-refresher/target/scala-3.7.3/lis-mapping-refresher.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
