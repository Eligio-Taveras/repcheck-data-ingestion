# Multi-stage build for repcheck-data-ingestion
# Stage 1: Build with sbt-assembly
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY . .
RUN ./sbt repcheckdataingestion/assembly

# Stage 2: Runtime with Google Distroless
FROM gcr.io/distroless/java21-debian12
WORKDIR /app
COPY --from=build /app/repcheck-data-ingestion/target/scala-3.4.1/*-assembly-*.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
