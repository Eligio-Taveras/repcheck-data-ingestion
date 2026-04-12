FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY . .
RUN ./sbt billTextAvailabilityChecker/assembly

FROM gcr.io/distroless/java21-debian12
WORKDIR /app
COPY --from=build /app/bill-text-availability-checker/target/scala-3.7.3/bill-text-availability-checker.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
