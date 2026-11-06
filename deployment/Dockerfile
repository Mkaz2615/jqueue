# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /src

# Dependencies are resolved before the source is copied, so a change to application code
# reuses the cached dependency layer instead of re-downloading the whole graph every build.
COPY pom.xml ./
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q package -DskipTests

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
# distroless/java21 runs as a non-root user with no shell and no package manager, which
# removes most of what an attacker would reach for after a compromise.
FROM gcr.io/distroless/java21-debian12:nonroot

WORKDIR /app

COPY --from=build /src/target/jqueue.jar /app/jqueue.jar
COPY config /app/config

USER nonroot:nonroot

# 8080 public API, 9090 private metrics/health/dashboard.
EXPOSE 8080 9090

ENV JQUEUE_CONFIG_PATH=/app/config/config.local.yaml

# Size the heap from the container's memory limit rather than the host's.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/jqueue.jar"]
