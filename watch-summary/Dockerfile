# Multi-stage: build with a full Gradle+JDK image, run on a slim JRE only -
# the fat jar from shadowJar means the runtime stage needs no dependency
# resolution step at all.
FROM gradle:8.10-jdk21 AS build
WORKDIR /app
COPY build.gradle settings.gradle ./
COPY src ./src
RUN gradle shadowJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/build/libs/watch-summary-all.jar app.jar
COPY schemas ./schemas
CMD ["java", "-jar", "app.jar"]
