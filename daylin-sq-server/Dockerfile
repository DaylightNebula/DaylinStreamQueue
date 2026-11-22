# build from the repository root: docker build -f daylin-sq-server/Dockerfile .
FROM gradle:9.9.2-jdk25 AS build
WORKDIR /app
COPY daylin-sq-core daylin-sq-core
COPY daylin-sq-server daylin-sq-server
WORKDIR /app/daylin-sq-server
RUN gradle bootJar --no-daemon

FROM eclipse-temurin:25-jre
COPY --from=build /app/daylin-sq-server/build/libs/*.jar /app.jar
EXPOSE 5000
ENTRYPOINT ["java", "-jar", "/app.jar"]
