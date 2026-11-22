FROM gradle:9.9.2-jdk25 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY src/main src/main
RUN gradle bootJar --no-daemon

FROM eclipse-temurin:25-jre
COPY --from=build /app/build/libs/*.jar /app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app.jar"]
