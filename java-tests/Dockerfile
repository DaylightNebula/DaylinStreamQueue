# build from the repository root: docker build -f java-tests/Dockerfile --build-arg PROJECT=consumer .
FROM gradle:9.9.2-jdk25 AS build
ARG PROJECT
WORKDIR /app
COPY daylin-sq-core daylin-sq-core
COPY daylin-sq-client daylin-sq-client
COPY java-tests/${PROJECT} java-tests/${PROJECT}
WORKDIR /app/java-tests/${PROJECT}
RUN gradle installDist --no-daemon && mv build/install/${PROJECT} /dist && mv /dist/bin/${PROJECT} /dist/bin/start

FROM eclipse-temurin:25-jre
COPY --from=build /dist /dist
ENTRYPOINT ["/dist/bin/start"]
