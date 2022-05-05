# Build the functions code ...
FROM maven:3.8.1-jdk-11 AS builder
COPY src /usr/src/app/src
COPY pom.xml /usr/src/app
RUN mvn -f /usr/src/app/pom.xml clean package

# ... and run the web server!
FROM openjdk:11
WORKDIR /
COPY --from=builder /usr/src/app/target/query-functions-app*jar-with-dependencies.jar query-functions-app.jar
#COPY target/query-functions-app*jar-with-dependencies.jar query-functions-app.jar
EXPOSE 1108
CMD java -jar query-functions-app.jar