FROM eclipse-temurin:21-alpine
MAINTAINER Matthias Schneider (matthias.schneider@ipa.fraunhofer.de)

COPY target/*-exec.jar /app/app.jar
COPY src/main/docker/startup.sh /app/startup.sh
RUN chmod +x /app/startup.sh

WORKDIR /app

ENTRYPOINT ["/bin/bash", "-c", "/app/startup.sh"]
