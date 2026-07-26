FROM eclipse-temurin:17-jre
RUN useradd --system --uid 10001 repair
WORKDIR /app
COPY target/production-data-repair-engine.jar /app/repair-engine.jar
USER 10001
ENTRYPOINT ["java","-jar","/app/repair-engine.jar"]
