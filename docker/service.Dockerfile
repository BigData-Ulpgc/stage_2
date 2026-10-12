# Template image for every Java microservice (multi-stage build).
#
# Build from the REPOSITORY ROOT, choosing the Maven module:
#   docker build -f docker/service.Dockerfile --build-arg MODULE=search-service -t stage2/search-service .
# In docker-compose.yml:
#   build: { context: ., dockerfile: docker/service.Dockerfile, args: { MODULE: search-service } }
#
# Requirement for each module: its pom.xml sets <finalName>${project.artifactId}</finalName>
# and builds a fat jar (all dependencies inside), so the jar is target/<module>.jar.

# ---- Stage 1: compile with Maven (this image is discarded afterwards) ----
FROM maven:3.9-eclipse-temurin-17 AS build
ARG MODULE
WORKDIR /src
COPY . .
# -pl: only this module; -am: also the modules it depends on (e.g. common)
RUN mvn -q -B -pl "${MODULE}" -am package -DskipTests

# ---- Stage 2: runtime image with only the JRE and the jar ----
FROM eclipse-temurin:17-jre
ARG MODULE
WORKDIR /app
COPY --from=build /src/${MODULE}/target/${MODULE}.jar /app/app.jar
# Heap and other JVM flags are set per service in docker-compose.yml
ENV JAVA_OPTS=""
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
