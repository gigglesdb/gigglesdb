FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /probe
RUN mvn -B org.apache.maven.plugins:maven-dependency-plugin:3.6.1:copy -Dartifact=org.postgresql:postgresql:42.7.3 -DoutputDirectory=/probe/lib -DremoteRepositories=https://repo.maven.apache.org/maven2
COPY JdbcCompatProbe.java .
RUN mkdir classes && javac -encoding UTF-8 -cp lib/postgresql-42.7.3.jar -d classes JdbcCompatProbe.java

FROM eclipse-temurin:21-jre
WORKDIR /probe
COPY --from=build /probe/lib /probe/lib
COPY --from=build /probe/classes /probe/classes
ENTRYPOINT ["java", "-cp", "/probe/classes:/probe/lib/postgresql-42.7.3.jar", "JdbcCompatProbe"]
