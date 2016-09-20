FROM maven:onbuild-alpine
EXPOSE 8080

CMD ["java","-Ddw.host=cassandra", "-jar","target/cajrr-1.0-SNAPSHOT.jar", "server", "pkg/config.yml"]