FROM maven:onbuild-alpine
EXPOSE 8080

CMD ["java","-jar","target/cajrr-2.0-SNAPSHOT.jar", "server", "/etc/cajrr/config.yml"]
