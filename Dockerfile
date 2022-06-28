FROM artifactory.notebookbr.corp/docker-hub/openjdk:11
VOLUME /tmp
COPY target/*.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]