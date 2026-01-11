# # Use an official Flink image as the base
# FROM flink:1.19.1-scala_2.12
#
# # Copy your application JAR into the recommended directory
# # The /opt/flink/usrlib directory is for user-supplied application JARs
# COPY target/quickstart-0.1.jar /opt/flink/examples/quickstart-0.1.jar
#
# # (Optional) Copy any additional connector dependencies if needed
# COPY target/flink-connector-files-1.19.1.jar /opt/flink/usrlib/flink-connector-files-1.19.1.jar
FROM openjdk:17.0.2-slim
ARG JAR_FILE=target/quickstart-*.jar
COPY ${JAR_FILE} app.jar
COPY target/quickstart-*.jar /opt/flink/usrlib/quickstart-*.jar
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /app.jar ${APP_ARGS}"]