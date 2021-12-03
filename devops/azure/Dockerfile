FROM community.opengroup.org:5555/osdu/platform/deployment-and-operations/base-containers-azure/alpine-zulu8:0.0.3
VOLUME /tmp
ARG JAR_FILE=./provider/storage-azure/target/storage-azure-0.11.0-spring-boot.jar
ARG APPLICATIONINSIGHTS_INSTRUMENTATION_KEY
COPY ${JAR_FILE} app.jar
ENV DEFAULT_JVM_OPTS="-XX:InitialRAMPercentage=25.0 -XX:MaxRAMPercentage=50.0 -XX:+HeapDumpOnOutOfMemoryError"
ENV JAVA_OPTS="${DEFAULT_JVM_OPTS} -javaagent:/opt/agents/applicationinsights-agent.jar ${JAVA_OPTS}"

ENTRYPOINT exec java ${JAVA_OPTS} -jar /app.jar
