FROM eclipse-temurin:17-jdk-alpine
WORKDIR /app
COPY Server.java .
RUN javac Server.java
EXPOSE 6379
CMD ["java", "Server"]