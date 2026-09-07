FROM eclipse-temurin:17-jdk-alpine
WORKDIR /app
COPY . .
RUN javac Server.java
EXPOSE 8080 6379
CMD ["java", "Server"]