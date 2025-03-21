FROM openjdk:11-jdk

WORKDIR /app

# Copy the compiled classes
COPY ./compiled/UserService/ /app/

# Copy the PostgreSQL JDBC driver
COPY ./lib/postgresql-42.7.5.jar /app/lib/

# Copy the HikariCP JAR file into the container
COPY ./lib/HikariCP-6.2.1.jar /app/lib/
COPY ./lib/slf4j-api-2.0.17.jar /app/lib/
COPY ./lib/slf4j-simple-2.0.17.jar /app/lib/
COPY ./lib/jedis-4.2.3.jar /app/lib/

# Expose the port
EXPOSE 8081

# Command to run the application with the correct classpath
CMD ["java", "-cp", "/app/lib/postgresql-42.7.5.jar:/app/lib/HikariCP-6.2.1.jar:/app/lib/slf4j-api-2.0.17.jar:/app/lib/slf4j-simple-2.0.17.jar:/app/lib/jedis-4.2.3.jar:/app", "UserService", "config.json"]