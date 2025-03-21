FROM openjdk:11-jdk

WORKDIR /app

# Copy the compiled classes
COPY ./compiled/OrderService/ /app/

# Copy the PostgreSQL JDBC driver
COPY ./lib/postgresql-42.7.5.jar /app/lib/

# Copy the HikariCP JAR file into the container
COPY ./lib/HikariCP-6.2.1.jar /app/lib/
COPY ./lib/slf4j-api-2.0.17.jar /app/lib/
COPY ./lib/slf4j-simple-2.0.17.jar /app/lib/
COPY ./lib/jedis-4.2.3.jar /app/lib/
COPY ./lib/commons-pool2-2.11.1.jar /app/lib/
COPY ./lib/gson-2.8.6.jar /app/lib/

# Expose the port
EXPOSE 8080

# Command to run the application
CMD ["java", "-cp", "/app:/app/lib/*", "OrderService", "config.json"]