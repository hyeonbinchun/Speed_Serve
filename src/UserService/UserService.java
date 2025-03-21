import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * UserService is responsible for handling all user-related operations,
 * including get user data, create user, update user, and delete user.
 * This version uses PostgreSQL database instead of text file storage.
 */
public class UserService {
    private static String DB_URL;
    private static String DB_USER;
    private static String DB_PASSWORD;
    private static int USER_SERVICE_PORT;

    // Database connection pool
    private static HikariDataSource dataSource;

    /**
     * The entry point for the UserService application.
     * 
     * This method loads the configuration from the `config.json` file, starts the
     * database if needed,
     * and sets up an HTTP UserService server to listen for requests on the
     * specified port.
     * 
     * @param args the command-line arguments: config.json file
     * @throws IOException if there is an error reading the configuration file or
     *                     starting the server
     */
    public static void main(String[] args) throws IOException {
        // Check if a config file is provided
        if (args.length < 1) {
            System.out.println("Please provide the path to the config file.");
            return;
        }

        // Use the first argument as the config file path
        String configFilePath = args[0];

        Map<String, String> config = loadConfig(configFilePath);
        USER_SERVICE_PORT = Integer.parseInt(config.get("UserService.port"));
        DB_URL = config.get("Database.url");
        DB_USER = config.get("Database.user");
        DB_PASSWORD = config.get("Database.password");

        // Initialize the database connection pool
        initializeConnectionPool(
            DB_URL,
            DB_USER,
            DB_PASSWORD
        );

        HttpServer server = HttpServer.create(new InetSocketAddress(USER_SERVICE_PORT), 0);
        server.setExecutor(Executors.newFixedThreadPool(10));
        server.createContext("/user", new UserHandler());
        server.start();
        System.out.println("UserService is running on port " + USER_SERVICE_PORT);
    }

    /**
     * Loads configuration from the specified JSON file. (Assume copy of config.json
     * is in the same directory as UserService)
     *
     * @param filePath The path to the JSON configuration file.
     * @return A map containing the configuration key-value pairs.
     * @throws IOException If an I/O error occurs while reading the file.
     */
    private static Map<String, String> loadConfig(String configFilePath) throws IOException {
        Map<String, String> configMap = new HashMap<>();

        try {
            // Read the config file
            String content = new String(Files.readAllBytes(Paths.get(configFilePath)));

            // Very basic JSON parsing - look for the Database section
            int databaseStart = content.indexOf("\"Database\"");
            if (databaseStart > 0) {
                // Find URL within Database section
                int urlStart = content.indexOf("\"url\"", databaseStart);
                if (urlStart > 0) {
                    int valueStart = content.indexOf(":", urlStart) + 1;
                    int valueEnd = content.indexOf(",", valueStart);
                    if (valueEnd < 0) { // In case this is the last property
                        valueEnd = content.indexOf("}", valueStart);
                    }
                    String urlValue = content.substring(valueStart, valueEnd).trim();
                    // Remove quotes
                    urlValue = urlValue.replaceAll("\"", "");
                    configMap.put("Database.url", urlValue);
                }

                // Find user within Database section
                int userStart = content.indexOf("\"user\"", databaseStart);
                if (userStart > 0) {
                    int valueStart = content.indexOf(":", userStart) + 1;
                    int valueEnd = content.indexOf(",", valueStart);
                    if (valueEnd < 0) {
                        valueEnd = content.indexOf("}", valueStart); // Handles the last value
                    }
                    String userValue = content.substring(valueStart, valueEnd).trim();
                    userValue = userValue.replaceAll("\"", "");
                    configMap.put("Database.user", userValue);
                }

                // Find password within Database section
                int passwordStart = content.indexOf("\"password\"", databaseStart);
                if (passwordStart > 0) {
                    int valueStart = content.indexOf(":", passwordStart) + 1;
                    int valueEnd = content.indexOf("}", valueStart); // Find the closing brace
                    String passwordValue = content.substring(valueStart, valueEnd).trim();

                    // Remove quotes and any trailing whitespace
                    passwordValue = passwordValue.replaceAll("\"", "").trim();

                    configMap.put("Database.password", passwordValue);
                }
            }

            // Similarly parse UserService section for port
            int userServiceStart = content.indexOf("\"UserService\"");
            if (userServiceStart > 0) {
                int portStart = content.indexOf("\"port\"", userServiceStart);
                if (portStart > 0) {
                    int valueStart = content.indexOf(":", portStart) + 1;
                    int valueEnd = content.indexOf(",", valueStart);
                    if (valueEnd < 0) {
                        valueEnd = content.indexOf("}", valueStart); // Handles the last value
                    }
                    String portValue = content.substring(valueStart, valueEnd).trim();
                    configMap.put("UserService.port", portValue);
                }
            }

        } catch (Exception e) {
            System.out.println("Error loading config: " + e.getMessage());
            e.printStackTrace();
        }

        return configMap;
    }

     /**
     * Initializes the database connection pool.
     * 
     * @param url Database URL
     * @param user Database username
     * @param password Database password
     */
    private static void initializeConnectionPool(String url, String user, String password) {
        try {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url);
            config.setUsername(user);
            config.setPassword(password);
            config.setMaximumPoolSize(10);
            config.setMinimumIdle(5);
            config.setIdleTimeout(300000);
            config.setConnectionTimeout(30000);
            config.setPoolName("UserServiceConnectionPool");
            config.setAutoCommit(true);
            
            // Add more configurations as needed
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            
            dataSource = new HikariDataSource(config);
            
            // Test the connection
            try (Connection conn = getConnection()) {
                System.out.println("Successfully connected to the PostgreSQL database with connection pool.");
            }
        } catch (SQLException e) {
            System.err.println("Database connection pool initialization error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Gets a database connection from the connection pool.
     * 
     * @return A connection to the PostgreSQL database.
     * @throws SQLException If a database access error occurs.
     */
    static Connection getConnection() throws SQLException {
        if (dataSource == null) {
            throw new SQLException("Connection pool has not been initialized");
        }
        return dataSource.getConnection();
    }

    /**
     * Handles incoming HTTP requests for user-related operations.
     * Routes the request to the appropriate handler method based on the HTTP method
     * (GET or POST).
     *
     * @param exchange The HTTP exchange object containing the request and response.
     * @throws IOException If an I/O error occurs during request handling.
     */
    static class UserHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String path = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();
                if ("POST".equals(method)) {
                    handlePost(exchange);
                } else if ("GET".equals(method)) {
                    handleGet(exchange, path);
                } else {
                    // Handle unsupported methods
                    sendResponse(exchange, 400, "{}");
                }
            } catch (Exception e) {
                e.printStackTrace();
                sendResponse(exchange, 500, "{\"status\": \"Internal Server Error\"}");
            } finally {
                exchange.close();
            }
        }

        /**
         * Handles POST requests for creating, updating, or deleting a user.
         *
         * @param exchange The HTTP exchange object containing the request and response.
         * @throws IOException If an I/O error occurs during request handling.
         */
        private void handlePost(HttpExchange exchange) throws IOException {
            try {
                String requestBody = getRequestBody(exchange);
                Map<String, String> requestMap = parseRequest(requestBody);
                String command = requestMap.get("command");
                String response;
                int statusCode;

                try {
                    switch (command.toLowerCase()) {
                        case "create":
                            response = handleCreate(requestMap);
                            statusCode = getStatusCodeFromResponse(response);
                            break;
                        case "update":
                            response = handleUpdate(requestMap);
                            statusCode = getStatusCodeFromResponse(response);
                            break;
                        case "delete":
                            response = handleDelete(requestMap);
                            statusCode = getStatusCodeFromResponse(response);
                            break;
                        default:
                            sendResponse(exchange, 400, "{}");
                            return;
                    }
                    if (statusCode == 200) {
                        sendResponse(exchange, statusCode, response);
                    } else {
                        sendResponse(exchange, statusCode, "{}");
                    }
                } catch (NumberFormatException e) {
                    sendResponse(exchange, 400, "{}");
                }
            } catch (Exception e) {
                sendResponse(exchange, 500, "{\"status\": \"Internal Server Error\"}");
            }
        }

        /**
         * Handles GET requests for retrieving user details.
         *
         * @param exchange The HTTP exchange object containing the request and response.
         * @param path     The request path that contain the user ID.
         * @throws IOException If an I/O error occurs during request handling.
         */
        private void handleGet(HttpExchange exchange, String path) throws IOException {
            try {
                String[] pathParts = path.split("/");
                try {
                    int userId = Integer.parseInt(pathParts[2]);
                    String userJson = getUserJson(userId);
                    if (userJson != null) {
                        sendResponse(exchange, 200, userJson);
                    } else { // 404: user not found
                        sendResponse(exchange, 404, "{}");
                    }
                } catch (NumberFormatException e) {
                    sendResponse(exchange, 400, "{}");
                }
            } catch (Exception e) {
                sendResponse(exchange, 500, "{\"status\": \"Internal Server Error\"}");
            }
        }

        /**
         * Retrieves the user details as a JSON string based on the user ID.
         *
         * @param id The ID of the user to retrieve.
         * @return A JSON string containing the user details, or null if the user is not
         *         found.
         * @throws IOException If an I/O error occurs while reading the database file.
         */
        private String getUserJson(int id) throws SQLException {
            String sql = "SELECT id, username, email, password FROM users WHERE id = ?";

            try (Connection conn = getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setInt(1, id);

                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        return String.format(
                                "{\"id\": %d, \"username\": \"%s\", \"email\": \"%s\", \"password\": \"%s\"}",
                                rs.getInt("id"), rs.getString("username"),
                                rs.getString("email"), rs.getString("password"));
                    }
                }
            }
            return null;
        }

        /**
         * Hashes the password using SHA-256.
         *
         * @param password The plain text password to hash
         * @return The hashed password as a hex string
         */
        private String hashPassword(String password) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] encodedHash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
                return bytesToHex(encodedHash);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Error hashing password", e);
            }
        }

        /**
         * Converts a byte array to a hex string representation.
         *
         * @param hash The byte array to convert
         * @return A hex string representation of the byte array
         */
        private String bytesToHex(byte[] hash) {
            StringBuilder hexString = new StringBuilder(2 * hash.length);
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1)
                    hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        }

        /**
         * Handles the user creation process.
         *
         * @param request A map containing the request parameters.
         * @return A JSON string representing the created user or an error message.
         * @throws SQLException If a database access error occurs.
         */
        private String handleCreate(Map<String, String> request) throws SQLException {
            // Validate required fields
            if (!request.containsKey("id") || !request.containsKey("username") ||
                    !request.containsKey("email") || !request.containsKey("password")) {
                return "{\"status_code\": \"400\"}"; // Missing required fields
            }
            try {
                int id = Integer.parseInt(request.get("id"));
                String username = request.get("username");
                String email = request.get("email");
                String password = request.get("password");
                if (id <= 0 || username == null || username.isEmpty() ||
                        email == null || email.isEmpty() ||
                        password == null || password.isEmpty()) {
                    return "{\"status_code\": \"400\"}"; // Invalid field values
                }
                if (isInteger(username) || isInteger(email) || isInteger(password)) {
                    return "{\"status_code\": \"400\"}";
                }
                if (userExists(id)) {
                    return "{\"status_code\": \"409\"}";
                }
                // Hash the password before storing
                String hashedPassword = hashPassword(password);
                String sql = "INSERT INTO users (id, username, email, password) VALUES (?, ?, ?, ?)";

                try (Connection conn = getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(sql)) {

                    pstmt.setInt(1, id);
                    pstmt.setString(2, username);
                    pstmt.setString(3, email);
                    pstmt.setString(4, hashedPassword);

                    int rowsAffected = pstmt.executeUpdate();
                    if (rowsAffected > 0) {
                        return String.format(
                                "{\"id\": %d, \"username\": \"%s\", \"email\": \"%s\", \"password\": \"%s\"}",
                                id, username, email, hashedPassword);
                    } else {
                        return "{\"status_code\": \"400\"}";
                    }
                }
            } catch (NumberFormatException e) {
                return "{\"status_code\": \"400\"}";
            }
        }

        /**
         * Handles the user update process.
         *
         * @param request A map containing the request parameters.
         * @return A JSON string representing the updated user or an error message.
         * @throws SQLException If a database access error occurs.
         */
        private String handleUpdate(Map<String, String> request) throws SQLException {
            if (!request.containsKey("id")) {
                return "{\"status_code\": \"400\"}";
            }

            try {
                int id = Integer.parseInt(request.get("id"));

                if (!userExists(id)) {
                    return "{\"status_code\": \"404\"}";
                }

                // First get the current user data
                String currentUserSql = "SELECT username, email, password FROM users WHERE id = ?";
                String updatedUsername = null;
                String updatedEmail = null;
                String updatedPassword = null;

                try (Connection conn = getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(currentUserSql)) {

                    pstmt.setInt(1, id);

                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            updatedUsername = request.containsKey("username") ? request.get("username")
                                    : rs.getString("username");
                            updatedEmail = request.containsKey("email") ? request.get("email") : rs.getString("email");

                            // Hash the password if a new one is provided
                            if (request.containsKey("password")) {
                                updatedPassword = hashPassword(request.get("password"));
                            } else {
                                updatedPassword = rs.getString("password");
                            }

                            // Validate updated fields
                            if (updatedUsername == null || updatedUsername.isEmpty() ||
                                    updatedEmail == null || updatedEmail.isEmpty()) {
                                return "{\"status_code\": \"400\"}";
                            }

                            if (isInteger(updatedUsername) || isInteger(updatedEmail)) {
                                return "{\"status_code\": \"400\"}";
                            }
                        } else {
                            return "{\"status_code\": \"404\"}";
                        }
                    }
                }

                // Update the user data
                String updateSql = "UPDATE users SET username = ?, email = ?, password = ? WHERE id = ?";

                try (Connection conn = getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(updateSql)) {

                    pstmt.setString(1, updatedUsername);
                    pstmt.setString(2, updatedEmail);
                    pstmt.setString(3, updatedPassword);
                    pstmt.setInt(4, id);

                    int rowsAffected = pstmt.executeUpdate();

                    if (rowsAffected > 0) {
                        return String.format(
                                "{\"id\": %d, \"username\": \"%s\", \"email\": \"%s\", \"password\": \"%s\"}",
                                id, updatedUsername, updatedEmail, updatedPassword);
                    } else {
                        return "{\"status_code\": \"400\"}";
                    }
                }
            } catch (NumberFormatException e) {
                return "{\"status_code\": \"400\"}";
            }
        }

        /**
         * Handles the user deletion process.
         *
         * @param request A map containing the request parameters.
         * @return A JSON string indicating the success or failure of the deletion.
         * @throws SQLException If a database access error occurs.
         */
        private String handleDelete(Map<String, String> request) throws SQLException {
            if (!request.containsKey("id") || !request.containsKey("username") ||
                    !request.containsKey("email") || !request.containsKey("password")) {
                return "{\"status_code\": \"400\"}";
            }

            try {
                int id = Integer.parseInt(request.get("id"));

                if (!userExists(id)) {
                    return "{\"status_code\": \"404\"}";
                }

                String username = request.get("username");
                String email = request.get("email");
                String hashedPassword = hashPassword(request.get("password"));

                // Verify credentials before deletion
                String verifySql = "SELECT id FROM users WHERE id = ? AND username = ? AND email = ? AND password = ?";

                try (Connection conn = getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(verifySql)) {

                    pstmt.setInt(1, id);
                    pstmt.setString(2, username);
                    pstmt.setString(3, email);
                    pstmt.setString(4, hashedPassword);

                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            // Credentials match, proceed with deletion
                            String deleteSql = "DELETE FROM users WHERE id = ?";

                            try (PreparedStatement deleteStmt = conn.prepareStatement(deleteSql)) {
                                deleteStmt.setInt(1, id);

                                int rowsAffected = deleteStmt.executeUpdate();

                                if (rowsAffected > 0) {
                                    return "{}";
                                }
                            }
                        } else {
                            // Credentials don't match
                            return "{\"status_code\": \"401\"}";
                        }
                    }
                }

                return "{\"status_code\": \"400\"}";
            } catch (NumberFormatException e) {
                return "{\"status_code\": \"400\"}";
            }
        }

        /**
         * Retrieves the raw request body from the HTTP exchange.
         *
         * @param exchange The HTTP exchange object containing the request body.
         * @return The request body as a string.
         * @throws IOException If an I/O error occurs while reading the request body.
         */
        private String getRequestBody(HttpExchange exchange) throws IOException {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                StringBuilder requestBody = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    requestBody.append(line);
                }
                return requestBody.toString();
            }
        }

        /**
         * Parses the request body into a map of key-value pairs.
         *
         * @param requestBody The raw request body.
         * @return A map containing the parsed key-value pairs.
         */
        private Map<String, String> parseRequest(String requestBody) {
            Map<String, String> requestMap = new HashMap<>();

            // Remove outer curly braces and trim whitespace
            requestBody = requestBody.trim();
            if (requestBody.startsWith("{") && requestBody.endsWith("}")) {
                requestBody = requestBody.substring(1, requestBody.length() - 1);
            }

            // Split by commas, but be cautious of values with commas
            String[] pairs = requestBody.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            for (String pair : pairs) {
                String[] keyValue = pair.split(":", 2);
                if (keyValue.length == 2) {
                    // Remove quotes and trim spaces
                    String key = keyValue[0].trim().replaceAll("^\"|\"$", "");
                    String value = keyValue[1].trim().replaceAll("^\"|\"$", "");
                    requestMap.put(key, value);
                }
            }
            return requestMap;
        }

        /**
         * Sends the HTTP response with the specified status code and response body.
         *
         * @param exchange   The HTTP exchange object for sending the response.
         * @param statusCode The HTTP status code to send.
         * @param response   The response body.
         * @throws IOException If an I/O error occurs while sending the response.
         */
        private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
            byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);

            // Set the Content-Type hearder to application/json
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }

        /**
         * Returns the appropriate HTTP status code based on the response content.
         *
         * @param response The response content.
         * @return The HTTP status code.
         */
        private int getStatusCodeFromResponse(String response) {
            if (response.contains("\"status_code\": \"400\""))
                return 400;
            if (response.contains("\"status_code\": \"401\""))
                return 401;
            if (response.contains("\"status_code\": \"404\""))
                return 404;
            if (response.contains("\"status_code\": \"409\""))
                return 409;
            return 200;
        }

        /**
         * Checks if the given string can be parsed into an integer.
         *
         * This method attempts to parse the string to an integer using
         * `Integer.parseInt()`.
         * If the string is successfully parsed, the method returns `true`. Otherwise,
         * if the
         * string cannot be parsed into an integer (e.g., it's not a valid number), it
         * returns `false`.
         *
         * @param str the string to be checked
         * @return {@code true} if the string is a valid integer, {@code false}
         *         otherwise
         */
        private static boolean isInteger(String str) {
            try {
                Integer.parseInt(str); // Try to parse the string to an integer
                return true; // It was an integer
            } catch (NumberFormatException e) {
                return false; // Not an integer
            }
        }

        /**
         * Checks if a user with the given ID exists in the database.
         *
         * @param id The ID of the user to check.
         * @return true if the user exists, false otherwise.
         * @throws SQLException If a database access error occurs.
         */
        private static boolean userExists(int id) throws SQLException {
            String sql = "SELECT 1 FROM users WHERE id = ?";

            try (Connection conn = getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setInt(1, id);

                try (ResultSet rs = pstmt.executeQuery()) {
                    return rs.next();
                }
            }
        }
    }
}