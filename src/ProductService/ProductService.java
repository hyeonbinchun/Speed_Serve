import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * ProductService is responsible for handling all product-related operations,
 * including get product data, create product, update product, delete product,
 * and decrease product quantity.
 */
public class ProductService {
    /**
     * Database connection properties.
     */
    private static String DB_URL;
    private static String DB_USER;
    private static String DB_PASSWORD;

    private static int PRODUCT_SERVICE_PORT;

    // Database connection pool
    private static HikariDataSource dataSource;

    /**
     * Main method to start the ProductService server.
     *
     * @param args Command-line arguments: config.json file
     * @throws IOException If an error occurs while starting the server.
     */
    public static void main(String[] args) throws IOException {
        // Check if a config file is provided
        if (args.length < 1) {
            System.out.println("Please provide the path to the config file.");
            return;
        }

        // Use the first argument as the config file path
        String configFilePath = args[0];

        // Load config.json
        Map<String, String> config = loadConfig(configFilePath);
        PRODUCT_SERVICE_PORT = Integer.parseInt(config.get("ProductService.port"));

        // Get database configuration
        DB_URL = config.get("Database.url");
        DB_USER = config.get("Database.user");
        DB_PASSWORD = config.get("Database.password");
        // Initialize the database connection pool
        initializeConnectionPool(
                DB_URL,
                DB_USER,
                DB_PASSWORD);

        HttpServer server = HttpServer.create(new InetSocketAddress(PRODUCT_SERVICE_PORT), 0);
        server.setExecutor(Executors.newFixedThreadPool(10));
        server.createContext("/product", new ProductHandler());
        server.start();
        System.out.println("ProductService is running on port " + PRODUCT_SERVICE_PORT);
    }

    /**
     * Loads configuration settings from a JSON file.
     *
     * @param filePath Path to the configuration file.
     * @return A map containing configuration key-value pairs.
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

            // Similarly parse ProductService section for port
            int productServiceStart = content.indexOf("\"ProductService\"");
            if (productServiceStart > 0) {
                int portStart = content.indexOf("\"port\"", productServiceStart);
                if (portStart > 0) {
                    int valueStart = content.indexOf(":", portStart) + 1;
                    int valueEnd = content.indexOf(",", valueStart);
                    if (valueEnd < 0) {
                        valueEnd = content.indexOf("}", valueStart); // Handles the last value
                    }
                    String portValue = content.substring(valueStart, valueEnd).trim();
                    configMap.put("ProductService.port", portValue);
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
     * @param url      Database URL
     * @param user     Database username
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
            config.setPoolName("ProductServiceConnectionPool");
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
     * Handles HTTP requests for managing products.
     */
    static class ProductHandler implements HttpHandler {
        /**
         * Handles incoming HTTP requests.
         *
         * @param exchange The HTTP exchange containing request and response details.
         * @throws IOException If an error occurs while handling the request.
         */
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
         * Handles POST requests for creating, updating, deleting, and decreasing
         * product quantities.
         *
         * @param exchange The HTTP exchange containing request and response details.
         * @throws IOException If an error occurs while handling the request.
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
                        case "decrease":
                            response = handleDecrease(requestMap);
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
         * Handles GET requests for retrieving product details.
         *
         * @param exchange The HTTP exchange containing request and response details.
         * @param path     The request path.
         * @throws IOException If an error occurs while handling the request.
         */
        private void handleGet(HttpExchange exchange, String path) throws IOException {
            try {
                String[] pathParts = path.split("/");
                try {
                    int productId = Integer.parseInt(pathParts[2]);
                    String productJson = getProductJson(productId);
                    if (productJson != null) {
                        sendResponse(exchange, 200, productJson);
                    } else {
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
         * Retrieves the product details as a JSON string based on the product ID.
         *
         * @param id The ID of the product to retrieve.
         * @return A JSON string containing the product details, or null if the product
         *         is not found.
         * @throws SQLException If a database access error occurs.
         */
        private String getProductJson(int id) throws SQLException {
            String sql = "SELECT id, name, description, price, quantity FROM products WHERE id = ?";

            try (Connection conn = getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setInt(1, id);

                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        int productId = rs.getInt("id");
                        String name = rs.getString("name");
                        String description = rs.getString("description");
                        float price = rs.getFloat("price");
                        int quantity = rs.getInt("quantity");

                        return String.format(
                                "{\"id\": %d, \"name\": \"%s\", \"description\": \"%s\", \"price\": %.2f, \"quantity\": %d}",
                                productId, name, description, price, quantity);
                    }
                }
            }
            return null;
        }

        /**
         * Handles the product decreament process.
         * decrease the quantity of product according to the order
         * 
         * @param request A map containing the request parameters.
         * @return A JSON string representing the decreament product or an error
         *         message.
         * @throws SQLException If a database access error occurs.
         */
        private String handleDecrease(Map<String, String> request) throws SQLException {
            // Validate required fields
            if (!request.containsKey("id") || !request.containsKey("quantity")) {
                return "{\"status_code\": \"400\"}";
            }

            try {
                int id = Integer.parseInt(request.get("id"));
                int decreaseAmount = Integer.parseInt(request.get("quantity"));

                if (decreaseAmount <= 0) {
                    return "{\"status_code\": \"400\"}"; // Invalid decrease amount
                }

                if (!productExists(id)) {
                    return "{\"status_code\": \"404\"}"; // Product not found
                }

                Connection conn = null;
                PreparedStatement checkStmt = null;
                PreparedStatement updateStmt = null;
                ResultSet rs = null;

                try {
                    conn = getConnection();
                    conn.setAutoCommit(false); // Start transaction

                    // Check current quantity
                    String checkSql = "SELECT quantity FROM products WHERE id = ?";
                    checkStmt = conn.prepareStatement(checkSql);
                    checkStmt.setInt(1, id);
                    rs = checkStmt.executeQuery();

                    if (rs.next()) {
                        int currentQuantity = rs.getInt("quantity");

                        // Check if we have enough stock
                        if (currentQuantity < decreaseAmount) {
                            return "{\"status_code\": \"400\"}"; // Not enough stock
                        }

                        // Update quantity
                        String updateSql = "UPDATE products SET quantity = quantity - ? WHERE id = ?";
                        updateStmt = conn.prepareStatement(updateSql);
                        updateStmt.setInt(1, decreaseAmount);
                        updateStmt.setInt(2, id);

                        int updated = updateStmt.executeUpdate();

                        if (updated > 0) {
                            conn.commit();
                            return "{}"; // Success
                        }
                    }

                    return "{\"status_code\": \"404\"}"; // Product not found
                } catch (SQLException e) {
                    if (conn != null) {
                        try {
                            conn.rollback();
                        } catch (SQLException ex) {
                            ex.printStackTrace();
                        }
                    }
                    throw e;
                } finally {
                    if (rs != null)
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    if (checkStmt != null)
                        try {
                            checkStmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    if (updateStmt != null)
                        try {
                            updateStmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    if (conn != null) {
                        try {
                            conn.setAutoCommit(true);
                            conn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (NumberFormatException e) {
                return "{\"status_code\": \"400\"}"; // Invalid number format
            }
        }

        /**
         * Handles the product creation process.
         *
         * @param request A map containing the request parameters.
         * @return A JSON string representing the created product or an error message.
         * @throws SQLException If a database access error occurs.
         */
        private String handleCreate(Map<String, String> request) throws SQLException {
            // Validate required fields
            if (!request.containsKey("id") || !request.containsKey("name") ||
                    !request.containsKey("description") || !request.containsKey("price")
                    || !request.containsKey("quantity")) {
                return "{\"status_code\": \"400\"}"; // Missing required fields
            }

            try {
                int id = Integer.parseInt(request.get("id"));

                String name = request.get("name");
                String description = request.get("description");
                Float price = Float.parseFloat(request.get("price"));
                Integer quantity = Integer.parseInt(request.get("quantity"));
                String priceStr = request.get("price");
                String quantityStr = request.get("quantity");

                if (id <= 0 || name == null || name.isEmpty() ||
                        description == null || description.isEmpty() ||
                        priceStr == null || priceStr.isEmpty() ||
                        quantityStr == null || quantityStr.isEmpty()) {
                    return "{\"status_code\": \"400\"}"; // Invalid field values
                }

                if (isInteger(name) || isInteger(description)) {
                    return "{\"status_code\": \"400\"}";
                }

                // Additional validation (e.g., price and quantity must be positive)
                if (price <= 0 || quantity <= 0) {
                    return "{\"status_code\": \"400\"}";
                }

                if (productExists(id)) {
                    return "{\"status_code\": \"409\"}";
                }

                String sql = "INSERT INTO products (id, name, description, price, quantity) VALUES (?, ?, ?, ?, ?)";

                try (Connection conn = getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(sql)) {

                    pstmt.setInt(1, id);
                    pstmt.setString(2, name);
                    pstmt.setString(3, description);
                    pstmt.setFloat(4, price);
                    pstmt.setInt(5, quantity);

                    int affectedRows = pstmt.executeUpdate();

                    if (affectedRows > 0) {
                        return String.format(
                                "{\"id\": %d, \"name\": \"%s\", \"description\": \"%s\", \"price\": \"%.2f\", \"quantity\": \"%s\"}",
                                id, name, description, price, quantity);
                    }
                }

                return "{\"status_code\": \"400\"}";
            } catch (NumberFormatException e) {
                return "{\"status_code\": \"400\"}";
            }
        }

        /**
         * Handles the product update process.
         *
         * @param request A map containing the request parameters.
         * @return A JSON string representing the updated product or an error message.
         * @throws SQLException If a database access error occurs.
         */
        private String handleUpdate(Map<String, String> request) throws SQLException {
            if (!request.containsKey("id")) {
                return "{\"status_code\": \"400\"}";
            }

            try {
                int id = Integer.parseInt(request.get("id"));

                if (!productExists(id)) {
                    return "{\"status_code\": \"404\"}";
                }

                // Get current product details
                String productSql = "SELECT name, description, price, quantity FROM products WHERE id = ?";
                String updatedName = null;
                String updatedDescription = null;
                Float updatedPrice = null;
                Integer updatedQuantity = null;

                try (Connection conn = getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(productSql)) {

                    pstmt.setInt(1, id);

                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            // Get current values to use if not updated
                            updatedName = request.containsKey("name") ? request.get("name") : rs.getString("name");
                            updatedDescription = request.containsKey("description") ? request.get("description")
                                    : rs.getString("description");
                            updatedPrice = request.containsKey("price") ? Float.parseFloat(request.get("price"))
                                    : rs.getFloat("price");
                            updatedQuantity = request.containsKey("quantity")
                                    ? Integer.parseInt(request.get("quantity"))
                                    : rs.getInt("quantity");
                        } else {
                            return "{\"status_code\": \"404\"}";
                        }
                    }
                }

                // Validate updated fields
                if (updatedName == null || updatedName.isEmpty() ||
                        updatedDescription == null || updatedDescription.isEmpty() ||
                        updatedPrice == null || updatedQuantity == null) {
                    return "{\"status_code\": \"400\"}";
                }

                if (isInteger(updatedName) || isInteger(updatedDescription)) {
                    return "{\"status_code\": \"400\"}";
                }

                // Additional validation (e.g., price and quantity must be positive)
                if (updatedPrice <= 0 || updatedQuantity <= 0) {
                    return "{\"status_code\": \"400\"}";
                }

                // Update the product
                String updateSql = "UPDATE products SET name = ?, description = ?, price = ?, quantity = ? WHERE id = ?";

                try (Connection conn = getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(updateSql)) {

                    pstmt.setString(1, updatedName);
                    pstmt.setString(2, updatedDescription);
                    pstmt.setFloat(3, updatedPrice);
                    pstmt.setInt(4, updatedQuantity);
                    pstmt.setInt(5, id);

                    int affectedRows = pstmt.executeUpdate();

                    if (affectedRows > 0) {
                        return String.format(
                                "{\"id\": %d, \"name\": \"%s\", \"description\": \"%s\", \"price\": \"%.2f\", \"quantity\": \"%s\"}",
                                id, updatedName, updatedDescription, updatedPrice, updatedQuantity);
                    }
                }

                return "{\"status_code\": \"400\"}";
            } catch (NumberFormatException e) {
                return "{\"status_code\": \"400\"}";
            }
        }

        /**
         * Handles the product deletion process.
         *
         * @param request A map containing the request parameters.
         * @return A JSON string indicating the success or failure of the deletion.
         * @throws SQLException If a database access error occurs.
         */
        private String handleDelete(Map<String, String> request) throws SQLException {
            if (!request.containsKey("id") || !request.containsKey("name") ||
                    !request.containsKey("price") || !request.containsKey("quantity")) {
                return "{\"status_code\": \"400\"}"; // Missing required fields
            }

            try {
                int id = Integer.parseInt(request.get("id"));
                String name = request.get("name");
                Float price = Float.parseFloat(request.get("price"));
                Integer quantity = Integer.parseInt(request.get("quantity"));

                if (!productExists(id)) {
                    return "{\"status_code\": \"404\"}";
                }

                // Verify product details before deletion
                String verifySql = "SELECT name, price, quantity FROM products WHERE id = ?";

                boolean verified = false;
                try (Connection conn = getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(verifySql)) {

                    pstmt.setInt(1, id);

                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            String dbName = rs.getString("name");
                            float dbPrice = rs.getFloat("price");
                            int dbQuantity = rs.getInt("quantity");

                            // Manual verification with tolerance for floating point
                            boolean nameMatch = name.equals(dbName);
                            boolean priceMatch = Math.abs(price - dbPrice) < 0.01; // Allow small difference
                            boolean quantityMatch = quantity == dbQuantity;
                            verified = nameMatch && priceMatch && quantityMatch;
                       
                        } else {
                            System.out.println("No product found with id: " + id);
                        }
                    }
                }

                if (!verified) {
                    return "{\"status_code\": \"401\"}";
                }

                // Delete the product
                String deleteSql = "DELETE FROM products WHERE id = ?";

                try (Connection conn = getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(deleteSql)) {

                    pstmt.setInt(1, id);
                    int affectedRows = pstmt.executeUpdate();

                    if (affectedRows > 0) {
                        return "{}";
                    }
                }

                return "{\"status_code\": \"401\"}";
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
    }

    /**
     * Checks if a product with the given ID exists in the database.
     *
     * @param id The ID of the product to check.
     * @return true if the product exists, false otherwise.
     * @throws SQLException If a database access error occurs.
     */
    private static boolean productExists(int id) throws SQLException {
        String sql = "SELECT COUNT(*) FROM products WHERE id = ?";

        try (Connection conn = getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, id);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }
}
