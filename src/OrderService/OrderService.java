import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * OrderService is responsible for handling all order, user, and product related
 * operations:
 * /order: handle order logic
 * /user: forward to user service
 * /product: forward to product service
 */
public class OrderService {
    private static String USER_SERVICE_URL;
    private static String PRODUCT_SERVICE_URL;
    private static int ORDER_SERVICE_PORT;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    // Database connection properties
    private static String DB_URL;
    private static String DB_USER;
    private static String DB_PASSWORD;

    // Database connection pool
    private static HikariDataSource dataSource;

    // Redis connection properties
    private static String REDIS_HOST;
    private static int REDIS_PORT;
    private static JedisPool jedisPool;

    // Redis utility for centralized Redis operations
    private static RedisUtil redisUtil;

    /**
     * Redis utility class to centralize Redis operations and connection management
     */
    static class RedisUtil {
        private final JedisPool pool;

        public RedisUtil(JedisPool pool) {
            this.pool = pool;
        }

        /**
         * Sets a key in Redis with expiration time.
         */
        public void setWithExpiry(String key, String value, int expirySeconds) {
            try (Jedis jedis = pool.getResource()) {
                jedis.setex(key, expirySeconds, value);
            }
        }

        /**
         * Gets a value from Redis.
         */
        public String get(String key) {
            try (Jedis jedis = pool.getResource()) {
                return jedis.get(key);
            }
        }

        /**
         * Checks if a key exists in Redis.
         */
        public boolean exists(String key) {
            try (Jedis jedis = pool.getResource()) {
                return jedis.exists(key);
            }
        }

        /**
         * Deletes a key from Redis.
         */
        public void delete(String key) {
            try (Jedis jedis = pool.getResource()) {
                jedis.del(key);
            }
        }
    }

    /**
     * The main entry point for starting the OrderService.
     * 
     * @param args the command-line arguments: config.json file
     * @throws IOException If an I/O error occurs while setting up the server.
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
        // Assign service URLs dynamically
        USER_SERVICE_URL = "http://user-service:" + config.get("UserService.port");
        PRODUCT_SERVICE_URL = "http://product-service:" + config.get("ProductService.port");
        ORDER_SERVICE_PORT = Integer.parseInt(config.get("OrderService.port"));

        // Load database configuration
        DB_URL = config.get("Database.url");
        DB_USER = config.get("Database.user");
        DB_PASSWORD = config.get("Database.password");

        // Initialize the database connection pool
        initializeConnectionPool(
                DB_URL,
                DB_USER,
                DB_PASSWORD);

        System.out.println("Redis host: " + config.get("Redis.host"));
        System.out.println("Redis port: " + config.get("Redis.port"));
        // Load Redis configuration
        REDIS_HOST = config.get("Redis.host");
        REDIS_PORT = Integer.parseInt(config.get("Redis.port"));

        // Initialize Redis connection pool
        initializeRedisPool(REDIS_HOST, REDIS_PORT);

        HttpServer server = HttpServer.create(new InetSocketAddress(ORDER_SERVICE_PORT), 0);
        server.setExecutor(Executors.newFixedThreadPool(20));
        server.createContext("/user/purchased", new UserPurchaseHandler());
        server.createContext("/user", new ForwardingHandler());
        server.createContext("/product", new ForwardingHandler());
        server.createContext("/order", new OrderHandler());
        server.start();

        System.out.println("OrderService is running on port " + ORDER_SERVICE_PORT);

        // Add shutdown hook to close the connection pool
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down connection pools...");
            if (dataSource != null && !dataSource.isClosed()) {
                dataSource.close();
            }
            if (jedisPool != null) {
                try {
                    jedisPool.close();
                } catch (Exception e) {
                    System.err.println("Error closing Redis pool: " + e.getMessage());
                }
            }
        }));

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
            config.setPoolName("OrderServiceConnectionPool");
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

    private static void initializeRedisPool(String host, int port) {
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setJmxEnabled(false);
            jedisPool = new JedisPool(poolConfig, host, port);
            
            // Initialize the RedisUtil object
            redisUtil = new RedisUtil(jedisPool);
    
            // Test the connection
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.ping();
                System.out.println("Successfully connected to Redis");
            }
        } catch (Exception e) {
            System.err.println("Redis connection pool initialization error: " + e.getMessage());
            e.printStackTrace();
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
     * Loads configuration from the specified JSON file.
     *
     * @param filePath The path to the JSON configuration file.
     * @return A map containing the configuration key-value pairs.
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
            }

            // Similarly parse OrderService section for port
            int productServiceStart = content.indexOf("\"OrderService\"");
            if (productServiceStart > 0) {
                int portStart = content.indexOf("\"port\"", productServiceStart);
                if (portStart > 0) {
                    int valueStart = content.indexOf(":", portStart) + 1;
                    int valueEnd = content.indexOf(",", valueStart);
                    if (valueEnd < 0) {
                        valueEnd = content.indexOf("}", valueStart); // Handles the last value
                    }
                    String portValue = content.substring(valueStart, valueEnd).trim();
                    configMap.put("OrderService.port", portValue);
                }
            }

            int redisStart = content.indexOf("\"Redis\"");
            if (redisStart > 0) {
                int hostStart = content.indexOf("\"host\"", redisStart);
                if (hostStart > 0) {
                    int valueStart = content.indexOf(":", hostStart) + 1;
                    int valueEnd = content.indexOf(",", valueStart);
                    if (valueEnd < 0) {
                        valueEnd = content.indexOf("}", valueStart);
                    }
                    String hostValue = content.substring(valueStart, valueEnd).trim();
                    hostValue = hostValue.replaceAll("\"", "");
                    configMap.put("Redis.host", hostValue);
                }

                int portStart = content.indexOf("\"port\"", redisStart);
                if (portStart > 0) {
                    int valueStart = content.indexOf(":", portStart) + 1;
                    int valueEnd = content.indexOf(",", valueStart);
                    if (valueEnd < 0) {
                        valueEnd = content.indexOf("}", valueStart);
                    }
                    String portValue = content.substring(valueStart, valueEnd).trim();
                    configMap.put("Redis.port", portValue);
                }
            }

        } catch (Exception e) {
            System.out.println("Error loading config: " + e.getMessage());
            e.printStackTrace();
        }

        return configMap;
    }

    /**
     * Utility method to send HTTP responses consistently across all handlers.
     */
    private static void sendResponse(HttpExchange exchange, ApiResponse response) throws IOException {
        byte[] responseBytes = response.getJsonResponse().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(response.getStatusCode(), responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    /**
     * Utility method to retrieve the body of an HTTP request as a string.
     */
    private static String getRequestBody(HttpExchange exchange) throws IOException {
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
     * Utility method to checks if a user exists.
     */
    /**
     * Utility method to checks if a user exists.
     */
    private static boolean userExists(int userId) throws SQLException {
        String cacheKey = "user:" + userId;

        // Check cache first using RedisUtil
        if (redisUtil.exists(cacheKey)) {
            return Boolean.parseBoolean(redisUtil.get(cacheKey));
        }

        // If not in cache, check database
        String query = "SELECT COUNT(*) AS count FROM users WHERE id = ?";

        try (Connection conn = getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query)) {

            pstmt.setInt(1, userId);
            ResultSet rs = pstmt.executeQuery();

            boolean exists = false;
            if (rs.next()) {
                exists = rs.getInt("count") > 0;
            }

            // Cache the result for 1 minutes using RedisUtil
            redisUtil.setWithExpiry(cacheKey, String.valueOf(exists), 60);

            return exists;
        }
    }

    /**
     * Response class to standardize API responses.
     */
    static class ApiResponse {
        private final String jsonResponse;
        private final int statusCode;

        /**
         * Creates a success response with the provided data.
         * 
         * @param jsonData The JSON data to include in the response.
         * @return A new ApiResponse with 200 status code.
         */
        public static ApiResponse success(String jsonData) {
            return new ApiResponse(jsonData, 200);
        }

        /**
         * Creates an error response with the provided status and message.
         * 
         * @param message    The error message.
         * @param statusCode The HTTP status code.
         * @return A new ApiResponse with the specified status code.
         */
        public static ApiResponse error(String message, int statusCode) {
            return new ApiResponse(String.format("{\"status\": \"%s\"}", message), statusCode);
        }

        private ApiResponse(String jsonResponse, int statusCode) {
            this.jsonResponse = jsonResponse;
            this.statusCode = statusCode;
        }

        public String getJsonResponse() {
            return jsonResponse;
        }

        public int getStatusCode() {
            return statusCode;
        }
    }

    /**
     * Handles retrieving purchased items for a user.
     */
    static class UserPurchaseHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");

            if (parts.length != 4) {
                OrderService.sendResponse(exchange, ApiResponse.error("Invalid Request", 400));
                return;
            }

            String userIdStr = parts[3];
            try {
                int userId = Integer.parseInt(userIdStr);

                // Check if user exists
                if (!OrderService.userExists(userId)) {
                    OrderService.sendResponse(exchange, ApiResponse.error("User Not Found", 404));
                    return;
                }

                // Get purchased products from PostgreSQL
                Map<Integer, Integer> purchases = getUserPurchases(userId);

                // Send response
                String jsonResponse = convertToJson(purchases);

                OrderService.sendResponse(exchange, ApiResponse.success(jsonResponse));

            } catch (NumberFormatException e) {
                OrderService.sendResponse(exchange, ApiResponse.error("Invalid User ID", 400));
            } catch (SQLException e) {
                OrderService.sendResponse(exchange, ApiResponse.error("Database Error", 500));
            }
        }

        private String convertToJson(Map<Integer, Integer> purchases) {
            StringBuilder json = new StringBuilder("{");

            for (Map.Entry<Integer, Integer> entry : purchases.entrySet()) {
                json.append("\"").append(entry.getKey()).append("\":")
                        .append(entry.getValue()).append(",");
            }

            if (!purchases.isEmpty()) {
                json.deleteCharAt(json.length() - 1); // Remove trailing comma
            }

            json.append("}");
            return json.toString();
        }

        /**
         * Retrieve a user's purchases.
         */
        private Map<Integer, Integer> getUserPurchases(int userId) throws SQLException {
            String cacheKey = "user:purchases:" + userId;

            // Check cache first using RedisUtil
            if (redisUtil.exists(cacheKey)) {
                String cachedData = redisUtil.get(cacheKey);
                return parseJsonToMap(cachedData);
            }

            Map<Integer, Integer> purchases = new HashMap<>();
            String query = "SELECT product_id, SUM(quantity) as total_quantity FROM orders WHERE user_id = ? GROUP BY product_id";

            try (Connection conn = getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query)) {

                pstmt.setInt(1, userId);
                ResultSet rs = pstmt.executeQuery();

                while (rs.next()) {
                    int productId = rs.getInt("product_id");
                    int quantity = rs.getInt("total_quantity");
                    purchases.put(productId, quantity);
                }

                // Cache the result for 2 minutes using RedisUtil
                String jsonData = convertToJson(purchases);
                redisUtil.setWithExpiry(cacheKey, jsonData, 60);
            }

            return purchases;
        }

        private Map<Integer, Integer> parseJsonToMap(String json) {
            Map<Integer, Integer> result = new HashMap<>();
            // Remove outer braces
            json = json.substring(1, json.length() - 1);

            if (json.isEmpty()) {
                return result;
            }

            // Split by commas
            String[] pairs = json.split(",");
            for (String pair : pairs) {
                String[] keyValue = pair.split(":");
                if (keyValue.length == 2) {
                    int key = Integer.parseInt(keyValue[0].replaceAll("\"", ""));
                    int value = Integer.parseInt(keyValue[1]);
                    result.put(key, value);
                }
            }

            return result;
        }

    }

    /**
     * Handles incoming HTTP requests for order-related operations.
     */
    static class OrderHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String method = exchange.getRequestMethod();
                if ("POST".equals(method)) {
                    handlePost(exchange);
                } else {
                    OrderService.sendResponse(exchange, ApiResponse.error("Method Not Allowed", 405));
                }
            } catch (Exception e) {
                e.printStackTrace();
                OrderService.sendResponse(exchange, ApiResponse.error("Internal Server Error", 500));
            } finally {
                exchange.close();
            }
        }

        /**
         * Handles POST requests for creating orders.
         */
        private void handlePost(HttpExchange exchange) throws IOException {
            try {
                String requestBody = OrderService.getRequestBody(exchange);
                Map<String, String> requestMap = parseRequest(requestBody);
                String command = requestMap.get("command");

                ApiResponse response;
                if ("place order".equalsIgnoreCase(command)) {
                    response = handlePlaceOrder(requestMap);
                } else {
                    response = ApiResponse.error("Invalid Request", 400);
                }

                OrderService.sendResponse(exchange, response);
            } catch (Exception e) {
                e.printStackTrace();
                OrderService.sendResponse(exchange, ApiResponse.error("Invalid Request", 400));
            }
        }

        /**
         * Validates and processes an order placement request.
         */
        private ApiResponse handlePlaceOrder(Map<String, String> request) throws IOException, SQLException {
            if (!request.containsKey("user_id") || !request.containsKey("product_id")
                    || !request.containsKey("quantity")) {
                return ApiResponse.error("Invalid Request", 400);
            }

            try {
                int userId = Integer.parseInt(request.get("user_id"));
                int productId = Integer.parseInt(request.get("product_id"));
                int quantity = Integer.parseInt(request.get("quantity"));
                // Check if user exists
                if (!OrderService.userExists(userId)) {
                    return ApiResponse.error("Invalid Request", 404);
                }

                // Check if product exists
                if (!productExists(productId)) {
                    return ApiResponse.error("Invalid Request", 404);
                }

                // Check if quantity is valid
                if (quantity <= 0) {
                    return ApiResponse.error("Invalid Request", 400);
                }

                // Check if in stock
                if (!isProductInStock(productId, quantity)) {
                    return ApiResponse.error("Exceeded quantity limit", 400);
                }

                // Update product stock in database
                updateProductStock(productId, quantity);

                // Save order to PostgreSQL database
                int orderId = saveOrderToDatabase(productId, userId, quantity);

                // Invalidate user purchases cache using RedisUtil
                redisUtil.delete("user:purchases:" + userId);

                // Return success response
                String successResponse = String.format(
                        "{\"product_id\": %d, \"user_id\": %d, \"quantity\": %d, \"status\": \"Success\"}",
                        productId, userId, quantity);

                return ApiResponse.success(successResponse);
            } catch (NumberFormatException e) {
                return ApiResponse.error("Invalid Request", 400);
            }
        }

        /**
         * Saves an order to the PostgreSQL database.
         */
        private int saveOrderToDatabase(int productId, int userId, int quantity) throws SQLException {
            String query = "INSERT INTO orders (product_id, user_id, quantity) VALUES (?, ?, ?) RETURNING order_id";

            try (Connection conn = getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query)) {

                pstmt.setInt(1, productId);
                pstmt.setInt(2, userId);
                pstmt.setInt(3, quantity);

                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    return rs.getInt("order_id");
                } else {
                    throw new SQLException("Failed to retrieve generated order ID");
                }
            }
        }

        /**
         * Checks if a product exists
         */
        private boolean productExists(int productId) throws IOException {
            String cacheKey = "product:" + productId;

            // Check cache first using RedisUtil
            if (redisUtil.exists(cacheKey)) {
                return Boolean.parseBoolean(redisUtil.get(cacheKey));
            }

            // If not in cache, check via HTTP
            String productEndpoint = PRODUCT_SERVICE_URL + "/product/" + productId;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(productEndpoint))
                    .GET()
                    .build();
            try {
                HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
                boolean exists = response.statusCode() == 200;

                // Cache the result for 5 minutes using RedisUtil
                redisUtil.setWithExpiry(cacheKey, String.valueOf(exists), 60);

                return exists;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Error while checking product existence via ISCS", e);
            }
        }

        /**
         * Checks if a product has enough quantity in stock.
         */
        private boolean isProductInStock(int productId, int quantity) throws SQLException {
            String cacheKey = "product:stock:" + productId;

            // Check cache first using RedisUtil
            if (redisUtil.exists(cacheKey)) {
                int cachedStock = Integer.parseInt(redisUtil.get(cacheKey));
                return cachedStock >= quantity;
            }

            // If not in cache, check database
            String query = "SELECT quantity FROM products WHERE id = ?";

            try (Connection conn = getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query)) {

                pstmt.setInt(1, productId);
                ResultSet rs = pstmt.executeQuery();

                if (rs.next()) {
                    int stockQuantity = rs.getInt("quantity");

                    // Cache the result for 1 minute (shorter time for stock levels) using RedisUtil
                    redisUtil.setWithExpiry(cacheKey, String.valueOf(stockQuantity), 60);

                    return stockQuantity >= quantity;
                }
                return false;
            }
        }

        /**
         * Updates the stock of a product by sending a request to the ISCS service.
         */
        private void updateProductStock(int productId, int quantity) throws SQLException {
            String cacheKey = "product:stock:" + productId;

            String query = "UPDATE products SET quantity = quantity - ? WHERE id = ?";

            try (Connection conn = getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query)) {

                pstmt.setInt(1, quantity);
                pstmt.setInt(2, productId);

                int rowsAffected = pstmt.executeUpdate();
                if (rowsAffected == 0) {
                    throw new SQLException("Failed to update product stock");
                }

                // Update cache or invalidate cache using RedisUtil
                if (redisUtil.exists(cacheKey)) {
                    // Get current stock
                    String query2 = "SELECT quantity FROM products WHERE id = ?";
                    try (PreparedStatement pstmt2 = conn.prepareStatement(query2)) {
                        pstmt2.setInt(1, productId);
                        ResultSet rs = pstmt2.executeQuery();
                        if (rs.next()) {
                            int newStock = rs.getInt("quantity");
                            redisUtil.setWithExpiry(cacheKey, String.valueOf(newStock), 60);
                        } else {
                            // If we can't get the new stock, invalidate cache
                            redisUtil.delete(cacheKey);
                        }
                    }
                }
            }
        }


        /**
         * Parses the request body and returns a map of key-value pairs.
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
    }

    /**
     * Forwards requests to the UserService or ProductService based on the request
     * method.
     */
    static class ForwardingHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            try {
                String requestBody = OrderService.getRequestBody(exchange);
                String path = exchange.getRequestURI().getPath();
                if (path.contains("user")) {
                    forwardRequest(exchange, requestBody, method, USER_SERVICE_URL);
                } else if (path.contains("product")) {
                    forwardRequest(exchange, requestBody, method, PRODUCT_SERVICE_URL);
                } else {
                    OrderService.sendResponse(exchange, ApiResponse.error("Invalid Request", 400));
                }
            } catch (Exception e) {
                e.printStackTrace();
                String errorResponse = "Internal Server Error: " + e.getMessage();
                exchange.sendResponseHeaders(500, errorResponse.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorResponse.getBytes(StandardCharsets.UTF_8));
                }
            } finally {
                exchange.close();
            }
        }

        /**
         * Forwards the request to the User or Product service and sends the response
         * back to the client
         */
        private void forwardRequest(HttpExchange exchange, String payload, String method, String serviceUrl)
                throws IOException, InterruptedException {
            String path = exchange.getRequestURI().getPath();
            String endpoint = serviceUrl + path;

            HttpRequest request;
            if ("POST".equals(method)) {
                request = HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build();
            } else {
                request = HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .header("Content-Type", "application/json")
                        .GET()
                        .build();
            }

            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

            // Forward service response back to client
            byte[] responseBytes = response.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(response.statusCode(), responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
    }
}