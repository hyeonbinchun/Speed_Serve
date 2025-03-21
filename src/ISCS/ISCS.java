import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * ISCS (Inter-Service Communication Server) is a simple HTTP server that handles communication between services,
 * specifically UserService and ProductService, by forwarding HTTP requests to them and responding with the results.
 * 
 * The server loads configuration dynamically from a `config.json` file and starts an HTTP server on a specified port.
 * It supports HTTP GET and POST methods for interacting with the respective services.
 */

public class ISCS {
    private static String USER_SERVICE_URL;
    private static String PRODUCT_SERVICE_URL;
    private static int ISCS_SERVICE_PORT;

    /**
     * Main method to start the ISCS server.
     *
     * @param args Command-line arguments: config.json file
     */
    public static void main(String[] args) {
        try {
            // Check if a config file is provided
            if (args.length < 1) {
                System.out.println("Please provide the path to the config file.");
                return;
            }
            
            // Use the first argument as the config file path
            String configFilePath = args[0];

            // Load config.json
            Map<String, String> config = loadConfig(configFilePath);

            // Assign service URLs dynamically
            USER_SERVICE_URL = "http://" + config.get("UserService.ip") + ":" + config.get("UserService.port");
            PRODUCT_SERVICE_URL = "http://" + config.get("ProductService.ip") + ":" + config.get("ProductService.port");
            ISCS_SERVICE_PORT = Integer.parseInt(config.get("InterServiceCommunication.port"));

            HttpServer server = HttpServer.create(new InetSocketAddress(ISCS_SERVICE_PORT), 0);
            server.setExecutor(Executors.newFixedThreadPool(10));            
            server.createContext("/user", new ServiceRequestHandler(USER_SERVICE_URL));
            server.createContext("/product", new ServiceRequestHandler(PRODUCT_SERVICE_URL));
            server.start();
            System.out.println("ISCS is running on port " + ISCS_SERVICE_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Loads configuration settings from a JSON file.
     *
     * @param filePath Path to the configuration file.
     * @return A map containing configuration key-value pairs.
     */
    private static Map<String, String> loadConfig(String filePath) {
        Map<String, String> configMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            String section = null;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("{") || line.startsWith("}")) continue;
                if (line.endsWith("{")) {
                    section = line.split(":")[0].trim().replace("\"", "");
                } else if (line.contains(":")) {
                    String[] parts = line.split(":");
                    String key = section + "." + parts[0].trim().replace("\"", "");
                    String value = parts[1].trim().replace("\"", "").replace(",", "");
                    configMap.put(key, value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return configMap;
    }
   
    /**
     * ServiceRequestHandler is a handler for HTTP requests. It forwards requests to a specified service
     * (UserService or ProductService) based on the request URI and method (GET or POST).
     */
    static class ServiceRequestHandler implements HttpHandler {
        private final String serviceUrl;

        /**
         * Constructs a new ServiceRequestHandler with the given service URL.
         *
         * @param serviceUrl The base URL of the service to forward requests to.
         */
        public ServiceRequestHandler(String serviceUrl) {
            this.serviceUrl = serviceUrl;
        }

        /**
         * Handles HTTP requests. Depending on the request method, it forwards the request to the respective service.
         * 
         * @param exchange The HTTP exchange object containing the request and response.
         * @throws IOException If an I/O error occurs during request processing.
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            try {
                if ("POST".equals(method)) {
                    String requestBody = getRequestBody(exchange);
                    forwardToService(exchange, requestBody, method);
                } else if ("GET".equals(method)) {
                    forwardToService(exchange, "", method);
                } else {
                    exchange.sendResponseHeaders(405, 0); // Method Not Allowed
                }
            } catch (Exception e) {
                e.printStackTrace();
                String errorResponse = "ISCS Error: " + e.getMessage();
                exchange.sendResponseHeaders(500, errorResponse.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorResponse.getBytes(StandardCharsets.UTF_8));
                }
            } finally {
                exchange.close();
            }
        }


        /**
         * Forwards the HTTP request to the corresponding service and sends the response back to the client.
         * 
         * @param exchange The HTTP exchange object.
         * @param payload The request body (for POST requests).
         * @param method The HTTP method (GET or POST).
         * @throws IOException If an I/O error occurs during request forwarding.
         * @throws InterruptedException If the request processing is interrupted.
         */
        private void forwardToService(HttpExchange exchange, String payload,  String method) throws IOException, InterruptedException {
            String path = exchange.getRequestURI().getPath();
            String serviceEndpoint = serviceUrl+ path;
            
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request;
            
            if ("POST".equals(method)) {
                // Create request to UserService
                request = HttpRequest.newBuilder()
                    .uri(URI.create(serviceEndpoint))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();
            } else {
                // GET request with body
                request = HttpRequest.newBuilder()
                    .uri(URI.create(serviceEndpoint))
                    .GET()
                    .build();
            }
           
            // Send request to service
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            // Forward response back to OrderService
            byte[] responseBytes = response.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(response.statusCode(), responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }

        /**
         * Reads the request body from the HTTP exchange.
         *
         * @param exchange The HTTP exchange object.
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
    }
}