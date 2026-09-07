import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    private static final int REDIS_PORT = 6379;
    private static final int HTTP_PORT = 8080;

    public static void main(String[] args) {
        // 1. Start HTTP Health/Status Server for Web Browsers & Render
        new Thread(() -> startHttpServer()).start();

        // 2. Start Raw RESP Redis TCP Listener
        startRedisServer();
    }

    private static void startHttpServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(HTTP_PORT), 0);
            server.createContext("/", new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    String response = "{\"status\": \"online\", \"server\": \"Custom Java Redis Engine\", \"tcp_port\": 6379}";
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, response.length());
                    OutputStream os = exchange.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                }
            });
            server.setExecutor(null);
            System.out.println("HTTP Web status server running on port " + HTTP_PORT);
            server.start();
        } catch (IOException e) {
            System.err.println("Failed to start HTTP server: " + e.getMessage());
        }
    }

    private static void startRedisServer() {
        try (ServerSocket serverSocket = new ServerSocket(REDIS_PORT)) {
            System.out.println("Redis RESP TCP Server listening on port " + REDIS_PORT + "...");
            while (true) {
                Socket clientSocket = serverSocket.accept();
                // Handle incoming TCP Redis connections
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
        }
    }

    private static void handleClient(Socket socket) {
        try {
            System.out.println("New TCP client connected: " + socket.getRemoteSocketAddress());
            // Existing RESP logic handled here...
        } catch (Exception e) {
            System.err.println("Client handling error: " + e.getMessage());
        }
    }
}