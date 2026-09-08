import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class Server {
    private static final int REDIS_PORT = 6379;
    private static final int HTTP_PORT = 8080;
    
    // Thread-safe storage for key-value pairs
    private static final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        new Thread(() -> startHttpServer()).start();
        startRedisServer();
    }

    private static void startHttpServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(HTTP_PORT), 0);
            server.createContext("/", new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    String response = "{\"status\": \"online\", \"server\": \"Custom Java Redis Engine\", \"tcp_port\": 6379}";
                    byte[] responseBytes = response.getBytes();
                    exchange.getResponseHeaders().set("Content-Type", "application/json");

                    if ("HEAD".equalsIgnoreCase(exchange.getRequestMethod())) {
                        exchange.sendResponseHeaders(200, -1);
                        exchange.getResponseBody().close();
                    } else {
                        exchange.sendResponseHeaders(200, responseBytes.length);
                        OutputStream os = exchange.getResponseBody();
                        os.write(responseBytes);
                        os.close();
                    }
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
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
        }
    }

    private static void handleClient(Socket socket) {
        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            OutputStream out = socket.getOutputStream()
        ) {
            System.out.println("New TCP client connected: " + socket.getRemoteSocketAddress());
            String line;

            while ((line = reader.readLine()) != null) {
                // Inline / raw string command handling
                if (!line.startsWith("*")) {
                    String[] parts = line.trim().split("\\s+");
                    processCommand(parts, out);
                    continue;
                }

                // RESP Array command handling
                int arraySize = Integer.parseInt(line.substring(1));
                String[] args = new String[arraySize];

                for (int i = 0; i < arraySize; i++) {
                    reader.readLine(); // Skip length prefix ($3, $12, etc.)
                    args[i] = reader.readLine(); // Read actual command/argument
                }

                processCommand(args, out);
            }
        } catch (Exception e) {
            System.err.println("Client disconnected or error: " + e.getMessage());
        }
    }

    private static void processCommand(String[] args, OutputStream out) throws IOException {
        if (args.length == 0 || args[0] == null) return;

        String command = args[0].toUpperCase();

        if ("SET".equals(command) && args.length >= 3) {
            store.put(args[1], args[2]);
            out.write("+OK\r\n".getBytes());
            out.flush();
        } else if ("GET".equals(command) && args.length >= 2) {
            String val = store.get(args[1]);
            if (val == null) {
                out.write("$-1\r\n".getBytes());
            } else {
                String resp = "$" + val.length() + "\r\n" + val + "\r\n";
                out.write(resp.getBytes());
            }
            out.flush();
        } else {
            out.write("-ERR unknown command\r\n".getBytes());
            out.flush();
        }
    }
}