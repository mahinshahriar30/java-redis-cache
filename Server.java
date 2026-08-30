import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class Server {
    private static final int PORT = 6379;
    
    // In-memory key-value database (Thread-safe)
    private static final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.out.println("🚀 Starting Java Redis Server on port " + PORT + "...");

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("✅ Server ready! Waiting for client connections...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("⚡ New client connected: " + clientSocket.getInetAddress());

                // Run each client on a separate thread so multiple users can connect at once
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("❌ Server error: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String inputLine;
            while ((inputLine = reader.readLine()) != null) {
                System.out.println("Received: " + inputLine);

                // Split input into words: e.g., "SET user Mahin" -> ["SET", "user", "Mahin"]
                String[] parts = inputLine.trim().split("\\s+");
                String command = parts[0].toUpperCase();

                switch (command) {
                    case "PING":
                        writer.println("+PONG");
                        break;

                    case "SET":
                        if (parts.length >= 3) {
                            store.put(parts[1], parts[2]); // Save key and value to RAM
                            writer.println("+OK");
                        } else {
                            writer.println("-ERR wrong number of arguments for 'SET'");
                        }
                        break;

                    case "GET":
                        if (parts.length >= 2) {
                            String value = store.get(parts[1]); // Look up key
                            if (value != null) {
                                writer.println(value);
                            } else {
                                writer.println("(nil)"); // Key not found
                            }
                        } else {
                            writer.println("-ERR wrong number of arguments for 'GET'");
                        }
                        break;

                    default:
                        writer.println("-ERR unknown command '" + command + "'");
                        break;
                }
            }
        } catch (IOException e) {
            System.out.println("Client disconnected.");
        }
    }
}