import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Server {
    private static final int PORT = 6379;

    // Key-Value Store for Strings
    private static final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    // Key-Value Store for Lists
    private static final ConcurrentHashMap<String, Deque<String>> listStore = new ConcurrentHashMap<>();
    // Expiration Store
    private static final ConcurrentHashMap<String, Long> expiryStore = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.out.println("Starting Java Redis Server on port " + PORT + "...");
        startActiveEviction();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server ready! Waiting for client connections...");
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getInetAddress());
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }

    private static void startActiveEviction() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (String key : expiryStore.keySet()) {
                Long expireAt = expiryStore.get(key);
                if (expireAt != null && now > expireAt) {
                    store.remove(key);
                    listStore.remove(key);
                    expiryStore.remove(key);
                    System.out.println("Active Eviction: Automatically cleaned up expired key -> " + key);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private static void handleClient(Socket clientSocket) {
        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String inputLine;
            while ((inputLine = reader.readLine()) != null) {
                System.out.println("Received: " + inputLine);
                String[] parts = inputLine.trim().split("\\s+");
                String command = parts[0].toUpperCase();

                switch (command) {
                    case "PING":
                        writer.println("+PONG");
                        break;

                    case "SET":
                        if (parts.length >= 3) {
                            String key = parts[1];
                            String value = parts[2];
                            listStore.remove(key);
                            store.put(key, value);

                            if (parts.length >= 5 && parts[3].equalsIgnoreCase("EX")) {
                                try {
                                    long seconds = Long.parseLong(parts[4]);
                                    long expireAt = System.currentTimeMillis() + (seconds * 1000);
                                    expiryStore.put(key, expireAt);
                                } catch (NumberFormatException e) {
                                    writer.println("-ERR value is not an integer or out of range");
                                    break;
                                }
                            } else {
                                expiryStore.remove(key);
                            }
                            writer.println("+OK");
                        } else {
                            writer.println("-ERR wrong number of arguments for 'SET'");
                        }
                        break;

                    case "GET":
                        if (parts.length >= 2) {
                            String key = parts[1];
                            if (isExpired(key)) {
                                store.remove(key);
                                listStore.remove(key);
                                expiryStore.remove(key);
                                writer.println("(nil)");
                            } else {
                                String value = store.get(key);
                                writer.println(value != null ? value : "(nil)");
                            }
                        } else {
                            writer.println("-ERR wrong number of arguments for 'GET'");
                        }
                        break;

                    case "EXISTS":
                        if (parts.length >= 2) {
                            int count = 0;
                            for (int i = 1; i < parts.length; i++) {
                                String key = parts[i];
                                if (isExpired(key)) {
                                    store.remove(key);
                                    listStore.remove(key);
                                    expiryStore.remove(key);
                                } else if (store.containsKey(key) || listStore.containsKey(key)) {
                                    count++;
                                }
                            }
                            writer.println(":" + count);
                        } else {
                            writer.println("-ERR wrong number of arguments for 'EXISTS'");
                        }
                        break;

                    case "DEL":
                        if (parts.length >= 2) {
                            int deletedCount = 0;
                            for (int i = 1; i < parts.length; i++) {
                                String key = parts[i];
                                expiryStore.remove(key);
                                boolean removedStr = store.remove(key) != null;
                                boolean removedList = listStore.remove(key) != null;
                                if (removedStr || removedList) {
                                    deletedCount++;
                                }
                            }
                            writer.println(":" + deletedCount);
                        } else {
                            writer.println("-ERR wrong number of arguments for 'DEL'");
                        }
                        break;

                    case "INCR":
                        if (parts.length >= 2) {
                            handleIncrement(writer, parts[1], 1);
                        } else {
                            writer.println("-ERR wrong number of arguments for 'INCR'");
                        }
                        break;

                    case "DECR":
                        if (parts.length >= 2) {
                            handleIncrement(writer, parts[1], -1);
                        } else {
                            writer.println("-ERR wrong number of arguments for 'DECR'");
                        }
                        break;

                    case "RPUSH":
                        if (parts.length >= 3) {
                            String key = parts[1];
                            if (isExpired(key)) {
                                listStore.remove(key);
                                store.remove(key);
                                expiryStore.remove(key);
                            }
                            Deque<String> list = listStore.computeIfAbsent(key, k -> new ArrayDeque<>());
                            for (int i = 2; i < parts.length; i++) {
                                list.addLast(parts[i]); // Standard append to tail
                            }
                            writer.println(":" + list.size());
                        } else {
                            writer.println("-ERR wrong number of arguments for 'RPUSH'");
                        }
                        break;

                    case "LPUSH":
                        if (parts.length >= 3) {
                            String key = parts[1];
                            if (isExpired(key)) {
                                listStore.remove(key);
                                store.remove(key);
                                expiryStore.remove(key);
                            }
                            Deque<String> list = listStore.computeIfAbsent(key, k -> new ArrayDeque<>());
                            for (int i = 2; i < parts.length; i++) {
                                list.addFirst(parts[i]); // Push to head
                            }
                            writer.println(":" + list.size());
                        } else {
                            writer.println("-ERR wrong number of arguments for 'LPUSH'");
                        }
                        break;

                    case "LPOP":
                        if (parts.length >= 2) {
                            String key = parts[1];
                            if (isExpired(key)) {
                                listStore.remove(key);
                                expiryStore.remove(key);
                                writer.println("(nil)");
                                break;
                            }
                            Deque<String> list = listStore.get(key);
                            if (list == null || list.isEmpty()) {
                                writer.println("(nil)");
                            } else {
                                String popped = list.pollFirst();
                                if (list.isEmpty()) {
                                    listStore.remove(key);
                                }
                                writer.println(popped);
                            }
                        } else {
                            writer.println("-ERR wrong number of arguments for 'LPOP'");
                        }
                        break;

                    case "RPOP":
                        if (parts.length >= 2) {
                            String key = parts[1];
                            if (isExpired(key)) {
                                listStore.remove(key);
                                expiryStore.remove(key);
                                writer.println("(nil)");
                                break;
                            }
                            Deque<String> list = listStore.get(key);
                            if (list == null || list.isEmpty()) {
                                writer.println("(nil)");
                            } else {
                                String popped = list.pollLast();
                                if (list.isEmpty()) {
                                    listStore.remove(key);
                                }
                                writer.println(popped);
                            }
                        } else {
                            writer.println("-ERR wrong number of arguments for 'RPOP'");
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

    private static void handleIncrement(PrintWriter writer, String key, long delta) {
        if (isExpired(key)) {
            store.remove(key);
            expiryStore.remove(key);
        }

        String currentVal = store.get(key);
        long num = 0;

        if (currentVal != null) {
            try {
                num = Long.parseLong(currentVal);
            } catch (NumberFormatException e) {
                writer.println("-ERR value is not an integer or out of range");
                return;
            }
        }

        num += delta;
        store.put(key, String.valueOf(num));
        writer.println(":" + num);
    }

    private static boolean isExpired(String key) {
        Long expireAt = expiryStore.get(key);
        if (expireAt == null) {
            return false;
        }
        return System.currentTimeMillis() > expireAt;
    }
}