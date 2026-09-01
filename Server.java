import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Server {
    private static final int PORT = 6379;
    private static final String AOF_FILE = "database.aof";

    private static final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Deque<String>> listStore = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, String>> hashStore = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> expiryStore = new ConcurrentHashMap<>();

    private static BufferedWriter aofWriter;

    public static void main(String[] args) {
        System.out.println("Starting Java Redis Server on port " + PORT + "...");
        
        // 1. Load persisted data from disk
        loadAOF();

        // 2. Initialize AOF writer for live logging
        try {
            aofWriter = new BufferedWriter(new FileWriter(AOF_FILE, true));
        } catch (IOException e) {
            System.err.println("Failed to initialize AOF writer: " + e.getMessage());
        }

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
                    hashStore.remove(key);
                    expiryStore.remove(key);
                    System.out.println("Active Eviction: Cleaned up expired key -> " + key);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private static void handleClient(Socket clientSocket) {
        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts;

                if (line.startsWith("*")) {
                    parts = parseRESPArray(reader, line);
                    if (parts == null || parts.length == 0) continue;
                } else {
                    parts = line.trim().split("\\s+");
                }

                if (parts.length == 0 || parts[0].isEmpty()) continue;
                executeCommand(parts, writer, true);
            }
        } catch (IOException e) {
            System.out.println("Client disconnected.");
        }
    }

    // Core execution engine (shared by network clients and AOF loader)
    private static void executeCommand(String[] parts, PrintWriter writer, boolean isLiveClient) {
        String command = parts[0].toUpperCase();

        switch (command) {
            case "PING":
                if (writer != null) writer.println("+PONG");
                break;

            case "SET":
                if (parts.length >= 3) {
                    String key = parts[1];
                    String value = parts[2];
                    listStore.remove(key);
                    hashStore.remove(key);
                    store.put(key, value);

                    if (parts.length >= 5 && parts[3].equalsIgnoreCase("EX")) {
                        try {
                            long seconds = Long.parseLong(parts[4]);
                            long expireAt = System.currentTimeMillis() + (seconds * 1000);
                            expiryStore.put(key, expireAt);
                        } catch (NumberFormatException e) {
                            if (writer != null) writer.println("-ERR value is not an integer or out of range");
                            break;
                        }
                    } else {
                        expiryStore.remove(key);
                    }
                    if (isLiveClient) appendToAOF(parts);
                    if (writer != null) writer.println("+OK");
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'SET'");
                }
                break;

            case "GET":
                if (parts.length >= 2) {
                    String key = parts[1];
                    if (isExpired(key)) {
                        store.remove(key);
                        listStore.remove(key);
                        hashStore.remove(key);
                        expiryStore.remove(key);
                        if (writer != null) writer.println("(nil)");
                    } else {
                        String value = store.get(key);
                        if (writer != null) writer.println(value != null ? value : "(nil)");
                    }
                } else if (writer != null) {
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
                            hashStore.remove(key);
                            expiryStore.remove(key);
                        } else if (store.containsKey(key) || listStore.containsKey(key) || hashStore.containsKey(key)) {
                            count++;
                        }
                    }
                    if (writer != null) writer.println(":" + count);
                } else if (writer != null) {
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
                        boolean removedHash = hashStore.remove(key) != null;
                        if (removedStr || removedList || removedHash) {
                            deletedCount++;
                        }
                    }
                    if (isLiveClient) appendToAOF(parts);
                    if (writer != null) writer.println(":" + deletedCount);
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'DEL'");
                }
                break;

            case "INCR":
                if (parts.length >= 2) {
                    handleIncrement(writer, parts[1], 1, isLiveClient);
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'INCR'");
                }
                break;

            case "DECR":
                if (parts.length >= 2) {
                    handleIncrement(writer, parts[1], -1, isLiveClient);
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'DECR'");
                }
                break;

            case "RPUSH":
                if (parts.length >= 3) {
                    String key = parts[1];
                    if (isExpired(key)) {
                        listStore.remove(key);
                        store.remove(key);
                        hashStore.remove(key);
                        expiryStore.remove(key);
                    }
                    Deque<String> list = listStore.computeIfAbsent(key, k -> new ArrayDeque<>());
                    for (int i = 2; i < parts.length; i++) {
                        list.addLast(parts[i]);
                    }
                    if (isLiveClient) appendToAOF(parts);
                    if (writer != null) writer.println(":" + list.size());
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'RPUSH'");
                }
                break;

            case "LPUSH":
                if (parts.length >= 3) {
                    String key = parts[1];
                    if (isExpired(key)) {
                        listStore.remove(key);
                        store.remove(key);
                        hashStore.remove(key);
                        expiryStore.remove(key);
                    }
                    Deque<String> list = listStore.computeIfAbsent(key, k -> new ArrayDeque<>());
                    for (int i = 2; i < parts.length; i++) {
                        list.addFirst(parts[i]);
                    }
                    if (isLiveClient) appendToAOF(parts);
                    if (writer != null) writer.println(":" + list.size());
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'LPUSH'");
                }
                break;

            case "LPOP":
                if (parts.length >= 2) {
                    String key = parts[1];
                    if (isExpired(key)) {
                        listStore.remove(key);
                        expiryStore.remove(key);
                        if (writer != null) writer.println("(nil)");
                        break;
                    }
                    Deque<String> list = listStore.get(key);
                    if (list == null || list.isEmpty()) {
                        if (writer != null) writer.println("(nil)");
                    } else {
                        String popped = list.pollFirst();
                        if (list.isEmpty()) {
                            listStore.remove(key);
                        }
                        if (isLiveClient) appendToAOF(parts);
                        if (writer != null) writer.println(popped);
                    }
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'LPOP'");
                }
                break;

            case "RPOP":
                if (parts.length >= 2) {
                    String key = parts[1];
                    if (isExpired(key)) {
                        listStore.remove(key);
                        expiryStore.remove(key);
                        if (writer != null) writer.println("(nil)");
                        break;
                    }
                    Deque<String> list = listStore.get(key);
                    if (list == null || list.isEmpty()) {
                        if (writer != null) writer.println("(nil)");
                    } else {
                        String popped = list.pollLast();
                        if (list.isEmpty()) {
                            listStore.remove(key);
                        }
                        if (isLiveClient) appendToAOF(parts);
                        if (writer != null) writer.println(popped);
                    }
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'RPOP'");
                }
                break;

            case "HSET":
                if (parts.length >= 4) {
                    String key = parts[1];
                    if (isExpired(key)) {
                        hashStore.remove(key);
                        store.remove(key);
                        expiryStore.remove(key);
                    }
                    ConcurrentHashMap<String, String> map = hashStore.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
                    int fieldsAdded = 0;
                    for (int i = 2; i < parts.length - 1; i += 2) {
                        String field = parts[i];
                        String val = parts[i + 1];
                        if (!map.containsKey(field)) {
                            fieldsAdded++;
                        }
                        map.put(field, val);
                    }
                    if (isLiveClient) appendToAOF(parts);
                    if (writer != null) writer.println(":" + fieldsAdded);
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'HSET'");
                }
                break;

            case "HGET":
                if (parts.length >= 3) {
                    String key = parts[1];
                    String field = parts[2];
                    if (isExpired(key)) {
                        hashStore.remove(key);
                        expiryStore.remove(key);
                        if (writer != null) writer.println("(nil)");
                        break;
                    }
                    ConcurrentHashMap<String, String> map = hashStore.get(key);
                    if (map == null || !map.containsKey(field)) {
                        if (writer != null) writer.println("(nil)");
                    } else {
                        if (writer != null) writer.println(map.get(field));
                    }
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'HGET'");
                }
                break;

            case "HGETALL":
                if (parts.length >= 2) {
                    String key = parts[1];
                    if (isExpired(key)) {
                        hashStore.remove(key);
                        expiryStore.remove(key);
                        if (writer != null) writer.println("(empty list or set)");
                        break;
                    }
                    ConcurrentHashMap<String, String> map = hashStore.get(key);
                    if (map == null || map.isEmpty()) {
                        if (writer != null) writer.println("(empty list or set)");
                    } else {
                        if (writer != null) {
                            for (Map.Entry<String, String> entry : map.entrySet()) {
                                writer.println(entry.getKey());
                                writer.println(entry.getValue());
                            }
                        }
                    }
                } else if (writer != null) {
                    writer.println("-ERR wrong number of arguments for 'HGETALL'");
                }
                break;

            default:
                if (writer != null) writer.println("-ERR unknown command '" + command + "'");
                break;
        }
    }

    private static synchronized void appendToAOF(String[] parts) {
        if (aofWriter == null) return;
        try {
            aofWriter.write(String.join(" ", parts));
            aofWriter.newLine();
            aofWriter.flush();
        } catch (IOException e) {
            System.err.println("Error writing to AOF: " + e.getMessage());
        }
    }

    private static void loadAOF() {
        File file = new File(AOF_FILE);
        if (!file.exists()) return;

        System.out.println("Loading state from " + AOF_FILE + "...");
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] parts = line.trim().split("\\s+");
                executeCommand(parts, null, false);
                count++;
            }
            System.out.println("AOF Load complete! Restored " + count + " operations.");
        } catch (IOException e) {
            System.err.println("Error reading AOF file: " + e.getMessage());
        }
    }

    private static String[] parseRESPArray(BufferedReader reader, String firstLine) throws IOException {
        try {
            int numElements = Integer.parseInt(firstLine.substring(1).trim());
            List<String> commandParts = new ArrayList<>();

            for (int i = 0; i < numElements; i++) {
                String lengthLine = reader.readLine();
                if (lengthLine == null || !lengthLine.startsWith("$")) return null;

                String valueLine = reader.readLine();
                if (valueLine == null) return null;

                commandParts.add(valueLine);
            }
            return commandParts.toArray(new String[0]);
        } catch (Exception e) {
            return null;
        }
    }

    private static void handleIncrement(PrintWriter writer, String key, long delta, boolean isLiveClient) {
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
                if (writer != null) writer.println("-ERR value is not an integer or out of range");
                return;
            }
        }

        num += delta;
        store.put(key, String.valueOf(num));
        if (isLiveClient) appendToAOF(new String[]{"SET", key, String.valueOf(num)});
        if (writer != null) writer.println(":" + num);
    }

    private static boolean isExpired(String key) {
        Long expireAt = expiryStore.get(key);
        if (expireAt == null) {
            return false;
        }
        return System.currentTimeMillis() > expireAt;
    }
}