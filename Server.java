import com.sun.net.httpserver.HttpServer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Custom concurrent in-memory Redis server (RESP protocol).
 *
 * Features: Strings (SET/GET/DEL/EXISTS/INCR/DECR), Lists (LPUSH/RPUSH/LPOP/RPOP),
 * Hashes (HSET/HGET/HGETALL), hybrid TTL eviction (passive + 1s active sweeper),
 * and AOF persistence (database.aof).
 */
public class Server {
    private static final int REDIS_PORT = 6379;
    private static final int HTTP_PORT = 8080;
    private static final String AOF_PATH = "database.aof";

    private static final int MAX_LINE = 64 * 1024;
    private static final int MAX_BULK = 64 * 1024 * 1024;
    private static final int MAX_ARGS = 1024 * 1024;

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] OK = "+OK\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NIL = "$-1\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY_ARRAY = "*0\r\n".getBytes(StandardCharsets.UTF_8);

    /** A stored value (String, list, or hash) with an optional absolute expiry (epoch ms, 0 = none). */
    private static final class Entry {
        final Object value;
        final long expireAt;

        Entry(Object value, long expireAt) {
            this.value = value;
            this.expireAt = expireAt;
        }

        boolean isExpired(long now) {
            return expireAt != 0 && now >= expireAt;
        }
    }

    /** Error that is reported to the client as a RESP error. */
    private static final class CmdError extends RuntimeException {
        CmdError(String message) {
            super(message, null, false, false);
        }
    }

    // Thread-safe key space. Values: String | ConcurrentLinkedDeque<String> | ConcurrentHashMap<String,String>
    private static final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();

    // Serialises all write commands so the AOF order always matches the order applied in memory.
    private static final Object LOCK = new Object();
    private static FileOutputStream aofFile; // guarded by LOCK
    private static volatile boolean aofDirty;

    // ------------------------------------------------------------------ main

    public static void main(String[] args) throws IOException {
        loadAof();
        aofFile = new FileOutputStream(AOF_PATH, true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            synchronized (LOCK) {
                try {
                    if (aofFile != null) {
                        aofFile.getFD().sync();
                        aofFile.close();
                        aofFile = null;
                    }
                } catch (IOException ignored) {
                    // best effort on shutdown
                }
            }
        }));

        startBackgroundTasks();
        new Thread(Server::startHttpServer, "http-status").start();
        startRedisServer();
    }

    // ------------------------------------------------------------ HTTP status

    private static void startHttpServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(HTTP_PORT), 0);
            server.createContext("/", exchange -> {
                String response = "{\"status\": \"online\", \"server\": \"Custom Java Redis Engine\", \"tcp_port\": 6379}";
                byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");

                if ("HEAD".equalsIgnoreCase(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(200, -1);
                    exchange.getResponseBody().close();
                } else {
                    exchange.sendResponseHeaders(200, responseBytes.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(responseBytes);
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

    // ------------------------------------------------------------- TCP server

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
        try (socket;
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {
            System.out.println("New TCP client connected: " + socket.getRemoteSocketAddress());
            RespReader reader = new RespReader(in);

            try {
                String[] args;
                while ((args = reader.readCommand()) != null) {
                    if (args.length == 0) continue;
                    if ("QUIT".equalsIgnoreCase(args[0])) {
                        out.write(OK);
                        out.flush();
                        break;
                    }
                    out.write(execute(args, false));
                    out.flush();
                }
            } catch (ProtocolException e) {
                out.write(error("ERR Protocol error: " + e.getMessage()));
                out.flush();
            }
        } catch (IOException e) {
            System.err.println("Client disconnected or error: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------ RESP reader

    /** Reads RESP arrays (binary-safe, length-prefixed) and inline commands from a stream. */
    private static final class RespReader {
        private final InputStream in;
        private long pos;

        RespReader(InputStream in) {
            this.in = in;
        }

        long position() {
            return pos;
        }

        private int read() throws IOException {
            int b = in.read();
            if (b != -1) pos++;
            return b;
        }

        private String readLine() throws IOException {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            int b;
            while ((b = read()) != -1) {
                if (b == '\n') {
                    byte[] arr = buf.toByteArray();
                    int len = arr.length;
                    if (len > 0 && arr[len - 1] == '\r') len--;
                    return new String(arr, 0, len, StandardCharsets.UTF_8);
                }
                buf.write(b);
                if (buf.size() > MAX_LINE) throw new ProtocolException("line too long");
            }
            return buf.size() == 0 ? null : new String(buf.toByteArray(), StandardCharsets.UTF_8);
        }

        private static int parseLength(String s, String what) throws ProtocolException {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                throw new ProtocolException("invalid " + what + " length");
            }
        }

        /** Returns the next command, an empty array for blank input, or null on EOF. */
        String[] readCommand() throws IOException {
            String line = readLine();
            if (line == null) return null;

            // Inline command (e.g. from PowerShell/telnet): "SET key value"
            if (line.isEmpty() || line.charAt(0) != '*') {
                String trimmed = line.trim();
                return trimmed.isEmpty() ? new String[0] : trimmed.split("\\s+");
            }

            // RESP array: *<n>\r\n then n x ($<len>\r\n<bytes>\r\n)
            int n = parseLength(line.substring(1), "multibulk");
            if (n <= 0) return new String[0];
            if (n > MAX_ARGS) throw new ProtocolException("too many arguments");

            String[] args = new String[n];
            for (int i = 0; i < n; i++) {
                String header = readLine();
                if (header == null) throw new EOFException("unexpected end of stream");
                if (header.isEmpty() || header.charAt(0) != '$') {
                    throw new ProtocolException("expected '$' bulk length");
                }
                int len = parseLength(header.substring(1), "bulk");
                if (len < 0 || len > MAX_BULK) throw new ProtocolException("invalid bulk length");

                byte[] data = in.readNBytes(len); // exactly len bytes: binary- and UTF-8-safe
                pos += data.length;
                if (data.length < len) throw new EOFException("unexpected end of stream");
                int cr = read();
                int lf = read();
                if (cr != '\r' || lf != '\n') throw new ProtocolException("bulk string not terminated by CRLF");
                args[i] = new String(data, StandardCharsets.UTF_8);
            }
            return args;
        }
    }

    // ------------------------------------------------------- RESP responses

    private static byte[] simple(String s) {
        return ("+" + s + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] integer(long n) {
        return (":" + n + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] error(String message) {
        return ("-" + message.replaceAll("[\\r\\n]", " ") + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    private static void writeBulk(ByteArrayOutputStream o, byte[] data) {
        byte[] header = ("$" + data.length + "\r\n").getBytes(StandardCharsets.UTF_8);
        o.write(header, 0, header.length);
        o.write(data, 0, data.length);
        o.write(CRLF, 0, CRLF.length);
    }

    /** Bulk string using the UTF-8 BYTE length (not String.length()). Null becomes a nil reply. */
    private static byte[] bulk(String s) {
        if (s == null) return NIL;
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        writeBulk(o, s.getBytes(StandardCharsets.UTF_8));
        return o.toByteArray();
    }

    private static byte[] array(List<String> items) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        byte[] header = ("*" + items.size() + "\r\n").getBytes(StandardCharsets.UTF_8);
        o.write(header, 0, header.length);
        for (String item : items) writeBulk(o, item.getBytes(StandardCharsets.UTF_8));
        return o.toByteArray();
    }

    // ------------------------------------------------------- command dispatch

    private static CmdError err(String message) {
        return new CmdError("ERR " + message);
    }

    private static CmdError wrongType() {
        return new CmdError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    private static void arity(String[] a, int min, int max) {
        if (a.length < min || (max >= 0 && a.length > max)) {
            throw err("wrong number of arguments for '" + a[0].toLowerCase(Locale.ROOT) + "' command");
        }
    }

    private static long parseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw err("value is not an integer or out of range");
        }
    }

    private static byte[] execute(String[] args, boolean replay) {
        String cmd = args[0].toUpperCase(Locale.ROOT);
        try {
            switch (cmd) {
                case "PING":
                    arity(args, 1, 2);
                    return args.length == 2 ? bulk(args[1]) : simple("PONG");
                case "ECHO":
                    arity(args, 2, 2);
                    return bulk(args[1]);
                case "COMMAND":
                    return EMPTY_ARRAY; // lets redis-cli finish its handshake quietly

                // read-only commands (lock-free)
                case "GET":
                    return get(args);
                case "EXISTS":
                    return exists(args);
                case "HGET":
                    return hget(args);
                case "HGETALL":
                    return hgetall(args);

                // write commands (serialised + appended to the AOF)
                case "SET":
                    return doSet(args, replay);
                case "DEL":
                    return write(args, replay, Server::del);
                case "INCR":
                    return write(args, replay, a -> incrBy(a, 1));
                case "DECR":
                    return write(args, replay, a -> incrBy(a, -1));
                case "LPUSH":
                    return write(args, replay, a -> push(a, true));
                case "RPUSH":
                    return write(args, replay, a -> push(a, false));
                case "LPOP":
                    return write(args, replay, a -> pop(a, true));
                case "RPOP":
                    return write(args, replay, a -> pop(a, false));
                case "HSET":
                    return write(args, replay, Server::hset);

                default:
                    return error("ERR unknown command '" + args[0] + "'");
            }
        } catch (CmdError e) {
            return error(e.getMessage());
        } catch (RuntimeException e) {
            System.err.println("Internal error running " + cmd + ": " + e);
            return error("ERR internal error");
        }
    }

    /** Runs a write command under the global lock and logs it to the AOF only if it succeeded. */
    private static byte[] write(String[] args, boolean replay, Function<String[], byte[]> handler) {
        synchronized (LOCK) {
            byte[] reply = handler.apply(args); // throws CmdError on failure -> nothing is logged
            if (!replay) appendAof(args);
            return reply;
        }
    }

    // ---------------------------------------------------------------- key space

    /** Returns the live entry for key, lazily evicting it if it has expired (passive TTL check). */
    private static Entry live(String key) {
        Entry e = store.get(key);
        if (e != null && e.isExpired(System.currentTimeMillis())) {
            store.remove(key, e);
            return null;
        }
        return e;
    }

    private static String stringOf(Entry e) {
        if (e.value instanceof String) return (String) e.value;
        throw wrongType();
    }

    @SuppressWarnings("unchecked")
    private static Deque<String> listOf(Entry e) {
        if (e.value instanceof ConcurrentLinkedDeque) return (Deque<String>) e.value;
        throw wrongType();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> hashOf(Entry e) {
        if (e.value instanceof ConcurrentHashMap) return (Map<String, String>) e.value;
        throw wrongType();
    }

    // ------------------------------------------------------------------ strings

    /**
     * Validates SET and converts any TTL option to an absolute PXAT timestamp, so the AOF
     * replays the exact expiry instead of restarting the countdown after a reboot.
     */
    private static String[] normalizeSet(String[] a) {
        arity(a, 3, -1);
        long expireAt = 0;
        if (a.length > 3) {
            if (a.length != 5) throw err("syntax error");
            long n = parseLong(a[4]);
            long now = System.currentTimeMillis();
            try {
                switch (a[3].toUpperCase(Locale.ROOT)) {
                    case "EX":
                        expireAt = Math.addExact(now, Math.multiplyExact(n, 1000L));
                        break;
                    case "PX":
                        expireAt = Math.addExact(now, n);
                        break;
                    case "PXAT":
                        expireAt = n;
                        break;
                    default:
                        throw err("syntax error");
                }
            } catch (ArithmeticException e) {
                throw err("invalid expire time in 'set' command");
            }
            if (n <= 0 || expireAt <= 0) throw err("invalid expire time in 'set' command");
        }
        return expireAt == 0
                ? new String[]{"SET", a[1], a[2]}
                : new String[]{"SET", a[1], a[2], "PXAT", Long.toString(expireAt)};
    }

    private static byte[] doSet(String[] args, boolean replay) {
        String[] n = normalizeSet(args);
        synchronized (LOCK) {
            long expireAt = n.length == 5 ? Long.parseLong(n[4]) : 0;
            store.put(n[1], new Entry(n[2], expireAt)); // plain SET also clears any old TTL
            if (!replay) appendAof(n);
        }
        return OK;
    }

    private static byte[] get(String[] a) {
        arity(a, 2, 2);
        Entry e = live(a[1]);
        return e == null ? NIL : bulk(stringOf(e));
    }

    private static byte[] exists(String[] a) {
        arity(a, 2, -1);
        long count = 0;
        for (int i = 1; i < a.length; i++) {
            if (live(a[i]) != null) count++;
        }
        return integer(count);
    }

    private static byte[] del(String[] a) {
        arity(a, 2, -1);
        long count = 0;
        for (int i = 1; i < a.length; i++) {
            Entry e = live(a[i]);
            if (e != null && store.remove(a[i], e)) count++;
        }
        return integer(count);
    }

    private static byte[] incrBy(String[] a, long delta) {
        arity(a, 2, 2);
        Entry e = live(a[1]);
        long current = 0;
        long expireAt = 0;
        if (e != null) {
            current = parseLong(stringOf(e));
            expireAt = e.expireAt; // INCR/DECR keep the existing TTL
        }
        long next;
        try {
            next = Math.addExact(current, delta);
        } catch (ArithmeticException ex) {
            throw err("increment or decrement would overflow");
        }
        store.put(a[1], new Entry(Long.toString(next), expireAt));
        return integer(next);
    }

    // -------------------------------------------------------------------- lists

    private static byte[] push(String[] a, boolean left) {
        arity(a, 3, -1);
        Entry e = live(a[1]);
        boolean created = false;
        Deque<String> list;
        if (e == null) {
            list = new ConcurrentLinkedDeque<>();
            created = true;
        } else {
            list = listOf(e);
        }
        for (int i = 2; i < a.length; i++) {
            if (left) list.addFirst(a[i]);
            else list.addLast(a[i]);
        }
        if (created) store.put(a[1], new Entry(list, 0));
        return integer(list.size());
    }

    private static byte[] pop(String[] a, boolean left) {
        arity(a, 2, 2);
        Entry e = live(a[1]);
        if (e == null) return NIL;
        Deque<String> list = listOf(e);
        String value = left ? list.pollFirst() : list.pollLast();
        if (list.isEmpty()) store.remove(a[1], e); // empty lists vanish, like real Redis
        return bulk(value);
    }

    // ------------------------------------------------------------------- hashes

    private static byte[] hset(String[] a) {
        arity(a, 4, -1);
        if (a.length % 2 != 0) throw err("wrong number of arguments for 'hset' command");
        Entry e = live(a[1]);
        boolean created = false;
        Map<String, String> map;
        if (e == null) {
            map = new ConcurrentHashMap<>();
            created = true;
        } else {
            map = hashOf(e);
        }
        long added = 0;
        for (int i = 2; i < a.length; i += 2) {
            if (map.put(a[i], a[i + 1]) == null) added++;
        }
        if (created) store.put(a[1], new Entry(map, 0));
        return integer(added);
    }

    private static byte[] hget(String[] a) {
        arity(a, 3, 3);
        Entry e = live(a[1]);
        if (e == null) return NIL;
        return bulk(hashOf(e).get(a[2]));
    }

    private static byte[] hgetall(String[] a) {
        arity(a, 2, 2);
        Entry e = live(a[1]);
        if (e == null) return EMPTY_ARRAY;
        List<String> flat = new ArrayList<>();
        for (Map.Entry<String, String> field : hashOf(e).entrySet()) {
            flat.add(field.getKey());
            flat.add(field.getValue());
        }
        return array(flat);
    }

    // --------------------------------------------------------- TTL active sweeper

    private static void startBackgroundTasks() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ttl-sweeper");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                store.entrySet().removeIf(en -> en.getValue().isExpired(now)); // active expiry
                syncAof(); // fsync at most once per second
            } catch (Exception e) {
                System.err.println("Background task error: " + e.getMessage());
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    // ---------------------------------------------------------------------- AOF

    /** Appends a command to database.aof in RESP format. Caller must hold LOCK. */
    private static void appendAof(String[] args) {
        if (aofFile == null) return;
        try {
            aofFile.write(array(Arrays.asList(args)));
            aofDirty = true;
        } catch (IOException e) {
            System.err.println("AOF write failed: " + e.getMessage());
        }
    }

    private static void syncAof() {
        FileOutputStream f = aofFile;
        if (f == null || !aofDirty) return;
        aofDirty = false;
        try {
            f.getFD().sync();
        } catch (IOException e) {
            System.err.println("AOF fsync failed: " + e.getMessage());
        }
    }

    /** Replays database.aof into memory at startup; drops a half-written trailing command if any. */
    private static void loadAof() {
        File file = new File(AOF_PATH);
        if (!file.exists()) return;

        long goodOffset = 0;
        long replayed = 0;
        boolean damaged = false;

        try (InputStream raw = new FileInputStream(file)) {
            RespReader reader = new RespReader(new BufferedInputStream(raw));
            String[] cmd;
            while ((cmd = reader.readCommand()) != null) {
                if (cmd.length > 0) {
                    execute(cmd, true);
                    replayed++;
                }
                goodOffset = reader.position();
            }
        } catch (EOFException | ProtocolException e) {
            damaged = true;
            System.err.println("AOF ends with an incomplete/corrupt command (" + e.getMessage() + "); truncating it.");
        } catch (IOException e) {
            System.err.println("Could not read AOF: " + e.getMessage());
            return;
        }

        if (damaged) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(goodOffset);
            } catch (IOException e) {
                System.err.println("Could not truncate AOF: " + e.getMessage());
            }
        }
        System.out.println("AOF loaded: " + replayed + " commands replayed.");
    }
}