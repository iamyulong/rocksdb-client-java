package com.ranksays.rocksdb.client;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

public class RocksDB {
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 8516;
    public static final String ENCODING = "UTF-8";
    public static final int CONNECT_TIMEOUT = 10_000;
    public static final int READ_TIMEOUT = 10_000;

    protected String host;
    protected int port;

    protected boolean authEnabled;
    protected String username;
    protected String password;

    /**
     * Creates a RocksDB instance.
     *
     * @param host        the server host
     * @param port        the server port
     * @param authEnabled whether is authorization enabled
     * @param username    the username
     * @param password    the password
     */
    public RocksDB(String host, int port, boolean authEnabled, String username, String password) {
        this.host = host;
        this.port = port;
        this.authEnabled = authEnabled;
        this.username = username;
        this.password = password;
    }

    public RocksDB(String host, int port, String username, String password) {
        this(host, port, true, username, password);
    }

    public RocksDB(String host, int port) {
        this(host, port, false, null, null);
    }

    public RocksDB(String username, String password) {
        this(DEFAULT_HOST, DEFAULT_PORT, true, username, password);
    }

    public RocksDB() {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }

    public byte[] get(String name, byte[] key) throws IOException {
        return get(name, Collections.singletonList(key)).get(0);
    }

    public String get(String name, String key) throws IOException {
        return decode(get(name, encode(key)));
    }

    public void put(String name, byte[] key, byte[] value) throws IOException {
        put(name, Collections.singletonList(key), Collections.singletonList(value));
    }

    public void put(String name, String key, String value) throws IOException {
        put(name, encode(key), encode(value));
    }

    public void delete(String name, byte[] key) throws IOException {
        delete(name, Collections.singletonList(key));
    }

    public void delete(String name, String key) throws IOException {
        delete(name, encode(key));
    }

    @Deprecated
    public byte[][] getBatch(String name, byte[][] keys) throws IOException {
        return get(name, Arrays.asList(keys)).toArray(new byte[0][]);
    }

    @Deprecated
    public void putBatch(String name, byte[][] keys, byte[][] values) throws IOException {
        put(name, Arrays.asList(keys), Arrays.asList(values));
    }

    @Deprecated
    public void remove(String name, byte[] key) throws IOException {
        delete(name, key);
    }

    @Deprecated
    public void removeBatch(String name, byte[][] keys) throws IOException {
        delete(name, Arrays.asList(keys));
    }

    public byte[] encode(String str) throws UnsupportedEncodingException {
        return str == null ? null : str.getBytes(ENCODING);
    }

    public String decode(byte[] bytes) throws UnsupportedEncodingException {
        return bytes == null ? null : new String(bytes, ENCODING);
    }

    /**
     * Get key-value pairs from a given database.
     *
     * @param name the database name
     * @param keys the keys
     * @throws IOException when an I/O error occurs
     */
    public List<byte[]> get(String name, List<byte[]> keys) throws IOException {
        Objects.requireNonNull(name);
        Objects.requireNonNull(keys);
        keys.forEach(Objects::requireNonNull);

        JSONObject request = new JSONObject();
        request.put("name", name);
        JSONArray array1 = new JSONArray();
        for (byte[] key : keys) {
            array1.put(Base64.getEncoder().encodeToString(key));
        }
        request.put("keys", array1);

        JSONObject response = doRequest("/get", request);

        List<byte[]> values = new ArrayList<>();
        JSONArray array2 = response.getJSONArray("body");
        for (int i = 0; i < array2.length(); i++) {
            values.add(array2.isNull(i) ? null : Base64.getDecoder().decode(array2.getString(i)));
        }
        return values;
    }

    /**
     * Put key-value pairs to a given database.
     *
     * @param name   the database name
     * @param keys   the keys
     * @param values the values
     * @throws IOException when an I/O error occurs
     */
    public void put(String name, List<byte[]> keys, List<byte[]> values) throws IOException {
        Objects.requireNonNull(name);
        Objects.requireNonNull(keys);
        Objects.requireNonNull(values);
        keys.forEach(Objects::requireNonNull);

        JSONObject request = new JSONObject();
        request.put("name", name);
        JSONArray array1 = new JSONArray();
        for (byte[] key : keys) {
            array1.put(Base64.getEncoder().encodeToString(key));
        }
        request.put("keys", array1);
        JSONArray array2 = new JSONArray();
        for (byte[] value : values) {
            array2.put(value == null ? null : Base64.getEncoder().encodeToString(value));
        }
        request.put("values", array2);

        doRequest("/put", request);
    }

    /**
     * Delete key-value pairs from a given database.
     *
     * @param name the database name
     * @param keys the keys
     * @throws IOException when an I/O error occurs
     */
    public void delete(String name, List<byte[]> keys) throws IOException {
        Objects.requireNonNull(name);
        Objects.requireNonNull(keys);
        keys.forEach(Objects::requireNonNull);

        JSONObject request = new JSONObject();
        request.put("name", name);
        JSONArray array1 = new JSONArray();
        for (byte[] key : keys) {
            array1.put(Base64.getEncoder().encodeToString(key));
        }
        request.put("keys", array1);

        doRequest("/delete", request);
    }

    /**
     * Create a database.
     *
     * @param name the database name
     * @throws IOException when an I/O error occurs
     */
    public void createDatabase(String name) throws IOException {
        Objects.requireNonNull(name);

        JSONObject request = new JSONObject();
        request.put("name", name);
        doRequest("/create", request);
    }

    /**
     * Drop a database.
     *
     * @param name the database name
     * @throws IOException when an I/O error occurs
     */
    public void dropDatabase(String name) throws IOException {
        Objects.requireNonNull(name);

        JSONObject request = new JSONObject();
        request.put("name", name);
        doRequest("/drop", request);
    }


    /**
     * Get statistics.
     *
     * @param name the database name
     * @throws IOException when an I/O error occurs
     */
    public String getStats(String name) throws IOException {
        Objects.requireNonNull(name);

        JSONObject request = new JSONObject();
        request.put("name", name);
        JSONObject response = doRequest("/stats", request);

        return response.getString("body");
    }

    protected JSONObject doRequest(String uri, JSONObject request) throws IOException {
        URL url = new URL("http://" + host + ":" + port + uri);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setConnectTimeout(CONNECT_TIMEOUT);
        con.setReadTimeout(READ_TIMEOUT);
        con.setRequestMethod("POST");
        con.setDoOutput(true);

        if (authEnabled) {
            String auth = username + ":" + password;
            con.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(auth.getBytes(ENCODING)));
        }
        con.getOutputStream().write(request.toString().getBytes(ENCODING));

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        BufferedInputStream in = new BufferedInputStream(con.getInputStream());
        for (int c; (c = in.read()) != -1; ) {
            buffer.write(c);
        }

        return new JSONObject(buffer.toString(ENCODING));
    }
}
