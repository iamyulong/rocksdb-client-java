package com.ranksays.rocksdb.client;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import org.json.JSONArray;
import org.json.JSONObject;

public class RocksDB {
	private static final String ENCODING = "UTF-8";

	protected String hostname = "127.0.0.1";
	protected int port = 8516;

	protected boolean authEnabled = false;
	protected String username = null;
	protected String password = null;

	/**
	 * Create a new RocksDB instance with default configuration (localhost and
	 * no authorization).
	 */
	public RocksDB() {
	}

	/**
	 * Create a new RocksDB instance with specified hostname and port
	 * 
	 * @param hostname
	 *            server hostname
	 * @param port
	 *            server port
	 */
	public RocksDB(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}

	/**
	 * Create a new RocksDB instance with specified auth credential.
	 * 
	 * @param username
	 *            auth user name
	 * @param password
	 *            auth password
	 */
	public RocksDB(String username, String password) {
		super();
		this.authEnabled = true;
		this.username = username;
		this.password = password;
	}

	/**
	 * Create a new RocksDB instance.
	 * 
	 * @param hostname
	 *            server hostname
	 * @param port
	 *            server port
	 * @param authEnabled
	 *            is authorization enabled
	 * @param username
	 *            auth user name
	 * @param password
	 *            auth password
	 */
	public RocksDB(String hostname, int port, boolean authEnabled, String username, String password) {
		super();
		this.hostname = hostname;
		this.port = port;
		this.authEnabled = authEnabled;
		this.username = username;
		this.password = password;
	}

	/**
	 * Get value by key in a specified database
	 * 
	 * @param db
	 *            database name
	 * @param key
	 *            key (can not be null)
	 * @return the retrieved value or null if not exists
	 * @throws IOException
	 */
	public byte[] get(String db, byte[] key) throws IOException {
		return getBatch(db, new byte[][] { key })[0];
	}

	/**
	 * Get values by keys in a specified database
	 * 
	 * @param db
	 *            database name
	 * @param keys
	 *            keys (can not be null, length >= 1)
	 * @return the retrieved value or null if not exists
	 * @throws IOException
	 */
	public byte[][] getBatch(String db, byte[][] keys) throws IOException {
		if (db == null || keys == null || keys.length < 1) {
			throw new IllegalArgumentException();
		}
		for (byte[] key : keys) {
			if (key == null) {
				throw new IllegalArgumentException("Key can not be null");
			}
		}
		HttpURLConnection con = openConnection("/get");

		JSONObject req = new JSONObject();
		req.put("db", db);
		JSONArray keyArr = new JSONArray();
		for (byte[] key : keys) {
			keyArr.put(Base64.getEncoder().encodeToString(key));
		}
		req.put("keys", keyArr);
		con.getOutputStream().write(req.toString().getBytes(ENCODING));

		Response resp = Response.fromJSON(parseResponse(con.getInputStream()));

		if (resp.getCode() == Response.CODE_OK) {
			JSONArray arr = (JSONArray) resp.getResults();

			byte[][] values = new byte[arr.length()][];
			for (int i = 0; i < arr.length(); i++) {
				if (arr.isNull(i)) {
					values[i] = null;
				} else {
					values[i] = Base64.getDecoder().decode(arr.getString(i));
				}
			}
			return values;
		} else {
			throw new IOException("code: " + resp.getCode() + ", message: " + resp.getMessage());
		}
	}

	/**
	 * Set the value of specified key.
	 * 
	 * @param db
	 *            database name
	 * @param key
	 *            key (can not be null)
	 * @param value
	 *            value (can not be null)
	 * @throws IOException
	 */
	public void put(String db, byte[] key, byte[] value) throws IOException {
		putBatch(db, new byte[][] { key }, new byte[][] { value });
	}

	/**
	 * Set the values of specified keys.
	 * 
	 * @param db
	 *            database name
	 * @param keys
	 *            keys (can not be null, length >=1)
	 * @param values
	 *            values (can not be null, length >=1)
	 * @throws IOException
	 */
	public void putBatch(String db, byte[][] keys, byte[][] values) throws IOException {
		if (db == null || keys == null || values == null || keys.length < 1 || values.length < 1) {
			throw new IllegalArgumentException();
		}
		for (byte[] key : keys) {
			if (key == null) {
				throw new IllegalArgumentException("Key can not be null");
			}
		}
		for (byte[] value : values) {
			if (value == null) {
				throw new IllegalArgumentException("Value can not be null");
			}
		}
		if (keys.length != values.length) {
			throw new IllegalArgumentException("Number of keys and values does not match");
		}
		HttpURLConnection con = openConnection("/put");

		JSONObject req = new JSONObject();
		req.put("db", db);
		JSONArray keyArr = new JSONArray();
		for (byte[] key : keys) {
			keyArr.put(Base64.getEncoder().encodeToString(key));
		}
		req.put("keys", keyArr);
		JSONArray valueArr = new JSONArray();
		for (byte[] value : values) {
			valueArr.put(Base64.getEncoder().encodeToString(value));
		}
		req.put("values", valueArr);
		con.getOutputStream().write(req.toString().getBytes(ENCODING));

		Response resp = Response.fromJSON(parseResponse(con.getInputStream()));

		if (resp.getCode() != Response.CODE_OK) {
			throw new IOException("code: " + resp.getCode() + ", message: " + resp.getMessage());
		}
	}

	/**
	 * Remove value associated with the specified key.
	 * 
	 * @param db
	 *            database name
	 * @param key
	 *            key (can not be null)
	 * @throws IOException
	 */
	public void remove(String db, byte[] key) throws IOException {
		removeBatch(db, new byte[][] { key });
	}

	/**
	 * Remove values associated with the specified keys.
	 * 
	 * @param db
	 *            database name
	 * @param keys
	 *            keys (can not be null, length >= 1)
	 * @throws IOException
	 */
	public void removeBatch(String db, byte[][] keys) throws IOException {
		if (db == null || keys == null || keys.length < 1) {
			throw new IllegalArgumentException();
		}
		for (byte[] key : keys) {
			if (key == null) {
				throw new IllegalArgumentException("Key can not be null");
			}
		}
		HttpURLConnection con = openConnection("/remove");

		JSONObject req = new JSONObject();
		req.put("db", db);
		JSONArray keyArr = new JSONArray();
		for (byte[] key : keys) {
			keyArr.put(Base64.getEncoder().encodeToString(key));
		}
		req.put("keys", keyArr);
		con.getOutputStream().write(req.toString().getBytes(ENCODING));

		Response resp = Response.fromJSON(parseResponse(con.getInputStream()));

		if (resp.getCode() != Response.CODE_OK) {
			throw new IOException("code: " + resp.getCode() + ", message: " + resp.getMessage());
		}
	}

	/**
	 * Drop a database.
	 * 
	 * @param db
	 *            database name
	 * @throws IOException
	 */
	public void dropDatabase(String db) throws IOException {
		if (db == null) {
			throw new IllegalArgumentException();
		}
		HttpURLConnection con = openConnection("/drop_database");

		JSONObject req = new JSONObject();
		req.put("db", db);
		con.getOutputStream().write(req.toString().getBytes(ENCODING));

		Response resp = Response.fromJSON(parseResponse(con.getInputStream()));

		if (resp.getCode() != Response.CODE_OK) {
			throw new IOException("code: " + resp.getCode() + ", message: " + resp.getMessage());
		}
	}

	private HttpURLConnection openConnection(String uri) throws IOException {
		URL url = new URL("http://" + hostname + ":" + port + uri);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();

		if (authEnabled) {
			String auth = username + ":" + password;
			String authHeader = "Basic " + Base64.getEncoder().encodeToString(auth.getBytes(ENCODING));
			con.setRequestProperty("Authorization", authHeader);
		}

		con.setRequestMethod("POST");
		con.setDoOutput(true);

		return con;
	}

	private JSONObject parseResponse(InputStream input) throws IOException {
		ByteArrayOutputStream buf = new ByteArrayOutputStream();
		BufferedInputStream in = new BufferedInputStream(input);

		for (int c; (c = in.read()) != -1;) {
			buf.write(c);
		}

		return new JSONObject(buf.toString(ENCODING));
	}
}
