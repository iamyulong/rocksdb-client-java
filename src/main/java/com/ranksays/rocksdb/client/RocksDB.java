package com.ranksays.rocksdb.client;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

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
		if (db == null || key == null) {
			throw new IllegalArgumentException();
		}
		HttpURLConnection con = openConnection("/get");

		JSONObject req = new JSONObject();
		req.put("db", db);
		req.put("key", Base64.getEncoder().encodeToString(key));
		con.getOutputStream().write(req.toString().getBytes(ENCODING));

		Response resp = Response.fromJSON(parseResponse(con.getInputStream()));

		if (resp.getCode() == Response.CODE_OK) {
			if (resp.getResults() == null) {
				return null;
			} else {
				return Base64.getDecoder().decode((String) resp.getResults());
			}
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
		if (db == null || key == null || value == null) {
			throw new IllegalArgumentException();
		}
		HttpURLConnection con = openConnection("/put");

		JSONObject req = new JSONObject();
		req.put("db", db);
		req.put("key", Base64.getEncoder().encodeToString(key));
		req.put("value", Base64.getEncoder().encodeToString(value));
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
		if (db == null || key == null) {
			throw new IllegalArgumentException();
		}
		HttpURLConnection con = openConnection("/remove");

		JSONObject req = new JSONObject();
		req.put("db", db);
		req.put("key", Base64.getEncoder().encodeToString(key));
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
