package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/* Author: Ajit Bhat - 50321790
 * Assumptions:
 * No network failures - meaning every message is a success
 * With the only exception being there's a failure
 * But how to make the system network failure tolerant as well ? */

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	static final int QUORUM_N = 3;
	static final int READ_QUORUM = 2;
	static final int FAILED_QUORUM = QUORUM_N - 1;
	static final int DO_INSERT = 200;
	static final int DO_UPDATE = 100;
	static final int DO_QUERY = 300;
	static final int DO_DELETE = 400;
	static final int VAL_IDX = 0;
	static final String ACTUAL_PORT0 = "5554";
	static final String ACTUAL_PORT1 = "5556";
	static final String ACTUAL_PORT2 = "5558";
	static final String ACTUAL_PORT3 = "5560";
	static final String ACTUAL_PORT4 = "5562";
	static final String PORT = "port";
	static final String FROM = "from";
	static final String SEND_PORT = "r_port";
	static final String TYPE = "type";
	static final String LOCAL = "@";
	static final String GLOBAL = "*";
	static final String SELECTION = "selection";
	static final String REPLICA = "replica";
	static final String REPLICATE = "replicate";
	static final String SELF = "self";
	static final String ACK = "ack";
	static final String LOCALIZE = "local";
	static final boolean NO_RECV = false;
	static final boolean REPLICATE_REQ = false;
	static final boolean DO_SYNC = false;
	static final boolean REVERSE = false;
	static final boolean SELF_VAL = false;
	static final boolean GET_LOCAL = true;
	static ArrayList<String> all_ports;

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	static final String URL = "content://" + PROVIDER_NAME;
	static final Uri CONTENT_URI = Uri.parse(URL);

	private SQLiteDatabase db;
	DynamoOp dynamoOp;
	static final String DATABASE_NAME = "DynamoDB";
	static final String TABLE_NAME = "myTable";
	static final String KEY_COLUMN = "key";
	static final String VALUE_COLUMN = "value";
	static final String EPOCH_COLUMN = "epoch";


	private static final String SQL_DELETE_ENTRIES =
			"DROP TABLE IF EXISTS " + TABLE_NAME;

	private static class DatabaseHandler extends SQLiteOpenHelper {

		DatabaseHandler(Context context){
			super(context, DATABASE_NAME, null, 1);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(createTableQuery(TABLE_NAME));
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			db.execSQL(SQL_DELETE_ENTRIES);
			onCreate(db);
		}
	}

	private DatabaseHandler dbHandler;

	private void dbInit() {
		if (db == null) {
			if (dbHandler != null) {
				db = dbHandler.getWritableDatabase();
			}
			else {
				Context context = getContext();
				dbHandler = new DatabaseHandler(context);
			}
		}
	}

	private static String createTableQuery(String tableName) {
		String createQuery = " CREATE TABLE IF NOT EXISTS " + tableName +
				" ( \"" + KEY_COLUMN +"\" TEXT NOT NULL UNIQUE ON CONFLICT REPLACE, " +
				VALUE_COLUMN + " TEXT NOT NULL, " + EPOCH_COLUMN + " TEXT NOT NULL);";
		Log.i(TAG, "createTableQuery: " + createQuery);
		return createQuery;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		dbInit();
		Log.i(TAG, "onCreate: I am alive, hue hue hue");

		all_ports = new ArrayList<String>();
		all_ports.add(ACTUAL_PORT0);
		all_ports.add(ACTUAL_PORT1);
		all_ports.add(ACTUAL_PORT2);
		all_ports.add(ACTUAL_PORT3);
		all_ports.add(ACTUAL_PORT4);

		// Get my port number
		Context context = getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		// initialize the dynamo
		dynamoOp = new DynamoOp(portStr);
		// initialize the directory
		dynamoOp.initDynamo();
		dynamoOp.addTable(dynamoOp.getWhoami(), TABLE_NAME);

		String prev = dynamoOp.getPrev(dynamoOp.getWhoami());
		createTable(prev);
		Log.i(TAG, "onCreate: " + dynamoOp.getTableName(prev));
		prev = dynamoOp.getPrev(prev);
		createTable(prev);


		// Create server socket and server thread
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			Log.i(TAG, "onCreate: Created server socket");
			Thread serverThread = new ServerThread(serverSocket);
			serverThread.start();
			// asynctask to send addTable request
			// asynctask or thread ? why ?
			// maybe generic function for quorum completion ? or not ?
			// find a way to build message efficiently
			// concurrency ? what ?
			// test and see! p-2
			// object versioning, why ? and how to store in table ??
		} catch (IOException e) {
			Log.i(TAG, "onCreate: Could not create server socket!");
		}

		(new syncThread()).start();

		return true;
	}


	boolean hasRecovered() {
		String count = "SELECT * FROM " + dynamoOp.getTableName(dynamoOp.getWhoami());
		Cursor cursor = db.rawQuery(count, null);
		int rowCount = 0;
		while(cursor.moveToNext()) {
			rowCount++;
		}
		cursor.close();
		return rowCount > 0;
	}

	void createTable(String from) {
		dbInit();
		Log.i(TAG, "createTable: " + dynamoOp.getTableLength());
		Log.i(TAG, "createTable: For: " + from);
		String tableName = REPLICA + dynamoOp.getTableLength();
		db.execSQL(createTableQuery(tableName));
		dynamoOp.addTable(from, tableName);
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		dbInit();
		return deleteValues(selection);
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		dbInit();
		insertValues(values);
		return uri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		dbInit();
		return getValues(selection);
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	class DynamoOp {
		private String whoami;
		private HashMap<String, HashMap<String, String>> nodeDirectory;
		private ArrayList<String> nodeList;
		private HashMap<String, String> tableLookup;

		DynamoOp(String me) {
			try {
				this.whoami = genHash(me);
				this.nodeDirectory = new HashMap<String, HashMap<String, String>>();
				this.nodeList = new ArrayList<String>();
				this.tableLookup = new HashMap<String, String>();
			} catch (NoSuchAlgorithmException e) {
				Log.i(TAG, "DynamoOp: Failed to get hash");
			}
		}

		void initDynamo() {
			try {
				for (String port: all_ports) {
					String hash = genHash(port);
					HashMap<String, String> nodeInfo = new HashMap<String, String>();
					nodeInfo.put(PORT, port);
					nodeInfo.put(SEND_PORT, String.valueOf((Integer.parseInt(port) * 2)));
					nodeDirectory.put(hash, nodeInfo);
					nodeList.add(hash);
				}
				// sort the nodeList based on hash value after everything is done!
				Collections.sort(nodeList);
			} catch (NoSuchAlgorithmException e) {
				Log.i(TAG, "initDynamo: Failed to get hash");
			}
		}

		ArrayList<String> getOthers() {
			ArrayList<String> cloneList = (ArrayList<String>) nodeList.clone();
			cloneList.remove(dynamoOp.getWhoami());
			return cloneList;
		}

		String getWhoami() {
			return whoami;
		}
		int getNodeListLength() {
			return nodeList.size();
		}
		HashMap<String, String> getTableLookup() {
			return tableLookup;
		}
		String getSendPort(String key) {
			return nodeDirectory.get(key).get(SEND_PORT);
		}
		void addTable(String key, String tableName) {
			tableLookup.put(key, tableName);
		}

		boolean checkTable(String key) {
			return tableLookup.containsKey(key);
		}

		String getTableName(String key) {
			return tableLookup.get(key);
		}

		int getTableLength() {
			return tableLookup.size();
		}

		String getNext(String key) {
			int index = nodeList.indexOf(key);
			int nextIndex = (index + 1) % getNodeListLength();
			return nodeList.get(nextIndex);
		}

		String getPrev(String key) {
			int index = nodeList.indexOf(key);
			if (index == 0) {
				return nodeList.get(getNodeListLength()-1);
			}
			int prevIndex = (index - 1) % getNodeListLength();
			return nodeList.get(prevIndex);
		}

		String getTargetNode(String key) {
			// perform binary search on list when the number of nodes increase!
			// only feasible because list size is very small
			ArrayList<String> cloneList = (ArrayList<String>) nodeList.clone();
			cloneList.add(key);
			Collections.sort(cloneList);
			int target = cloneList.indexOf(key);
			cloneList.remove(key);
			if (target == getNodeListLength()) {
				// means it was inserted at the end
				return cloneList.get(0);
			}
			return cloneList.get(target);
		}
	}

	class ServerThread extends Thread {
		ServerSocket serverSocket;

		ServerThread(ServerSocket serverSocket) {
			dbInit();
			Log.i(TAG, "ServerThread: Started!");
			this.serverSocket = serverSocket;
		}

		void doInsert(Socket socket, JSONObject jsonObject) {
			try {
				boolean replicate = jsonObject.getBoolean(REPLICATE);
				String from = jsonObject.getString(FROM);
				Log.i(TAG, "doInsert: Called at: " + dynamoOp.getWhoami() + " From: " + from);
				String key = jsonObject.getString(KEY_COLUMN);
				String value = jsonObject.getString(VALUE_COLUMN);
				String epoch = jsonObject.getString(EPOCH_COLUMN);
				ContentValues values = getContentValues(key, value, epoch);
				if (replicate) {
					Log.i(TAG, "doInsert: TABLE: " + from);
					Log.i(TAG, "doInsert: Replicate");
					insertIntoTable(dynamoOp.getTableName(from), values);
//					sendAck(socket);
				} else {
					Log.i(TAG, "doInsert: Self for key: " + key);
					insertIntoTable(dynamoOp.getTableName(dynamoOp.getWhoami()), values);
					insertReplicate(jsonObject, dynamoOp.getWhoami());
					sendAck(socket);
				}
			} catch (JSONException e) {
				Log.i(TAG, "doInsert: Error in JSON");
			} catch (SQLException e) {
				Log.i(TAG, "doInsert: SQL exception");
			}
		}

		void doQuery(Socket socket, JSONObject jsonObject) {
			try {
				String selection = jsonObject.getString(SELECTION);
				boolean repl = jsonObject.getBoolean(REPLICATE);
				JSONObject message;
				if (repl) {
					// TODO sanity check if table exists
					boolean self = jsonObject.getBoolean(SELF);
					boolean getLocal = jsonObject.getBoolean(LOCALIZE);
					if (getLocal) {
						selection = LOCAL;
					}
					Log.i(TAG, "doQuery: SELF value: "+self);
					if (self) {
						Log.i(TAG, "doQuery: Return my table: "+" from: "+jsonObject.getString(FROM));
						Log.i(TAG, "doQuery: For selection: "+selection);
						Cursor cursor = getValueFromTable(dynamoOp.getTableName(dynamoOp.getWhoami()), selection);
						message = buildCursorJson(cursor);
					} else {
						String targetNode = jsonObject.getString(FROM);
						Log.i(TAG, "doQuery: Return table at: "+" from: "+jsonObject.getString(FROM));
						Log.i(TAG, "doQuery: For selection: "+selection);
						Cursor cursor = getValueFromTable(dynamoOp.getTableName(targetNode), selection);
						message = buildCursorJson(cursor);
						Log.i(TAG, "doQuery: message: "+message);
					}
				} else {
					Log.i(TAG, "doQuery: Got: "+selection + " from: "+jsonObject.get(FROM));
					Cursor cursor = getLatestValues(jsonObject, selection, DO_SYNC);
					message = buildCursorJson(cursor);
					Log.i(TAG, "doQuery: Sending message: "+message);
				}
				sendMessage(socket, message);
			} catch (JSONException e) {
				Log.i(TAG, "doQuery: Error in JSON");
			}
		}

		void doDelete(Socket socket, JSONObject jsonObject) {
			try {
				String selection = jsonObject.getString(SELECTION);
				boolean repl = jsonObject.getBoolean(REPLICATE);
				String targetNode = jsonObject.getString(FROM);
				if (repl) {
					Log.i(TAG, "doDelete: From: " + targetNode);
					if (dynamoOp.checkTable(targetNode)) {
						deleteFromTable(dynamoOp.getTableName(targetNode), selection);
					}
				} else {
					// TODO separate thread?
					sendAck(socket);
					String next = dynamoOp.getNext(targetNode);
					jsonObject = prepareRep(jsonObject, targetNode);
					deleteFromTable(dynamoOp.getTableName(dynamoOp.getWhoami()), selection);
					for (int i = 0; i < READ_QUORUM; i++) {
						sendAndReceive(jsonObject, dynamoOp.getSendPort(next), NO_RECV);
						next = dynamoOp.getNext(next);
					}
				}
			} catch (JSONException e) {
				Log.i(TAG, "doDelete: Error in JSON");
			}
		}

		void doUpdate(JSONObject jsonObject) {
			Log.i(TAG, "doUpdate: Called!");
			try {
				JSONArray keys = jsonObject.getJSONArray(KEY_COLUMN);
				JSONArray values = jsonObject.getJSONArray(VALUE_COLUMN);
				JSONArray epochs = jsonObject.getJSONArray(EPOCH_COLUMN);
				String tableName = dynamoOp.getTableName(jsonObject.getString(FROM));
				Log.i(TAG, "doUpdate: Is table: "+tableName+" available at: "+dynamoOp.getWhoami());
				if (dynamoOp.checkTable(tableName)) {
					if (keys.length() == values.length() && values.length() == epochs.length()) {
						for (int i = 0; i < keys.length(); i++) {
							Log.i(TAG, "doUpdate: Calling update");
							updateValues(tableName, getContentValues(keys.getString(i), values.getString(i), epochs.getString(i)));
						}
					} else {
						Log.i(TAG, "doUpdate: Error IN UPDATE");
					}
				} else {
					Log.i(TAG, "doUpdate: Table does not exist");
				}

			} catch (JSONException e) {
				Log.i(TAG, "doUpdate: Error in json");
			}
		}

		@Override
		public void run() {
			while(true) {
				try {
					Socket socket = serverSocket.accept();
					BufferedReader inStream = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String jsonString = inStream.readLine();
					try {
						JSONObject jsonObject = new JSONObject(jsonString);
						int type = jsonObject.getInt(TYPE);
						switch (type) {
							case DO_INSERT: doInsert(socket, jsonObject);
								break;
							case DO_QUERY: doQuery(socket, jsonObject);
								break;
							case DO_DELETE: doDelete(socket, jsonObject);
								break;
							case DO_UPDATE: doUpdate(jsonObject);
								break;
						}
					} catch (JSONException j) {
						Log.i(TAG, "run: Error in parsing JSON");
					}
				} catch (IOException e) {
					Log.i(TAG, "run: Error in accepting connections!");
				}
			}
		}
	}

	class ClientInsert extends Thread {
		private JSONObject jsonObject;
		private String port;
		private String node;
		ClientInsert(JSONObject jsonObject, String node, String port) {
			this.jsonObject = jsonObject;
			this.node = node;
			this.port = port;
		}

		@Override
		public void run() {
			if (sendAndReceive(jsonObject, port, !NO_RECV) == null) {
				Log.i(TAG, "run: Target may have failed");
				insertReplicate(jsonObject, node);
			}
		}
	}

	void sendMessage(Socket socket, JSONObject jsonObject) {
		try {
			BufferedWriter outStream = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
			outStream.write(jsonObject.toString());
			outStream.newLine();
			outStream.flush();
		} catch (IOException e) {
			Log.i(TAG, "sendMessage: Could not send message!");
		}
	}

	JSONObject buildAck() {
		try {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put(ACK, ACK);
			return jsonObject;
		} catch (JSONException e) {
			Log.i(TAG, "buildAck: Error in JSON");
		}
		return null;
	}

	void sendAck(Socket socket) {
		try {
			BufferedWriter outStream = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
			outStream.write(buildAck().toString());
			outStream.newLine();
			outStream.flush();
		} catch (IOException e) {
			Log.i(TAG, "sendAck: Could not send message!");
		}
	}

	JSONObject sendAndReceive(JSONObject jsonObject, String port, boolean recv) {
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port));
			sendMessage(socket, jsonObject);
			// socket.setSoTimeout(500);
			if (recv) {
				String res;
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				try {
					if ((res = in.readLine()) != null) {
						socket.close();
						return new JSONObject(res);
					} else {
						socket.close();
						return null;
					}
				} catch (JSONException e) {
					Log.i(TAG, "sendAndReceive: Error in JSON");
				}
			}
			socket.close();
		} catch (UnknownHostException e) {
			Log.i(TAG, "sendAndReceive: Unknown Address");
		} catch (IOException e) {
			Log.i(TAG, "sendAndReceive: Error in opening connection: " + port);
		} catch (Exception e) {
			Log.i(TAG, "sendAndReceive: What error did i catch?");
		}
		return null;
	}

	long getCurrentEpoch() {
		return System.currentTimeMillis();
	}

	boolean compareEpoch(long first, long second) {
		return first > second;
	}

	ContentValues getContentValues(String key, String value, String epoch) {
		ContentValues contentValues = new ContentValues();
		contentValues.put(KEY_COLUMN, key);
		contentValues.put(VALUE_COLUMN, value);
		contentValues.put(EPOCH_COLUMN, epoch);
		return contentValues;
	}

	JSONObject extractContentValues(ContentValues values, String epoch) {
		JSONObject jsonObject = new JSONObject();
		String key = values.getAsString(KEY_COLUMN);
		String value = values.getAsString(VALUE_COLUMN);
		try {
			jsonObject.put(KEY_COLUMN, key);
			jsonObject.put(VALUE_COLUMN, value);
			jsonObject.put(EPOCH_COLUMN, epoch);
			return jsonObject;
		} catch (JSONException e) {
			Log.i(TAG, "extractContentValues: JSON exception");
		}
		return null;
	}

	JSONObject buildJson(JSONObject jsonObject, int type) {
		try {
			jsonObject.put(FROM, dynamoOp.getWhoami());
			jsonObject.put(TYPE, type);
			jsonObject.put(REPLICATE, REPLICATE_REQ);
			return jsonObject;
		} catch (JSONException e) {
			Log.i(TAG, "buildJson: Error in building json string");
		}
		return null;
	}

	JSONObject queryJson(String selection, boolean ...local) {
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put(SELECTION, selection);
			if (local == null || local.length == 0) {
				jsonObject.put(LOCALIZE, GET_LOCAL);
			} else {
				jsonObject.put(LOCALIZE, local[0]);
			}
			return jsonObject;
		} catch (JSONException e) {
			Log.i(TAG, "queryJson: Error in creating JSON");
		}
		return null;
	}

	Cursor buildCursor(JSONObject jsonObject) {

		try {
			Log.i(TAG, "buildCursor: Start");
			if (jsonObject == null || jsonObject.length() == 0) {
				return new MatrixCursor(new String[] {KEY_COLUMN, VALUE_COLUMN, EPOCH_COLUMN});
			}
			JSONArray keys = jsonObject.getJSONArray(KEY_COLUMN);
			Log.i(TAG, "buildCursor: Keys: "+ keys);
			JSONArray values = jsonObject.getJSONArray(VALUE_COLUMN);
			JSONArray epochs = jsonObject.getJSONArray(EPOCH_COLUMN);
			MatrixCursor cursor = new MatrixCursor(new String[] {KEY_COLUMN, VALUE_COLUMN, EPOCH_COLUMN});
			for (int i = 0; i < keys.length(); i++) {
				cursor.newRow()
						.add(KEY_COLUMN, keys.get(i))
						.add(VALUE_COLUMN, values.get(i))
						.add(EPOCH_COLUMN, epochs.get(i));
			}
			Log.i(TAG, "buildCursor: Fin.");
			return cursor;
		} catch (JSONException e) {
			Log.i(TAG, "buildCursor: Error in parsing JSON");
		}
		return null;
	}

	JSONObject buildCursorJson(Cursor cursor, JSONObject ...jsonObjects) {
		try {
			Log.i(TAG, "buildCursorJson: Start");
			// if we were to pass json with values already ?
			JSONObject jsonObject;
			if (jsonObjects == null || jsonObjects.length == 0) {
//				Log.i(TAG, "buildCursorJson: ?? is this right");
				jsonObject = new JSONObject();
			} else {
				jsonObject = jsonObjects[0];
			}
			JSONArray keys;
			JSONArray values;
			JSONArray epochs;
			if (jsonObject.has(KEY_COLUMN) && jsonObject.has(VALUE_COLUMN) && jsonObject.has(EPOCH_COLUMN)) {
				keys = jsonObject.getJSONArray(KEY_COLUMN);
				values = jsonObject.getJSONArray(VALUE_COLUMN);
				epochs = jsonObject.getJSONArray(EPOCH_COLUMN);
			} else {
				keys = new JSONArray();
				values = new JSONArray();
				epochs = new JSONArray();
			}

			if (cursor != null) {
				int keyIndex = cursor.getColumnIndex(KEY_COLUMN);
				int valueIndex = cursor.getColumnIndex(VALUE_COLUMN);
				int epochIndex = cursor.getColumnIndex(EPOCH_COLUMN);
				while(cursor.moveToNext()) {
					keys.put(cursor.getString(keyIndex));
					values.put(cursor.getString(valueIndex));
					epochs.put(cursor.getString(epochIndex));
				}
				cursor.close();
			}

			jsonObject.put(KEY_COLUMN, keys);
			jsonObject.put(VALUE_COLUMN, values);
			jsonObject.put(EPOCH_COLUMN, epochs);
			Log.i(TAG, "buildCursorJson: Keys:"+keys);
			Log.i(TAG, "buildCursorJson: Fin.");
			return jsonObject;
		} catch (JSONException e) {
			Log.i(TAG, "buildJSONCursor: Error in building JSON");
		}
		return null;
	}

	String getKeyHash(String key) {
		try {
			return genHash(key);
		} catch (NoSuchAlgorithmException e) {
			Log.i(TAG, "getKeyHash: Error in generating hash for key");
		}
		return null;
	}

	boolean itMe(String one, String two) {
		return one.equals(two);
	}

	void insertIntoTable(String tableName, ContentValues values) throws SQLException {
		long rowID = db.insert(tableName, null, values);
		if(rowID < 0) {
			throw new SQLException("Could not add record to database!");
		}
		Log.v("insert key: ", values.toString() + " at: " + dynamoOp.getWhoami());
		// return CONTENT_URI;
	}

	ContentValues addEpoch(ContentValues values, String epoch) {
		values.put(EPOCH_COLUMN, epoch);
		return values;
	}

	void insertValues(ContentValues values) {
		// if this a successor of the targetNode
		// then we don't have to make additional call back from target node
		// optimization TODO
		String keyToInsert = values.getAsString(KEY_COLUMN);
		String keyInQuestion = getKeyHash(keyToInsert);
		String targetNode = dynamoOp.getTargetNode(keyInQuestion);
		String epoch = Long.toString(getCurrentEpoch());
		JSONObject jsonObject = extractContentValues(values, epoch);
		jsonObject = buildJson(jsonObject, DO_INSERT);
		if (itMe(dynamoOp.getWhoami(), targetNode)) {
			try {
				insertIntoTable(dynamoOp.getTableName(targetNode), addEpoch(values, epoch));
				insertReplicate(jsonObject, targetNode);
			} catch (SQLException e) {
				Log.i(TAG, "insertValues: Error while inserting values");
			}
		} else {
			// sending directly to target node
			String targetPort = dynamoOp.getSendPort(targetNode);
			// not feasible for scale
			// what would be a better way ?
			if (sendAndReceive(jsonObject, targetPort, !NO_RECV) == null) {
				Log.i(TAG, "run: Target may have failed");
				insertReplicate(jsonObject, targetNode);
			}
			// new ClientInsert(jsonObject, targetNode, targetPort).start();
		}
		Log.i(TAG, "insertValues: Finished");
	}

	JSONObject prepareRep(JSONObject jsonObject, String targetNode, boolean ...self) {
		try {
			// check the from value for this! when you are passing it along backwards
			Log.i(TAG, "prepareRep: "+FROM+" "+targetNode);
			jsonObject.put(FROM, targetNode);
			jsonObject.put(REPLICATE, !REPLICATE_REQ);
			if (self == null || self.length == 0) {
				jsonObject.put(SELF, SELF_VAL);
			} else {
				jsonObject.put(SELF, self[0]);
			}
			return jsonObject;
		} catch (JSONException e) {
			Log.i(TAG, "prepareRep: Error in JSON");
		}
		return null;
	}

	void insertReplicate(JSONObject jsonObject, String targetNode) {
		// pass the node value which is to be replicated!
		Log.i(TAG, "insertReplicate: Calling at: "+dynamoOp.getWhoami());
		String next = dynamoOp.getNext(targetNode);
		jsonObject = prepareRep(jsonObject, targetNode);
		for (int i = 0; i < FAILED_QUORUM; i++) {
			// maybe add check to see if destination got message or not
//			JSONObject jsonObject1 = sendAndReceive(jsonObject, dynamoOp.getSendPort(next), !NO_RECV);
			sendAndReceive(jsonObject, dynamoOp.getSendPort(next), NO_RECV);
			next = dynamoOp.getNext(next);
		}
	}

	Cursor getValueFromTable(String tableName, String selection) {
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(tableName);
		if (selection.equals(LOCAL)) {
			return queryBuilder.query(db, null, null, null, null ,null, null);
		} else {
			
			String[] keyValueToSearch = {selection};
			selection = "\"" + KEY_COLUMN + "\"" + " = ?";
			Log.i(TAG, "getValueFromTable: Will return!");
			return queryBuilder.query(db, null, selection, keyValueToSearch, null, null, null);
		}
	}

	Cursor mergeCursors(ArrayList<Cursor> other) {
		return new MergeCursor(other.toArray(new Cursor[0]));
	}

	Cursor getValues(String selection) {
		Log.i(TAG, "getValues: Query called at: " + dynamoOp.getWhoami() + " with select: "+ selection);
		// JSONObject jsonObject = queryJson(selection);
		// latest: jsonObject = buildJson(jsonObject, DO_QUERY);
		if (selection.equals(LOCAL)) {
			return getAllLocal();
		} else if (selection.equals(GLOBAL)) {
			// JSONObject jsonObject = queryJson(selection);
			// Cursor myCursor = getLatestValues(jsonObject, LOCAL, DO_SYNC);
			Cursor myCursor = getAllLocal();
			ArrayList<String> otherNodes = dynamoOp.getOthers();
			JSONObject jsonObject = buildJson(queryJson(LOCAL), DO_QUERY);
			// TODO replace normal arrays with ArrayList!
			ArrayList<Cursor> otherCursors = new ArrayList<Cursor>();
			otherCursors.add(myCursor);
			for (String other: otherNodes) {
				JSONObject jsonObject1 = sendAndReceive(prepareRep(jsonObject, dynamoOp.getWhoami(), !SELF_VAL), dynamoOp.getSendPort(other), !NO_RECV);
				Cursor otherCursor;
				if (jsonObject1 == null || jsonObject1.length() == 0) {
					Log.i(TAG, "getValues: GLOBAL: ");
					Cursor[] cursors = queryReplicate(prepareRep(jsonObject, other, SELF_VAL), other, REVERSE);
					otherCursor = filterLatestValues(cursors, other);
				} else {
					Log.i(TAG, "getValues: GLOBAL!: From: "+other+" Map: "+jsonObject1);
					otherCursor = buildCursor(jsonObject1);
				}
				otherCursors.add(otherCursor);
			}
			return filterCursor(mergeCursors(otherCursors));
		} else {
			String keyInQuestion = getKeyHash(selection);
			String targetNode = dynamoOp.getTargetNode(keyInQuestion);
			JSONObject jsonObject = queryJson(selection, !GET_LOCAL);
			jsonObject = buildJson(jsonObject, DO_QUERY);
			if (itMe(dynamoOp.getWhoami(), targetNode)) {
				return filterCursor(getLatestValues(jsonObject, selection, DO_SYNC));
			} else {
				// TODO IMPORTANT
				// return from replicas if main is not available
				Log.i(TAG, "getValues: Sending");
				JSONObject jsonObject1 = sendAndReceive(jsonObject, dynamoOp.getSendPort(targetNode), !NO_RECV);
				Log.i(TAG, "getValues: RECEIVED");
				if (jsonObject1 == null || jsonObject1.length() == 0) {
					Log.i(TAG, "getValues: THERE IS A FAILURE, CALLED for selection: "+selection);
					try {
						Log.i(TAG, "getValues: Selection in json: "+jsonObject.getString(SELECTION));
					} catch (JSONException e) {
						Log.i(TAG, "getValues: ");
					}
					Cursor[] cursors = queryReplicate(jsonObject, targetNode, REVERSE);
					Log.i(TAG, "getValues: Returning! main failed");
					return filterCursor(filterLatestValues(cursors, targetNode));
				} else {
					Log.i(TAG, "getValues: Returning! no fails");
					return filterCursor(buildCursor(jsonObject1));
				}
				// return filterCursor(buildCursor());
			}
		}
	}

	Cursor filterLatestValues(Cursor[] cursors, String target) {
		String successor = dynamoOp.getNext(target);
		// for generalization
		int idx = 0;
		HashMap<String, ArrayList<String>> compareMap = cursorToMap(cursors[idx++]);
		HashMap<String, ArrayList<String>> finalMap = new HashMap<String, ArrayList<String>>();
		for (int i = idx; i < READ_QUORUM; i++) {
			HashMap<String, ArrayList<String>> thisMap = cursorToMap(cursors[i]);
			for (String key: compareMap.keySet()) {
				ArrayList<String> selfVal = compareMap.get(key);
				ArrayList<String> otherVal = thisMap.get(key);
				long epochSelf = Long.parseLong(selfVal.get(VAL_IDX+1));
				long epochOther = Long.parseLong(otherVal.get(VAL_IDX+1));
				ArrayList<String> newVal = new ArrayList<String>();
				if (epochOther > epochSelf) {
					newVal.add(otherVal.get(VAL_IDX));
					newVal.add(otherVal.get(VAL_IDX+1));
					finalMap.put(key, newVal);
				} else {
					newVal.add(selfVal.get(VAL_IDX));
					newVal.add(selfVal.get(VAL_IDX+1));
					finalMap.put(key, newVal);
				}
				// TODO make it general enough to work for any number of quorums
				// TODO NoteToSelf: and there's too much code replication yo!
			}
		}

		Log.i(TAG, "filterLatestValues: Values: "+finalMap);
		return mapToCursor(finalMap);
	}

	int deleteFromTable(String tableName, String selection) {
		int numRows = 0;
		// code replication
		Log.i(TAG, "deleteFromTable: Selection: " + selection);
		Log.i(TAG, "deleteFromTable: TABLE recv: " + tableName);
		if (selection.equals(LOCAL)) {
			for (String key: dynamoOp.getTableLookup().keySet()) {
				numRows = db.delete(dynamoOp.getTableName(key), null, null);
			}
		} else if (selection.equals(GLOBAL)) {
			for (String key: dynamoOp.getTableLookup().keySet()) {
				numRows = db.delete(dynamoOp.getTableName(key), null, null);
			}
		} else {
			String[] keyValueToSearch = {selection};
			selection = "\"" + KEY_COLUMN + "\"" + " = ?";
			Log.i(TAG, "deleteFromTable: HERE");
			numRows = db.delete(tableName, selection, keyValueToSearch);
		}
		Log.i(TAG, "deleteFromTable: After: numrows "+numRows);
		return numRows;
	}

	int deleteValues(String selection) {
		// TODO optimize delete
		dbInit();
		int numRows;
		if (selection.equals(LOCAL)) {
			numRows = deleteFromTable(dynamoOp.getTableName(dynamoOp.getWhoami()), selection);
			return numRows;
		} else if (selection.equals(GLOBAL)) {
			numRows = deleteFromTable(dynamoOp.getTableName(dynamoOp.getWhoami()), selection);
			JSONObject jsonObject = buildJson(queryJson(selection), DO_DELETE);
			for (String other: dynamoOp.getOthers()) {
				// TODO in separate thread
				sendAndReceive(jsonObject, dynamoOp.getSendPort(other), NO_RECV);
			}
			return numRows;
		} else {
			String keyInQuestion = getKeyHash(selection);
			String targetNode = dynamoOp.getTargetNode(keyInQuestion);
			JSONObject jsonObject = buildJson(queryJson(selection), DO_DELETE);
			if (itMe(dynamoOp.getWhoami(), targetNode)) {
				numRows = deleteFromTable(dynamoOp.getTableName(targetNode), selection);
				String next = dynamoOp.getNext(targetNode);
				jsonObject = prepareRep(jsonObject, targetNode);
				for (int i = 0; i < READ_QUORUM; i++) {
					sendAndReceive(jsonObject, dynamoOp.getSendPort(next), NO_RECV);
					next = dynamoOp.getNext(next);
				}
				return numRows;
			} else {
				// TODO in separate thread
				sendAndReceive(jsonObject, dynamoOp.getSendPort(targetNode), !NO_RECV);
				jsonObject = prepareRep(jsonObject, targetNode);
				String next = dynamoOp.getNext(targetNode);
				for (int i = 0; i < READ_QUORUM; i++) {
					sendAndReceive(jsonObject, dynamoOp.getSendPort(next), NO_RECV);
					next = dynamoOp.getNext(next);
				}
			}
		}
		return 0;
	}

	void updateValues(String tableName, ContentValues values) {
		// call update
		// update latest values to successor nodes as well
		// TODO too much repetition in tasks, pipeline it!
		long epoch = values.getAsLong(EPOCH_COLUMN);
		String key = values.getAsString(KEY_COLUMN);
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(tableName);
		String[] keyValueToSearch = {key};
		String selection = "\"" + KEY_COLUMN + "\"" + " = ?";
		Cursor cursor = queryBuilder.query(db, null, selection, keyValueToSearch, null, null, null);
		int keyIndex = cursor.getColumnIndex(KEY_COLUMN);
		int epochIndex = cursor.getColumnIndex(EPOCH_COLUMN);
		while (cursor.moveToNext()) {
			String thisKey = cursor.getString(keyIndex);
			long thisEpoch = Long.parseLong(cursor.getString(epochIndex));
			if (thisKey.equals(key)) {
				if (thisEpoch < epoch) {
					db.replace(tableName, null, values);
				}
			}
		}
		Log.i(TAG, "updateValues: Success!");
	}

	class updateThread extends Thread {
		HashMap<String, ArrayList<String>> map;
		String tableName;
		updateThread(HashMap<String, ArrayList<String>> map) {
			this.map = map;
			this.tableName = dynamoOp.getTableName(dynamoOp.getWhoami());
		}

		@Override
		public void run() {
			for (String key: map.keySet()) {
				String value = map.get(key).get(VAL_IDX);
				String epoch = map.get(key).get(VAL_IDX+1);
				updateValues(tableName, getContentValues(key, value, epoch));
			}
		}
	}

	Cursor[] queryReplicate(JSONObject jsonObject, String targetNode, boolean reverse) {
		Cursor[] result = new Cursor[READ_QUORUM];
		if (reverse) {
			jsonObject = prepareRep(jsonObject, targetNode, !SELF_VAL);
			String prev = dynamoOp.getPrev(targetNode);
			for (int i = 0; i < READ_QUORUM; i++) {
				Log.i(TAG, "queryReplicate: Prev: "+prev);
				JSONObject res = sendAndReceive(jsonObject, dynamoOp.getSendPort(prev), !NO_RECV);
				if (res == null || res.length() == 0) {
					result[i] = null;
				} else {
					result[i] = buildCursor(res);
				}
				prev = dynamoOp.getPrev(prev);
			}
		} else {
			String next = dynamoOp.getNext(targetNode);
			jsonObject = prepareRep(jsonObject, targetNode);
			for (int i = 0; i < READ_QUORUM; i++) {
				Log.i(TAG, "queryReplicate: Next: "+next);
				JSONObject res = sendAndReceive(jsonObject, dynamoOp.getSendPort(next), !NO_RECV);
				if (res == null || res.length() == 0) {
					Log.i(TAG, "queryReplicate: its null");
					result[i] = null;
				} else {
					result[i] = buildCursor(res);
				}
				next = dynamoOp.getNext(next);
			}
		}
		Log.i(TAG, "queryReplicate: Done");
		return result;
	}



	JSONObject buildMapJson(HashMap<String, ArrayList<String>> map) {
		try {
			Log.i(TAG, "buildMapJson: Start");
			// if we were to pass json with values already ?
			JSONObject jsonObject = new JSONObject();
			JSONArray keys = new JSONArray();
			JSONArray values = new JSONArray();
			JSONArray epochs = new JSONArray();

			for (String key: map.keySet()) {
				String value = map.get(key).get(VAL_IDX);
				String epoch = map.get(key).get(VAL_IDX+1);
				keys.put(key);
				values.put(value);
				epochs.put(epoch);
			}

			jsonObject.put(KEY_COLUMN, keys);
			jsonObject.put(VALUE_COLUMN, values);
			jsonObject.put(EPOCH_COLUMN, epochs);
			Log.i(TAG, "buildMapJson: Fin.");
			return jsonObject;
		} catch (JSONException e) {
			Log.i(TAG, "buildMapJson: Error in building JSON");
		}
		return null;
	}

	HashMap<String, ArrayList<String>> cursorToMap(Cursor cursor) {
		HashMap<String, ArrayList<String>> hashMap = new HashMap<String, ArrayList<String>>();
		int keyIndex = cursor.getColumnIndex(KEY_COLUMN);
		int valueIndex = cursor.getColumnIndex(VALUE_COLUMN);
		int epochIndex = cursor.getColumnIndex(EPOCH_COLUMN);
		while(cursor.moveToNext()) {
			ArrayList<String> valuesArr = new ArrayList<String>();
			valuesArr.add(cursor.getString(valueIndex));
			valuesArr.add(cursor.getString(epochIndex));
			hashMap.put(cursor.getString(keyIndex), valuesArr);
		}
		cursor.close();
		Log.i(TAG, "cursorToMap: Done");
		return hashMap;
	}

	Cursor mapToCursor(HashMap<String, ArrayList<String>> map) {
		MatrixCursor cursor = new MatrixCursor(new String[] {KEY_COLUMN, VALUE_COLUMN, EPOCH_COLUMN});
		for (String key: map.keySet()) {
			ArrayList<String> val = map.get(key);
//			Log.i(TAG, "mapToCursor: key: " + key + " value: " + val[VAL_IDX] + " epoch: " + val[VAL_IDX+1]);
			Log.i(TAG, "mapToCursor: value: " + val.get(VAL_IDX) + " key: "+key);
			cursor.newRow()
					.add(KEY_COLUMN, key)
					.add(VALUE_COLUMN, val.get(VAL_IDX))
					.add(EPOCH_COLUMN, val.get(VAL_IDX+1));
		}
		return cursor;
	}

	Cursor filterCursor(Cursor cursor) {
		MatrixCursor myCursor = new MatrixCursor(new String[] {KEY_COLUMN, VALUE_COLUMN});
		int keyIndex = cursor.getColumnIndex(KEY_COLUMN);
		int valueIndex = cursor.getColumnIndex(VALUE_COLUMN);
		while(cursor.moveToNext()) {
			String returnKey = cursor.getString(keyIndex);
			String returnValue = cursor.getString(valueIndex);
			myCursor.newRow()
					.add(KEY_COLUMN, returnKey)
					.add(VALUE_COLUMN, returnValue);
		}
		cursor.close();
		return myCursor;
	}

	Cursor getUpdatedCursor(HashMap<String, ArrayList<String>> myMap, Cursor[] other, boolean inSync) {
		Log.i(TAG, "getUpdatedCursor: Start");
		HashMap<String, ArrayList<String>> updateMap = new HashMap<String, ArrayList<String>>();
		ArrayList<HashMap<String, ArrayList<String>>> listMaps = new ArrayList<HashMap<String, ArrayList<String>>>();
		for (Cursor cur: other) {
			if (cur != null) {
				HashMap<String, ArrayList<String>> compareMap = cursorToMap(cur);
				Log.i(TAG, "getUpdatedCursor: This be the map!:" + compareMap);
				if (compareMap.size() == 0) {
					continue;
				}
				// added new!
				if (myMap.size() == 0) {
					myMap = compareMap;
					continue;
				}
				listMaps.add(compareMap);
				for(String key: myMap.keySet()) {
					ArrayList<String> selfVal = myMap.get(key);
					ArrayList<String> otherVal = compareMap.get(key);
					long epochSelf = Long.parseLong(selfVal.get(VAL_IDX+1));
					long epochOther = Long.parseLong(otherVal.get(VAL_IDX+1));
					if (epochOther >= epochSelf) {
						ArrayList<String> newVal = new ArrayList<String>();
						newVal.add(otherVal.get(VAL_IDX));
						newVal.add(otherVal.get(VAL_IDX+1));
						myMap.put(key, newVal);
					}
				}
			}
		}

		(new updateThread(myMap)).start();

		// we are getting the values but not updating it!
		if (!inSync) {
			// what if one of the replicas has latest values?
			// 3-way comparison! some values old and some values new
			// update self values in a separate thread
			// then how to update those keys selectively
			(new propagateThread(myMap, listMaps)).start();
		}
		Log.i(TAG, "getUpdatedCursor: MAP before return: " + myMap + " at " + dynamoOp.getWhoami());
		return mapToCursor(myMap);
	}

	Cursor getAllLocal() {
		ArrayList<Cursor> cursors = new ArrayList<Cursor>();
		HashMap<String, String> allTables = dynamoOp.getTableLookup();
		for (String key: allTables.keySet()) {
			cursors.add(filterCursor(getValueFromTable(allTables.get(key), LOCAL)));
		}

		return mergeCursors(cursors);
	}

	// TODO - Code is stable but lot of code replication, can be optimized further.

	void getMyReplicaValues(JSONObject jsonObject, HashMap<String, ArrayList<String>> myMap) {
		String prev = dynamoOp.getPrev(dynamoOp.getWhoami());
		Log.i(TAG, "getMyReplicaValues: Calling previous nodes to get their data!");
		ArrayList<HashMap<String, ArrayList<String>>> otherMaps = new ArrayList<HashMap<String, ArrayList<String>>>();
		ArrayList<String> myReplicas = new ArrayList<String>();
		jsonObject = prepareRep(jsonObject, dynamoOp.getWhoami(), !SELF_VAL);
		for (int i = 0; i < READ_QUORUM; i++) {
			Log.i(TAG, "getMyValues: Prev:" + prev);
			JSONObject res = sendAndReceive(jsonObject, dynamoOp.getSendPort(prev), !NO_RECV);
			otherMaps.add(cursorToMap(buildCursor(res)));
			myReplicas.add(prev);
			prev = dynamoOp.getPrev(prev);
		}

		for (int i = 0; i < otherMaps.size(); i++) {
			Log.i(TAG, "getMyValues: Insert by: "+ i +"th prev node");
			for (String key : otherMaps.get(i).keySet()) {
				ArrayList<String> otherVal = otherMaps.get(i).get(key);
				if (myMap.containsKey(key)) {
					ArrayList<String> selfVal = myMap.get(key);
					long epochSelf = Long.parseLong(selfVal.get(VAL_IDX + 1));
					long epochOther = Long.parseLong(otherVal.get(VAL_IDX + 1));
					if (epochOther > epochSelf) {
						insertIntoTable(dynamoOp.getTableName(myReplicas.get(i)), getContentValues(key, otherVal.get(VAL_IDX), otherVal.get(VAL_IDX+1)));
					}
				} else {
					insertIntoTable(dynamoOp.getTableName(myReplicas.get(i)), getContentValues(key, otherVal.get(VAL_IDX), otherVal.get(VAL_IDX+1)));
				}
			}
		}
	}

	void getMyValues(JSONObject jsonObject, HashMap<String, ArrayList<String>> myMap) {
		Log.i(TAG, "getMyValues: Calling next nodes to get my data");
		String next = dynamoOp.getNext(dynamoOp.getWhoami());
		ArrayList<HashMap<String, ArrayList<String>>> otherMaps = new ArrayList<HashMap<String, ArrayList<String>>>();
		jsonObject = prepareRep(jsonObject, dynamoOp.getWhoami());
		for (int i = 0; i < READ_QUORUM; i++) {
			Log.i(TAG, "getMyValues: Next:" + next);
			JSONObject res = sendAndReceive(jsonObject, dynamoOp.getSendPort(next), !NO_RECV);
			otherMaps.add(cursorToMap(buildCursor(res)));
			next = dynamoOp.getNext(next);
		}

		for (int i = 0; i < otherMaps.size(); i++) {
			Log.i(TAG, "getMyValues: Insert by: "+ i +"th next node");
			for (String key : otherMaps.get(i).keySet()) {
				ArrayList<String> otherVal = otherMaps.get(i).get(key);
				if (myMap.containsKey(key)) {
					ArrayList<String> selfVal = myMap.get(key);
					long epochSelf = Long.parseLong(selfVal.get(VAL_IDX + 1));
					long epochOther = Long.parseLong(otherVal.get(VAL_IDX + 1));
					if (epochOther > epochSelf) {
						insertIntoTable(dynamoOp.getTableName(dynamoOp.getWhoami()), getContentValues(key, otherVal.get(VAL_IDX), otherVal.get(VAL_IDX+1)));
					}
				} else {
					insertIntoTable(dynamoOp.getTableName(dynamoOp.getWhoami()), getContentValues(key, otherVal.get(VAL_IDX), otherVal.get(VAL_IDX+1)));
				}
			}
		}
	}


	void getOtherValues(JSONObject jsonObject) {
		String targetNode = dynamoOp.getWhoami();
		Cursor myCursor = getValueFromTable(dynamoOp.getTableName(targetNode), LOCAL);
		HashMap<String, ArrayList<String>> hashMap = cursorToMap(myCursor);
		getMyReplicaValues(jsonObject, hashMap);
		getMyValues(jsonObject, hashMap);
	}

	Cursor getLatestValues(JSONObject jsonObject, String selection, boolean syncOn) {
		// use update function to update values
		// always call this in coordinator
		Log.i(TAG, "getLatestValues: Here");
		String targetNode = dynamoOp.getWhoami();
		Cursor myCursor = getValueFromTable(dynamoOp.getTableName(targetNode), selection);
		HashMap<String, ArrayList<String>> hashMap = cursorToMap(myCursor);
		// compare epochs in each cursor and get key value with highest epoch
		// update the table and propagate values to successors
		Cursor[] resCursors = queryReplicate(jsonObject, targetNode, REVERSE);
		// what if i have latest values and successors have stale values ?
		// get cursor with latest values
		myCursor = getUpdatedCursor(hashMap, resCursors, syncOn);
		if (syncOn) {
			// sync is on then get from prev nodes, their values to be replicated here
			// use query replicate with reverse
			// and please put print statements everywhere
			Cursor[] revCursors = queryReplicate(jsonObject, targetNode, !REVERSE);
			// what if i have latest values and successors have stale values ?
			// get cursor with latest values
			myCursor = getUpdatedCursor(hashMap, revCursors, syncOn);
		}
		Log.i(TAG, "getLatestValues: Done!");
		return myCursor;
	}

	class syncThread extends Thread {

		@Override
		public void run() {
			performSync();
		}
	}

	class propagateThread extends Thread {
		HashMap<String, ArrayList<String>> map;
		ArrayList<HashMap<String, ArrayList<String>>> listMaps;
		propagateThread(HashMap<String, ArrayList<String>> map, ArrayList<HashMap<String, ArrayList<String>>> listMaps) {
			this.map = map;
			this.listMaps = listMaps;
		}
		// TODO code re-usage
		// TODO decouple requests!
		@Override
		public void run() {
			String next;
			for (HashMap<String, ArrayList<String>> cur: listMaps) {
				next = dynamoOp.getNext(dynamoOp.getWhoami());
				Log.i(TAG, "run: Sending to: " + next);
				if (cur == null) {
					continue;
				}
				HashMap<String, ArrayList<String>> sendMap = new HashMap<String, ArrayList<String>>();
				for (String key: map.keySet()) {
					ArrayList<String> selfVal = map.get(key);
					ArrayList<String> otherVal = cur.get(key);
					long epochSelf = Long.parseLong(selfVal.get(VAL_IDX+1));
					long epochOther = Long.parseLong(otherVal.get(VAL_IDX+1));
					if (epochOther > epochSelf) {
						ArrayList<String> newVal = new ArrayList<String>();
						newVal.add(otherVal.get(VAL_IDX));
						newVal.add(otherVal.get(VAL_IDX+1));
						sendMap.put(key, newVal);
					}
				}
				JSONObject jsonObject = buildMapJson(sendMap);
				jsonObject = buildJson(jsonObject, DO_UPDATE);
				sendAndReceive(jsonObject, dynamoOp.getSendPort(next), NO_RECV);
			}
		}
	}

	void performSync() {
		// get table values from predecessors and update self
		// get values of myself from successors and update using the same function as in getLatest
		Log.i(TAG, "performSync: CALLED !!!");
		JSONObject jsonObject = buildJson(queryJson(LOCAL), DO_QUERY);
		getOtherValues(jsonObject);
	}
}
