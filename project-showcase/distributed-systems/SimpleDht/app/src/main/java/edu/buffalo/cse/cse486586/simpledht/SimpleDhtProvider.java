package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/* Author - Ajit Bhat 50321790 */

public class SimpleDhtProvider extends ContentProvider {
    static final String LEADER = "11108";
    static final int SERVER_PORT = 10000;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.simpledht.provider";
    static final String URL = "content://" + PROVIDER_NAME;
    static final Uri CONTENT_URI = Uri.parse(URL);

    private SQLiteDatabase db;
    static final String DATABASE_NAME = "SimpleDB";
    static final String TABLE_NAME = "myStore";
    static final String KEY_COLUMN = "key";
    static final String VALUE_COLUMN = "value";

    private static final String SQL_CREATE_ENTRIES = " CREATE TABLE " + TABLE_NAME +
            " ( \"" + KEY_COLUMN +"\" TEXT NOT NULL UNIQUE ON CONFLICT REPLACE, " +
            VALUE_COLUMN + " TEXT NOT NULL);";

    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + TABLE_NAME;

    Node myNode;
    static final String TYPE = "type";
    static final String PORT = "port";
    static final String HASH_ID = "id";
    static final String KEY = "key";
    static final String PRED = "pred";
    static final String PRED_PORT = "predport";
    static final String SUCC = "succ";
    static final String SUCC_PORT = "succport";
    static final String URI = "uri";
    static final String CURSOR = "cursor";
    static final String CONTENT = "content";
    static final String LOCAL = "@";
    static final String GLOBAL = "*";
    static final String SELECTION = "selection";
    static final int DO_JOIN = 100;
    static final int DO_INSERT = 200;
    static final int DO_QUERY = 300;
    static final int DO_DELETE = 400;
    static final int GET_NOTIFY = 500;
    static final int REPLY = 600;

    private static class DatabaseHandler extends SQLiteOpenHelper {

        DatabaseHandler(Context context){
            super(context, DATABASE_NAME, null, 1);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {

            db.execSQL(SQL_CREATE_ENTRIES);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL(SQL_DELETE_ENTRIES);
            onCreate(db);
        }
    }

    private DatabaseHandler dbHandler;

    HashMap<String, HashMap<String, String>> nodeDirectory;

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

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String[] keyValueToSearch = {selection};
        selection = "\"" + KEY_COLUMN + "\"" + " = ?";
        return db.delete(TABLE_NAME,  selection, keyValueToSearch);
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
        return findPosition(uri, values);
    }



    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        dbInit();
        Log.i(TAG, "onCreate: I am alive, hue hue hue");

        // Get my port number
        Context context = getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        // Create server socket and server thread
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.i(TAG, "onCreate: Created server socket");
            myNode = new Node(myPort, portStr);
            Thread serverThread = new ServerThread(serverSocket);
            serverThread.start();
            // if not 5554 then send join request
            if (!myPort.equals(LEADER)) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, buildMessage(DO_JOIN, ""), LEADER);
            }
            else {
                nodeDirectory = new HashMap<String, HashMap<String, String>>();
                newEntry(myNode.getMyId(), myNode.getPredecessorId(), myNode.getSuccessorId(), myPort);
            }
        } catch (IOException e) {
            Log.i(TAG, "onCreate: Could not create server socket!");
        } catch (NoSuchAlgorithmException e) {
            Log.i(TAG, "onCreate: Could not create node due to error in SHA");
        }
        
        return true;
    }

    String buildMessage(int type, String key) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(PORT, myNode.getMyPort());
            jsonObject.put(HASH_ID, myNode.getMyId());
            jsonObject.put(TYPE, type);
            jsonObject.put(KEY, key);
            return jsonObject.toString();
        } catch (JSONException e) {
            Log.i(TAG, "buildMessage: Failed to build JSON");
        }
        return "FAIL";
    }

    String buildMessage(ContentValues values) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(PORT, myNode.getMyPort());
            jsonObject.put(HASH_ID, myNode.getMyId());
            jsonObject.put(TYPE, DO_INSERT);
            jsonObject = extractContentValues(jsonObject, values);
            return jsonObject.toString();
        } catch (JSONException e) {
            Log.i(TAG, "buildMessage: Failed to build JSON");
        }
        return "FAIL";
    }

    String buildMessage(Uri uri) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(PORT, myNode.getMyPort());
            jsonObject.put(HASH_ID, myNode.getMyId());
            jsonObject.put(TYPE, REPLY);
            jsonObject.put(URI, uri.toString());
            return jsonObject.toString();
        } catch (JSONException e) {
            Log.i(TAG, "buildMessage: Failed to build JSON");
        }
        return "FAIL";
    }

    String buildMessage(String selection) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(PORT, myNode.getMyPort());
            jsonObject.put(HASH_ID, myNode.getMyId());
            jsonObject.put(TYPE, DO_QUERY);

            jsonObject.put(SELECTION, selection);
            return jsonObject.toString();
        } catch (JSONException e) {
            Log.i(TAG, "buildMessage: Failed to build JSON");
        }
        return "FAIL";
    }

    String buildMessage(JSONObject json, int type) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(PORT, myNode.getMyPort());
            jsonObject.put(HASH_ID, myNode.getMyId());
            if(type == DO_QUERY) {
                jsonObject.put(TYPE, DO_QUERY);
            } else {
                jsonObject.put(TYPE, REPLY);
            }
            if (json.has(KEY_COLUMN)) {
                JSONArray keys = json.getJSONArray(KEY);
                JSONArray values = json.getJSONArray(VALUE_COLUMN);
                jsonObject.put(KEY, keys);
                jsonObject.put(VALUE_COLUMN, values);
            }
            return jsonObject.toString();
        } catch (JSONException e) {
            Log.i(TAG, "buildMessage: Failed to build JSON");
        }
        return "FAIL";
    }


    String buildNotify(String pred, String predPort, String succ, String succPort) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(PORT, myNode.getMyPort());
            jsonObject.put(HASH_ID, myNode.getMyId());
            jsonObject.put(TYPE, GET_NOTIFY);
            jsonObject.put(PRED, pred);
            jsonObject.put(PRED_PORT, predPort);
            jsonObject.put(SUCC, succ);
            jsonObject.put(SUCC_PORT, succPort);
            return jsonObject.toString();
        } catch (JSONException e) {
            Log.i(TAG, "buildMessage: Failed to build JSON");
        }
        return "FAIL";
    }

    void sendMessage(Socket socket, String message) {
        try {
            BufferedWriter outStream = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            outStream.write(message);
            outStream.newLine();
            outStream.flush();
        } catch (IOException e) {
            Log.i(TAG, "sendMessage: Could not send message!");
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        dbInit();
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(TABLE_NAME);
        if (selection.equals(LOCAL)) {
            Log.i(TAG, "query: all in local");
            return queryBuilder.query(db, null, null, null, null ,null, null);
        }
        Cursor cursor = buildCursor(findPosition(selection));
        myNode.setFlag(Boolean.FALSE);
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    class Node {
        String predecessorPort;
        String predecessorId;
        String successorPort;
        String successorId;
        String myPort;
        String myId;
        Boolean flag;
        Node(String port, String portId) throws NoSuchAlgorithmException {
            setPredecessorPort(port);
            setPredecessorId(genHash(portId));
            setSuccessorPort(port);
            setSuccessorId(genHash(portId));
            setMyPort(port);
            setMyId(genHash(portId));
            setFlag(Boolean.FALSE);
        }

        String getPredecessorPort() {
            return predecessorPort;
        }

        String getPredecessorId() {
            return predecessorId;
        }

        String getSuccessorPort() {
            return successorPort;
        }

        String getSuccessorId() {
            return successorId;
        }

        String getMyPort() {
            return myPort;
        }

        String getMyId() {
            return myId;
        }

        Boolean getFlag() {
            return flag;
        }

        void setFlag(Boolean flag) {
            this.flag = flag;
        }

        void setMyId(String myId) {
            this.myId = myId;
        }

        void setMyPort(String myPort) {
            this.myPort = myPort;
        }

        void setPredecessorPort(String predecessorPort) {
            this.predecessorPort = predecessorPort;
        }

        void setPredecessorId(String predecessorId) {
            this.predecessorId = predecessorId;
        }

        void setSuccessorPort(String successorPort) {
            this.successorPort = successorPort;
        }

        void setSuccessorId(String successorId) {
            this.successorId = successorId;
        }
    }

    class ServerThread extends Thread {
        ServerSocket serverSocket;
        ServerThread(ServerSocket serverSocket) {
            Log.i(TAG, "ServerThread: Started!");
            this.serverSocket = serverSocket;
        }

        void doJoin(JSONObject jsonObject) {
            Log.i(TAG, "doJoin: Entered");
            try {
                String port = jsonObject.getString(PORT);
                String remoteID = jsonObject.getString(HASH_ID);
                String id = myNode.getMyId();
                String myPort = myNode.getMyPort();
                if (myNode.getMyId().equals(myNode.getSuccessorId())) {
                    setPSPort(remoteID, port, remoteID, port);
                    String message = buildNotify(id, myPort, id, myPort);
                    createAndSend(message, port);
                    newEntry(id, myNode.getPredecessorId(), myNode.getSuccessorId());
                    newEntry(remoteID, id, id, port);
                } else {
                    findPosition(id, remoteID, port);
                }
                printMap();
                Log.i(TAG, "doJoin: Join complete: " + port);
            } catch (JSONException e) {
                Log.i(TAG, "doJoin: Exception in parsing JSON");
            }
        }

        void doInsert(Socket socket, JSONObject jsonObject) {
            dbInit();

            try {
                Log.i(TAG, "doInsert: Called by: " + jsonObject.get(PORT));
                String key = jsonObject.getString(KEY);
                String value = jsonObject.getString(VALUE_COLUMN);
                ContentValues values = getContentValues(key, value);
                Uri uri = findPosition(CONTENT_URI, values);
                String message = buildMessage(uri);
                sendMessage(socket, message);
                Log.i(TAG, "doInsert: Finished");
            } catch (JSONException e) {
                Log.i(TAG, "doInsert: Error in parsing JSON");
            }
        }

        void doQuery(Socket socket, JSONObject jsonObject) {
            dbInit();
            try {
                String message;
                Log.i(TAG, "doQuery: Called by: " + jsonObject.get(PORT));
                String selection = jsonObject.getString(SELECTION);
                Log.i(TAG, "doQuery: Key is: " + selection);
                JSONObject jsonObject1 = findPosition(selection);
                if (selection.equals(GLOBAL)) {
                    message = buildMessage(jsonObject1, DO_QUERY);
                    myNode.setFlag(Boolean.FALSE);
                } else {
                    message = buildMessage(jsonObject1, REPLY);
                }
                sendMessage(socket, message);
                Log.i(TAG, "doQuery: Finished");
            } catch (JSONException e) {
                Log.i(TAG, "doQuery: Error in parsing JSON");
            }
        }

        void doDelete() {

        }

        void getNotify(JSONObject jsonObject) {
            Log.i(TAG, "getNotify: Entered");
            try {
                String predecessor = jsonObject.getString(PRED);
                String predPort = jsonObject.getString(PRED_PORT);
                String successor = jsonObject.getString(SUCC);
                String succPort = jsonObject.getString(SUCC_PORT);
                setPSPort(predecessor, predPort, successor, succPort);
            } catch (JSONException e) {
                Log.i(TAG, "getNotify: Error in parsing JSON");
            }
            // printMe();
            Log.i(TAG, "getNotify: Finished!");
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
                            case DO_JOIN:
                                doJoin(jsonObject);
                                break;
                            case DO_INSERT:
                                doInsert(socket, jsonObject);
                                break;
                            case DO_QUERY:
                                doQuery(socket, jsonObject);
                                break;
                            case DO_DELETE:
                                doDelete();
                                break;
                            case GET_NOTIFY:
                                getNotify(jsonObject);
                                break;
                        }
                    } catch (JSONException j) {
                        Log.i(TAG, "run: Server: Could not convert to JSON");
                    }
                    // is this necessary ?
                    socket.close();
                } catch (IOException e) {
                    Log.i(TAG, "run: Server: Error at Server side");
                }
            }
        }
    }

    public void newEntry(String key, String pred, String succ) {
        nodeDirectory.get(key).put(PRED, pred);
        nodeDirectory.get(key).put(SUCC, succ);
    }

    public void newEntry(String key, String pred, String succ, String port) {
        HashMap<String, String> hashMap = new HashMap<String, String>();
        hashMap.put(PRED, pred);
        hashMap.put(SUCC, succ);
        hashMap.put(PORT, port);
        nodeDirectory.put(key, hashMap);
    }

    public void printMap() {
        for(String key: nodeDirectory.keySet()) {
            System.out.println("Key: " + key);
            System.out.println("Value: " + Collections.singletonList(nodeDirectory.get(key)));
        }
    }

    public void printMe() {
        // TODO Assertion!
        Log.i(TAG, "printMe: I am: " + myNode.getMyId() + " with port: " + myNode.getMyPort());
        Log.i(TAG, "printMe: My pred: " + myNode.getPredecessorId() + " with port: " + myNode.getPredecessorPort());
        Log.i(TAG, "printMe: My succ: " + myNode.getSuccessorId() + " with port: " + myNode.getSuccessorPort());
    }

    public JSONObject findPosition(String selection) {
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(TABLE_NAME);
        if (selection.equals(GLOBAL)) {
            Log.i(TAG, "findPosition: All in global");
            if (myNode.getPredecessorPort().equals(myNode.getMyId())) {
                return buildJSON(queryBuilder.query(db, null, null, null, null ,null, null));
            }
            if (!myNode.getFlag()) {
                // if not already visited
                myNode.setFlag(Boolean.TRUE);
                return merge(buildJSON(queryBuilder.query(db, null, null, null, null ,null, null)), clockwise(selection));
            } else {
                return new JSONObject();
            }
        } else {
            try {
                String keyInQuestion = genHash(selection);
                String originalKey = selection;
                String myId = myNode.getMyId();
                String prev = myNode.getPredecessorId();
                String[] keyValueToSearch = {selection};
                selection = "\"" + KEY_COLUMN + "\"" + " = ?";
                if ((myId.equals(prev)) || ((keyInQuestion.compareTo(prev) > 0) && (keyInQuestion.compareTo(myId) < 0))) {
                    Log.i(TAG, "findPosition: Query done on local: " + selection + " " + keyValueToSearch);
                    return buildJSON(queryBuilder.query(db, null, selection, keyValueToSearch, null, null, null));
                }
                if (myId.compareTo(prev) < 0) {
                    if ((keyInQuestion.compareTo(myId) < 0) || (keyInQuestion.compareTo(prev) > 0)) {
                        Log.i(TAG, "findPosition: Query done on local 2: " + selection + " " + keyValueToSearch);
                        return buildJSON(queryBuilder.query(db, null, selection, keyValueToSearch, null, null, null));
                    }
                }
                if (keyInQuestion.compareTo(myId) > 0) {
                    Log.i(TAG, "findPosition: Calling next");
                    JSONObject jsonObject = clockwise(originalKey);
                    Log.i(TAG, "findPosition: Query Done");
                    return jsonObject;
                } else {
                    Log.i(TAG, "findPosition: Calling prev");
                    JSONObject jsonObject = counterClockwise(originalKey);
                    Log.i(TAG, "findPosition: Query Done");
                    return jsonObject;
                }
            } catch (NoSuchAlgorithmException e) {
                Log.i(TAG, "findPosition: Error in genHash!");
            }
            return null;
        }
    }

    public JSONObject clockwise(String selection) {
        String message;
        message = buildMessage(selection);
        String succPort = myNode.getSuccessorPort();
        return getQueryResults(message, succPort);
    }

    public JSONObject counterClockwise(String selection) {
        String message = buildMessage(selection);
        String predPort = myNode.getPredecessorPort();
        return getQueryResults(message, predPort);
    }

    public Uri findPosition(Uri uri, ContentValues values) {
        try {
            String keyInQuestion = genHash(values.getAsString(KEY_COLUMN));
            String myId = myNode.getMyId();
            String prev = myNode.getPredecessorId();
            if ((myId.equals(prev)) || ((keyInQuestion.compareTo(prev) > 0) && (keyInQuestion.compareTo(myId) < 0))) {
                long rowID = db.insert(TABLE_NAME, null, values);
                //Log.i(TAG, "findPosition: Inserted: " + keyInQuestion + " at: " + myId);
                if(rowID < 0) {
                    throw new SQLException("Could not add record to database!");
                }
                Log.v("insert", values.toString());
                return uri;
            }
            if (myId.compareTo(prev) < 0) {
                if ((keyInQuestion.compareTo(myId) < 0) || (keyInQuestion.compareTo(prev) > 0)) {
                    long rowID = db.insert(TABLE_NAME, null, values);
                    //Log.i(TAG, "findPosition: Inserted: " + keyInQuestion + " at: " + myId);
                    if (rowID < 0) {
                        throw new SQLException("Could not add record to database!");
                    }
                    Log.v("insert", values.toString());
                    return uri;
                }
            }
            if (keyInQuestion.compareTo(myId) > 0) {
                uri = clockwise(keyInQuestion, values);
                //Log.i(TAG, "findPosition: Done");
                return uri;
            } else {
                uri = counterClockwise(keyInQuestion, values);
                //Log.i(TAG, "findPosition: Done");
                return uri;
            }
        } catch (NoSuchAlgorithmException e) {
            Log.i(TAG, "findPosition: Error in genHash!");
        }
        return null;
    }

    public Uri clockwise(String key, ContentValues values) {
        String message = buildMessage(values);
        String succPort = myNode.getSuccessorPort();
        Uri uri = sendAndReceive(message, succPort);
        //Log.i(TAG, "clockwise: Finished");
        return uri;
    }

    public Uri counterClockwise(String key, ContentValues values) {
        String message = buildMessage(values);
        String predPort = myNode.getPredecessorPort();
        Uri uri = sendAndReceive(message, predPort);
        //Log.i(TAG, "counterClockwise: Finished");
        return uri;
    }

    public void findPosition(String currentNode, String newNode, String port) {
        if (newNode.compareTo(currentNode) > 0) {
            clockwise(currentNode, newNode, port);
        } else if (newNode.compareTo(currentNode) < 0) {
            counterClockwise(currentNode, newNode, port);
        }
    }

    public void clockwise(String current, String candidate, String port) {
        String prev = current;
        current = getSuccessor(current);
        while ((candidate.compareTo(current) > 0) && (current.compareTo(prev) > 0)) {
            prev = current;
            current = getSuccessor(current);
        }
        changeAncestor(prev, candidate, current, port);
    }

    public void counterClockwise(String current, String candidate, String port) {
        String prev = current;
        current = getPredecessor(current);
        while ((candidate.compareTo(current) < 0) && (current.compareTo(prev) < 0)) {
            prev = current;
            current = getPredecessor(current);
        }
        changeAncestor(current, candidate, prev, port);
    }

    public void changeAncestor(String left, String middle, String right, String middlePort) {
        String myId = myNode.getMyId();
        nodeDirectory.get(left).put(SUCC, middle);
        newEntry(middle, left, right, middlePort);
        nodeDirectory.get(right).put(PRED, middle);
        if (left.equals(myId)) {
            myNode.setSuccessorId(middle);
            myNode.setSuccessorPort(middlePort);
        } else {
            String pred = getPredecessor(left);
            String predPort = getPort(pred);
            String message = buildNotify(pred, predPort, middle, middlePort);
            String leftPort = getPort(left);
            createAndSend(message, leftPort);
        }
        if (right.equals(myId)) {
            myNode.setPredecessorId(middle);
            myNode.setPredecessorPort(middlePort);
        } else {
            String succ = getSuccessor(right);
            String succPort = getPort(succ);
            String message = buildNotify(middle, middlePort, succ, succPort);
            String rightPort = getPort(right);
            createAndSend(message, rightPort);
        }

        String predPort = getPort(left);
        String succPort = getPort(right);
        String message = buildNotify(left, predPort, right, succPort);
        createAndSend(message, middlePort);
    }

    public void createAndSend(String message, String port) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            sendMessage(socket, message);
            socket.close();
        } catch (UnknownHostException e) {
            Log.i(TAG, "createAndSend: Unknown Address");
        } catch (IOException e) {
            Log.i(TAG, "createAndSend: Error in opening connection: " + port);
        }
    }

    public Uri sendAndReceive(String message, String port) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            sendMessage(socket, message);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String res = in.readLine();
            JSONObject jsonObject = new JSONObject(res);
            //Log.i(TAG, "sendAndReceive: Got back results");
            socket.close();
            return Uri.parse(jsonObject.getString(URI));
        } catch (UnknownHostException e) {
            Log.i(TAG, "sendAndReceive: Unknown Address");
        } catch (IOException e) {
            Log.i(TAG, "sendAndReceive: Error in opening connection: " + port);
        } catch (JSONException e) {
            Log.i(TAG, "sendAndReceive: Error in parsing");
        }
        return null;
    }

    public JSONObject getQueryResults(String message, String port) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            sendMessage(socket, message);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String res = in.readLine();
            JSONObject jsonObject = new JSONObject(res);
            socket.close();
            Log.i(TAG, "getQueryResults: Done");
            return jsonObject;
        } catch (UnknownHostException e) {
            Log.i(TAG, "sendAndReceive: Unknown Address");
        } catch (IOException e) {
            Log.i(TAG, "sendAndReceive: Error in opening connection: " + port);
        } catch (JSONException e) {
            Log.i(TAG, "sendAndReceive: Error in parsing");
        }
        return null;
    }

    public String getSuccessor(String key) {
        return nodeDirectory.get(key).get(SUCC);
    }

    public String getPredecessor(String key) {
        return nodeDirectory.get(key).get(PRED);
    }

    public String getPort(String key) {
        return nodeDirectory.get(key).get(PORT);
    }

    public void setPSPort(String pred, String predPort, String succ, String succPort) {
        myNode.setPredecessorPort(predPort);
        myNode.setPredecessorId(pred);
        myNode.setSuccessorPort(succPort);
        myNode.setSuccessorId(succ);
    }

    class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String message = msgs[0];
            String port = msgs[1];
            createAndSend(message, port);
            return null;
        }
    }

    JSONObject extractContentValues(JSONObject jsonObject, ContentValues values) {
        String key = values.getAsString(KEY_COLUMN);
        String value = values.getAsString(VALUE_COLUMN);
        try {
            jsonObject.put(KEY, key);
            jsonObject.put(VALUE_COLUMN, value);
            return jsonObject;
        } catch (JSONException e) {
            Log.i(TAG, "extractContentValues: JSON exception");
        }
        return null;
    }

    ContentValues getContentValues(String key, String value) {
        ContentValues contentValues = new ContentValues();
        contentValues.put(KEY_COLUMN, key);
        contentValues.put(VALUE_COLUMN, value);
        return contentValues;
    }

    JSONObject buildJSON(Cursor cursor) {
        try {
            Log.i(TAG, "buildJSON: Start");
            JSONObject jsonObject = new JSONObject();
            JSONArray keys;
            JSONArray values;
            if (jsonObject.has(KEY) && jsonObject.has(VALUE_COLUMN)) {
                keys = jsonObject.getJSONArray(KEY);
                values = jsonObject.getJSONArray(VALUE_COLUMN);
            } else {
                keys = new JSONArray();
                values = new JSONArray();
            }

            int keyIndex = cursor.getColumnIndex(KEY_COLUMN);
            int valueIndex = cursor.getColumnIndex(VALUE_COLUMN);
            while(cursor.moveToNext()) {
                keys.put(cursor.getString(keyIndex));
                values.put(cursor.getString(valueIndex));
            }
            cursor.close();
            jsonObject.put(KEY, keys);
            jsonObject.put(VALUE_COLUMN, values);
            Log.i(TAG, "buildJSON: Fin.");
            return jsonObject;
        } catch (JSONException e) {
            Log.i(TAG, "buildJSON: Error in building JSON");
        }
        return null;
    }

    JSONObject merge(JSONObject target, JSONObject source) {
        if (source.has(KEY_COLUMN)) {
            try {
                int length = source.getJSONArray(KEY_COLUMN).length();
                JSONArray sourceKeys = source.getJSONArray(KEY_COLUMN);
                JSONArray sourceValues = source.getJSONArray(VALUE_COLUMN);
                JSONArray targetKeys = target.getJSONArray(KEY_COLUMN);
                JSONArray targetValues = target.getJSONArray(VALUE_COLUMN);
                for (int i = 0; i < length; i++) {
                    targetKeys.put(sourceKeys.get(i));
                    targetValues.put(sourceValues.get(i));
                }
                target.put(KEY_COLUMN, targetKeys);
                target.put(VALUE_COLUMN, targetValues);
                return target;
            } catch (JSONException e) {
                Log.i(TAG, "merge: Error key not found! In: " + myNode.getMyPort());
            }
        }
        return target;
    }

    Cursor buildCursor(JSONObject jsonObject) {

        try {
            Log.i(TAG, "buildCursor: Start");
            JSONArray keys = jsonObject.getJSONArray(KEY);
            JSONArray values = jsonObject.getJSONArray(VALUE_COLUMN);
            MatrixCursor cursor = new MatrixCursor(new String[] {KEY_COLUMN, VALUE_COLUMN});
            for (int i = 0; i < keys.length(); i++) {
                cursor.newRow().add(KEY_COLUMN, keys.get(i)).add(VALUE_COLUMN, values.get(i));
            }
            Log.i(TAG, "buildCursor: Fin.");
            return cursor;
        } catch (JSONException e) {
            Log.i(TAG, "buildCursor: Error in parsing JSON");
        }
        return null;
    }


    // Producer Consumer model - results are added to queue
}

