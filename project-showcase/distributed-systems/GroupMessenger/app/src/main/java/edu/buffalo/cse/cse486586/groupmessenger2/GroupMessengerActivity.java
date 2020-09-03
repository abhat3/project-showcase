package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.graphics.Color;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 * Co-author Ajit Bhat (abhat3) - 50321790
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    final String KEY_COL = "key";
    final String VAL_COL = "value";
    Group newGroup;
    String portPass;
    ArrayList<String> ports;
    @Override
    protected void onDestroy() {
        super.onDestroy();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        // Get the current AVD's port number and map it to the virtual router.
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        portPass = myPort;
        ports = new ArrayList<String>();
        ports.add(REMOTE_PORT0);
        ports.add(REMOTE_PORT1);
        ports.add(REMOTE_PORT2);
        ports.add(REMOTE_PORT3);
        ports.add(REMOTE_PORT4);
        newGroup = new Group();

        // Create new server socket and execute a server async task on it
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (Exception e) {
            e.printStackTrace();
            Log.e(TAG, "onCreate: Could not create Server Socket - IOException");
            return;
        }

        final EditText et = (EditText) findViewById(R.id.editText1);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        final Button send = (Button) findViewById(R.id.button4);
        send.setOnClickListener(new View.OnClickListener(){
            public void onClick(View v){

                // Extract the string from the edit box and then display it.
                // Log.i(TAG, "onClick: Clicked!");

                String msg = et.getText().toString() + "\n";
                et.setText("");
                tv.setTextColor(Color.DKGRAY);
                tv.append("\t" + msg);
                tv.append("\n");

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

            }
        });
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        Uri uri;
        AtomicInteger seqNum = new AtomicInteger(0);
        double proposal = 1;
        double lastAcceptedProposal = proposal;
        private PriorityBlockingQueue<Double> minHeap =  new PriorityBlockingQueue<Double>();
        private Map<String, String> mappingMessage = new ConcurrentHashMap<String, String>();
        private Map<String, Double> mappingProposal = new ConcurrentHashMap<String, Double>();
        private Map<Double, String> mappingAccept = new ConcurrentHashMap<Double, String>();
        private final String MSG = "message";
        private final String ACC = "accept";
        final String ALIAS = "alias";
        private ContentResolver resolve = getContentResolver();


        synchronized double updateProposal(int type, double tieBreaker) {
            if (type == 0) {
                proposal = Math.ceil(Math.max(proposal, lastAcceptedProposal)) + tieBreaker;
                proposal += 1;
                return proposal;
            } else {
                lastAcceptedProposal = Math.max(lastAcceptedProposal, tieBreaker);
                return lastAcceptedProposal;
            }
        }

        synchronized void acceptProposal(JSONObject jsonObject) {
            try {
                String alias = jsonObject.getString(MSG);
                Double acc = jsonObject.getDouble(ACC);
                if (mappingAccept.containsKey(acc)) {
                    Log.i(TAG, "acceptProposal: already processed ?? " + alias);
                    return;
                }
                Log.i(TAG, "acceptProposal: Accepted proposal: " + acc + " for " + alias);
                minHeap.remove(mappingProposal.get(alias));
                minHeap.add(acc);
                mappingAccept.put(acc, alias);
                deliver();
            } catch (JSONException e) {
                Log.i(TAG, "acceptProposal: whaatt");
            }
        }


        synchronized void deliver() {
            ContentValues content;
            try {
                if (minHeap.size() == 0) {
                    Log.i(TAG, "deliver: entered deliver, but minheap is zero");
                    return;
                }
                while(true) {
                    double min = minHeap.peek();
                    if (mappingAccept.containsKey(min)) {
                        minHeap.poll();
                        String msg = mappingMessage.get(mappingAccept.get(min));
                        content = new ContentValues();
                        Log.i(TAG, "deliver: DELIVERED : " + seqNum.toString() + " " + msg);
                        content.put(KEY_COL, seqNum.toString());
                        seqNum.incrementAndGet();
                        content.put(VAL_COL, msg);
                        resolve.insert(uri, content);
                        if (minHeap.size() == 0) {
                            Log.i(TAG, "deliver: is size zero??");
                            return;
                        }
                    }
                    else {
                        Log.i(TAG, "deliver: Couldn't deliver, not accepted:" + min);
                        break;
                    }
                }
            } catch (NullPointerException n){
                Log.i(TAG, "deliver: whaat is happ");
            }
        }

        public class serverThread implements Runnable{

            private Socket socket;
            private BufferedReader inStream;
            private BufferedWriter outStream;

            serverThread(Socket socket) {
                final int timeout = 15000; // in milliseconds
                this.socket = socket;
                try {
                    this.socket.setKeepAlive(Boolean.TRUE);
                    this.socket.setSoTimeout(timeout);
                    inStream = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
                    outStream = new BufferedWriter(new OutputStreamWriter(this.socket.getOutputStream()));
                } catch (Exception e) {
                    Log.i(TAG, "serverThread: Can't set timeout");
                }
                uri = GroupMessengerProvider.CONTENT_URI;
                Log.i(TAG, "serverThread: Server created");
            }

            String processRec(JSONObject json, double proposeThis) throws Exception {
                final String PROP = "proposal";
                try {
                    String message = json.getString(MSG);
                    String alias = json.getString(ALIAS);
                    mappingMessage.put(alias, message);
                    mappingProposal.put(alias, proposeThis);
                    minHeap.add(proposeThis);
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put(MSG, alias);
                    jsonObject.put(PROP, proposeThis);
                    return jsonObject.toString();
                } catch (JSONException e) {
                    throw new Exception("WTF could not build message");
                }
            }


            @Override
            public void run() {
                String jsonStr;
                final String TYPE = "type";
                final String PORT = "port";
                String sendThis;

                Map<String, Double> mapping = new HashMap<String, Double>();
                Map<String, String> mapping2 = new HashMap<String, String>();
                mapping.put(REMOTE_PORT0, 0.1);
                mapping.put(REMOTE_PORT1, 0.2);
                mapping.put(REMOTE_PORT2, 0.3);
                mapping.put(REMOTE_PORT3, 0.4);
                mapping.put(REMOTE_PORT4, 0.5);
                mapping2.put("none", "none");
                mapping2.put(REMOTE_PORT0, "AVD0");
                mapping2.put(REMOTE_PORT1, "AVD1");
                mapping2.put(REMOTE_PORT2, "AVD2");
                mapping2.put(REMOTE_PORT3, "AVD3");
                mapping2.put(REMOTE_PORT4, "AVD4");
                int type;
                double tieBreaker = mapping.get(portPass);
                double localCopyProposal = 0;
                double localAcceptedProposal = 0;
                ArrayList<Double> soFar = new ArrayList<Double>();
                String myport = "none";
                try {
                    while((jsonStr = inStream.readLine()) != null) {
                        try {
                            JSONObject json = new JSONObject(jsonStr);
                            type = json.getInt(TYPE);
                            switch(type) {
                                case 0:
                                    myport = Integer.toString(json.getInt(PORT));
                                    Log.i(TAG, "run: Got normal msg " + json.getString(MSG) + " from " + mapping2.get(Integer.toString(json.getInt(PORT))));
                                    localCopyProposal = updateProposal(type, tieBreaker);
                                    soFar.add(localCopyProposal);
                                    try {
                                        sendThis = processRec(json, localCopyProposal);
                                        try {
                                            outStream.write(sendThis);
                                            outStream.newLine();
                                            outStream.flush();
                                        } catch (IOException x) {
                                            Log.i(TAG, "run: Error while sending on server side");
                                        }
                                    } catch (Exception n) {
                                        Log.i(TAG, "run: Why isn't the message processed?");
                                    }
                                    break;
                                case 1:
                                    localAcceptedProposal = updateProposal(type, json.getDouble(ACC));
                                    acceptProposal(json);
                                    break;
                            }
                        } catch (JSONException j) {
                            j.printStackTrace();
                            Log.i(TAG, "run: Error in parsing Json!" + myport);
                        }
                    }
                    Log.i(TAG, "run: SERVER WHY OH WHY");
                }
                catch (StreamCorruptedException e) {
                    Log.i(TAG, "Server run: Corrupt detected" + myport);
                } catch (SocketTimeoutException e) {
                    Log.i(TAG, "Server run: Timeout detected" + myport);
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.i(TAG, "Server run: Can't read from socket?");
                } catch (Exception e) {
                    Log.i(TAG, "run: What exception did i not catch");
                }

                if (localCopyProposal != 0) {
                    for(double k: soFar) {
                        if (!mappingAccept.containsKey(k)) {
                            minHeap.remove(k);
                        }
                    }
                    if (!mappingAccept.containsKey(localCopyProposal)) {
                        minHeap.remove(localCopyProposal);
                    }
                    Log.i(TAG, "run: Removing from minHeap:" + localCopyProposal);
                }

                try {
                    inStream.close();
                    outStream.close();
                    socket.close();
                } catch (IOException e) {
                    Log.i(TAG, "run: Server could not close socket and streams!");
                }
                deliver();
                Log.i(TAG, "run: SERVER: CRASH DETECTED" + mapping2.get(myport));
            }
        }


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            ExecutorService exeggutor = Executors.newFixedThreadPool(5);

            try {
                while (true) {
                    exeggutor.execute(new serverThread(serverSocket.accept()));
                }
            }
            catch (Exception e){
                Log.e(TAG, "doInBackground: Server IOException");
            }
            return null;
        }


        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            Log.i(TAG, "onProgressUpdate: Entered!");
            String strReceived = strings[0].trim();
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.setTextColor(Color.GRAY);
            tv.append("\t" + strReceived);
            tv.append("\n");

        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            if (newGroup.length() == 0) {
                // if group is empty then create new sockets and add them to group
                // spawn client threads to monitor server messages
                for (String port : ports){
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        newGroup.addMember(socket);
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask Socket IOException");
                    }
                }
                newGroup.spawnClients();
                newGroup.multiCast(msgs[0], msgs[1]);
            } else {
                newGroup.multiCast(msgs[0], msgs[1]);
            }
            return null;
        }
    }

    public class Group {
        private Queue<Socket> groupMembers;
        private HashMap<Socket, BufferedWriter> outputStreams;
        private HashMap<Socket, BufferedReader> inputStreams;
        private Map<String, ArrayList<Double>> messageProposal;
        private Map<String, ArrayList<String>> messageTrack;
        private Map<String, Double> messageAccept;
        private Map<String, String> mapping;
        private int msgNum;

        Group() {
            groupMembers = new ConcurrentLinkedQueue<Socket>();
            messageProposal = new ConcurrentHashMap<String, ArrayList<Double>>();
            messageTrack = new HashMap<String, ArrayList<String>>();
            outputStreams = new HashMap<Socket, BufferedWriter>();
            inputStreams = new HashMap<Socket, BufferedReader>();
            messageAccept = new HashMap<String, Double>();
            mapping = new HashMap<String, String>();
            mapping.put(REMOTE_PORT0, "A");
            mapping.put(REMOTE_PORT1, "B");
            mapping.put(REMOTE_PORT2, "C");
            mapping.put(REMOTE_PORT3, "D");
            mapping.put(REMOTE_PORT4, "E");
            msgNum = 1;
        }


        void addMember(Socket socket) {
            groupMembers.add(socket);
            try {
                inputStreams.put(socket, new BufferedReader(new InputStreamReader(socket.getInputStream())));
                outputStreams.put(socket, new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
            } catch (IOException e) {
                Log.i(TAG, "addMember: Error in fetching I/O stream!");
            }
            Log.i(TAG, "addMember: Adding..."+socket.getPort());
        }

        void removeMember(Socket socket) {
            groupMembers.remove(socket);
            outputStreams.remove(socket);
            inputStreams.remove(socket);
            Log.i(TAG, "removeMember: Removing...." + "Group now has: " + length());
        }

        void removeFromList(String msg) {
            messageProposal.remove(msg);
        }
        void removeFromTrack(String msg) {
            messageTrack.remove(msg);
        }
        synchronized int length() {
            return groupMembers.size();
        }

        void incrementNum() {
            msgNum++;
        }

        synchronized void addToList(String alias, Double val) {
            messageProposal.get(alias).add(val);
        }

        synchronized void addToTrack(String alias, String port) {
            String addThis = mapping.get(port);
            if (messageTrack.containsKey(alias)) {
                messageTrack.get(alias).add(addThis);
            } else {
                Log.i(TAG, "addToTrack: FAILED to insert: " + alias + " port: "+port);
            }
        }

        Boolean getMessageAccept(String msg) {
            return messageAccept.containsKey(msg);
        }

        synchronized void setMessageAccept(String msg, Double prop) {
            messageAccept.put(msg, prop);
        }

        int getListSize(String msg) {
            if (messageProposal.containsKey(msg)) {
                return messageProposal.get(msg).size();
            }
            return 0;
        }

        Double getMax(String msg) {
            return Collections.max(messageProposal.get(msg));
        }

        String makeMsgAlias(String key) throws Exception {
            String msgAlias;
            if (mapping.containsKey(key)) {
                msgAlias = mapping.get(key);
                msgAlias += msgNum;
                return msgAlias;
            } else {
                Log.i(TAG, "makeMsgAlias: Your key in mapping is missing");
                throw new Exception("Missing key");
            }
        }

        String processMessage(String msg, String msgAlias, String port) throws NullPointerException {
            final String TYPE = "type";
            final String MSG = "message";
            final String PORT = "port";
            final String NUM = "num";
            final String ALIAS = "alias";
            final int TYPE_MSG = 0;

            JSONObject obj = new JSONObject();
            try {
                obj.put(TYPE, TYPE_MSG);
                obj.put(MSG, msg);
                obj.put(PORT, port);
                obj.put(NUM, msgNum);
                obj.put(ALIAS, msgAlias);
            } catch (JSONException e) {
                Log.i(TAG, "processMessage: error in json creation");
            }
            String str = obj.toString();
            if (str.length() == 0) {
                throw new NullPointerException("Your JSON is null");
            }
            return str;
        }

        synchronized void sendMessage(Socket socket, String msg) {
            if (outputStreams.containsKey(socket)) {
                BufferedWriter out = outputStreams.get(socket);
                try {
                    Log.i(TAG, "sendMessage: " + msg);
                    out.write(msg);
                    out.newLine();
                    out.flush();
                } catch (IOException e) {
                    Log.i(TAG, "sendMessage: Could not send!");
                }
            } else {
                Log.i(TAG, "sendMessage: why does it not contain the stream?");
            }
        }

        BufferedReader getInputStream(Socket socket) throws NullPointerException{
            if (inputStreams.containsKey(socket)) {
                return inputStreams.get(socket);
            } else {
                throw new NullPointerException("Cannot fetch input stream!");
            }
        }

        void multiCast(String msg, String myPort) {
            if (groupMembers.isEmpty()) {
                Log.i(TAG, "multiCast: There are no group members to send to!");
                return;
            }
            String toSend, mAlias;
            try {
                mAlias = makeMsgAlias(myPort);
                if (mAlias.length() == 0) {
                    Log.i(TAG, "multiCast: Message alias is empty.. exiting..");
                    return;
                }
                messageProposal.put(mAlias, new ArrayList<Double>());
                messageTrack.put(mAlias, new ArrayList<String>());
                for (Socket member : groupMembers) {
                    try {
                        toSend = processMessage(msg, mAlias, myPort);
                        sendMessage(member, toSend);
                    } catch (Exception n) {
                        Log.i(TAG, "multiCast: Error in send");
                    }
                }
                incrementNum();
            } catch (Exception e) {
                Log.i(TAG, "multiCast: Error in alias creation");
            }

        }

        void multiCast(JSONObject jsonObject) {
            if (groupMembers.isEmpty()) {
                Log.i(TAG, "multiCast: There are no group members to send to!");
                return;
            }
            String toSend = jsonObject.toString();
            for (Socket member: groupMembers) {
                sendMessage(member, toSend);
            }
        }

        synchronized void sendAccept(String alias, String port) {
            final int TYPE_ACC = 1;
            final String ACC = "accept";
            final String MSG = "message";
            final String TYPE = "type";
            try {
                if (!newGroup.getMessageAccept(alias)) {
                    Log.i(TAG, "Client: Proposal received " + alias + ": " + " Group Length: " + newGroup.getListSize(alias) + " = " + newGroup.length() + " From " + port);
                    if (newGroup.length() <= newGroup.getListSize(alias)) {
                        Double proposal = newGroup.getMax(alias);
                        JSONObject json = new JSONObject();
                        json.put(MSG, alias);
                        json.put(ACC, proposal);
                        json.put(TYPE, TYPE_ACC);
                        Log.i(TAG, "Sending proposal: " + proposal + " for " + alias);
                        newGroup.removeFromList(alias);
                        newGroup.removeFromTrack(alias);
                        newGroup.multiCast(json);
                        newGroup.setMessageAccept(alias, proposal);
                    }
                } else {
                    Log.i(TAG, "sendAccept: Already accepted.. skipping");
                }
            } catch (JSONException e) {
                Log.i(TAG, "sendAccept: why json");
            }
        }

        void checkAndSend(String port) {
            ArrayList<String> tempList;
            for(String key: newGroup.messageProposal.keySet()) {
                tempList = messageTrack.get(key);
                Boolean c1 = tempList.contains(mapping.get(port)) && tempList.size() > newGroup.length();
                Boolean c2 = !tempList.contains(mapping.get(port)) && tempList.size() == newGroup.length();
                if (c1 || c2) {
                    sendAccept(key, port);
                }
            }
        }

        void spawnClients() {
            ExecutorService exeggutor = Executors.newFixedThreadPool(5);
            for (Socket member: groupMembers) {
                exeggutor.execute(new clientThread(member));
            }
        }

        void cleanUp(Socket socket) {
            if (length() == 0) {
                Log.i(TAG, "cleanUp: Already done. ");
                return;
            }
            if (groupMembers.contains(socket)) {
                removeMember(socket);
            } else {
                Log.i(TAG, "cleanUp: Socket not present");
            }
        }
    }

    public class clientThread implements Runnable {
        private Socket socket;
        private BufferedReader in;
        final String PROP = "proposal";
        final String MSG = "message";

        clientThread(Socket socket) {
            final int timeout = 15000; // in milliseconds
            this.socket = socket;
            try {
                in = newGroup.getInputStream(this.socket);
                this.socket.setSoTimeout(timeout);
            } catch (NullPointerException e) {
                Log.i(TAG, "clientThread: Error!");
            } catch (SocketException e) {
                Log.i(TAG, "clientThread: Error while setting timeout?");
            }
            Log.i(TAG, "clientThread: hey created");
        }

        @Override
        public void run() {
            String str;

            Map<String, String> mapping2 = new HashMap<String, String>();
            mapping2.put(REMOTE_PORT0, "AVD0");
            mapping2.put(REMOTE_PORT1, "AVD1");
            mapping2.put(REMOTE_PORT2, "AVD2");
            mapping2.put(REMOTE_PORT3, "AVD3");
            mapping2.put(REMOTE_PORT4, "AVD4");
            String alias = "none";
            try {
                while ((str = in.readLine()) != null) {
                    JSONObject jsonObject = new JSONObject(str);
                    alias = jsonObject.getString(MSG);
                    Double prop = jsonObject.getDouble(PROP);
                    Log.i(TAG, "run: Client: Alias:" + alias + " Prop was: " + prop);
                    newGroup.addToList(alias, prop);
                    newGroup.addToTrack(alias, Integer.toString(socket.getPort()));
                    Log.i(TAG, "run: Client: Proposal received " + alias + ": " + prop + " Group Length: " + newGroup.getListSize(alias) + " = " + newGroup.length() + " From" + socket.getPort());
                    newGroup.sendAccept(alias, Integer.toString(socket.getPort()));
                }
            } catch (JSONException e) {
                Log.i(TAG, "Client run: Json exception?");
            }
            catch (StreamCorruptedException e) {
                Log.i(TAG, "Client run: Lost connection to client Corrupt" + socket.getPort());
            } catch (SocketTimeoutException e) {
                Log.i(TAG, "Client run: Lost connection to client Timeout" + socket.getPort());
            } catch (IOException e) {
                Log.i(TAG, "clientThread: run: Lost connection to client");
            } catch (Exception e) {
                e.printStackTrace();
                Log.i(TAG, "run: what exception did i not catch, client?");
            }
            newGroup.cleanUp(socket);
            newGroup.checkAndSend(Integer.toString(socket.getPort()));
            Log.i(TAG, "run: Client: CRASH DETECTED" + mapping2.get(Integer.toString(socket.getPort())));
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
