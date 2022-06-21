package com.xxdb;

import java.io.*;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import com.xxdb.data.*;
import com.xxdb.data.Void;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.BigEndianDataOutputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.LittleEndianDataInputStream;
import com.xxdb.io.LittleEndianDataOutputStream;
import com.xxdb.io.ProgressListener;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Sets up a connection to DolphinDB server through TCP/IP protocol
 * Executes DolphinDB scripts
 * <p>
 * Example:
 * <p>
 * import com.xxdb;
 * DBConnection conn = new DBConnection();
 * boolean success = conn.connect("localhost", 8080);
 * conn.run("sum(1..100)");
 */

public class DBConnection {
    private static final int MAX_FORM_VALUE = Entity.DATA_FORM.values().length - 1;
    private static final int MAX_TYPE_VALUE = Entity.DATA_TYPE.DT_POINT_ARRAY.getValue();
    private static final int DEFAULT_PRIORITY = 4;
    private static final int DEFAULT_PARALLELISM = 2;
    private static final int localAPIVersion = 210;

    private ReentrantLock mutex_;
    private DBConnectionImpl conn_;
    private String uid_;
    private String pwd_;
    private String initialScript_ = null;
    private boolean enableHighAvailability_;
    private boolean enableSSL_;
    private boolean asynTask_;
    private List<Node> nodes_ = new ArrayList<>();
    private int lastConnNodeIndex_ = 0;
    private boolean compress_ = false;
    private int connTimeout_ = 0;
    private String[] highAvailabilitySites_ = null;
    private boolean python_ = false;
    private boolean closed_ = false;


    private enum ServerExceptionState {
        NEW_LEADER, WAIT, CONN_FAIL, OTHER_EXCEPTION, DATA_NODE_NOT_AVAILABLE
    }

    private enum ExceptionType{
        ET_IGNORE(0),
        ET_UNKNOW(1),
        ET_NEWLEADER(2),
        ET_NODENOTAVAIL(3);

        public int value;
        ExceptionType(int value){
            this.value = value;
        }
    }

    private static class Node {
        private String hostName;
        private int port;
        private double load = -1.0;

        public Node(){
            this.load = -1.0;
        }

        public Node(String hostName, int port, double load){
            this.hostName = hostName;
            this.port = port;
            this.load = load;
        }

        public Node(String hostName, int port){
            this.hostName = hostName;
            this.port = port;
            this.load = -1.0;
        }

        public Node(String ipPort, double loadValue){
            String[] v = ipPort.split(":");
            if (v.length < 2){
                throw new RuntimeException("The ipPort '" + ipPort + "' is invalid.");
            }
            this.hostName = v[0];
            this.port = Integer.parseInt(v[1]);
        }

        public Node(String ipPort){
            String[] v = ipPort.split(":");
            if (v.length < 2){
                throw new RuntimeException("The ipPort '" + ipPort + "' is invalid.");
            }
            this.hostName = v[0];
            this.port = Integer.parseInt(v[1]);
            this.load = -1.0;
        }

        public boolean isEqual(Node node){
            return hostName.equals(node.hostName) == true && port == node.port;
        }
    }

    public class DBConnectionImpl{
        private Socket socket_;
        private String sessionID_;
        private String hostName_;
        private int port_;
        private String userId_;
        private String pwd_;
        private boolean encrypted_;
        private boolean isConnected_;
        private boolean sslEnable_ = false;
        private boolean asynTask_ = false;
        private boolean compress_ = false;
        private int connTimeout_ = 0;
        private ExtendedDataOutput out_;
        private ExtendedDataInput in_;
        private boolean remoteLittleEndian_;
        private ReentrantLock lock_;
        private boolean usePython_ = false;

        public DBConnectionImpl(boolean sslEnable, boolean asynTask, boolean compress){
            this(sslEnable, asynTask, compress, false);
        }

        public DBConnectionImpl(boolean sslEnable, boolean asynTask, boolean compress, boolean usePython){
            sessionID_ = "";
            this.sslEnable_ = sslEnable;
            this.asynTask_ = asynTask;
            this.compress_ = compress;
            this.usePython_ = usePython;
            this.lock_ = new ReentrantLock();
        }

        public boolean connect(String hostName, int port, String userId, String password, boolean sslEnable, boolean asynTask, boolean compress) throws IOException{
            this.hostName_ = hostName;
            this.port_ = port;
            this.userId_ = userId;
            this.pwd_ = password;
            this.sslEnable_ = sslEnable;
            this.asynTask_ = asynTask;
            this.compress_ = compress;
            return connect();
        }

        private boolean connect()throws IOException{
            if (socket_ != null){
                socket_ = null;
                socket_.close();
            }
            this.isConnected_ = false;

            try {
                if(sslEnable_)
                    socket_ = getSSLSocketFactory().createSocket(hostName_,port_);
                else
                    socket_ = new Socket(hostName_, port_);
            } catch (ConnectException ex) {
                throw ex;
            }
            if (this.connTimeout_ > 0) {
                socket_.setSoTimeout(this.connTimeout_);
            }
            socket_.setKeepAlive(true);
            socket_.setTcpNoDelay(true);
            out_ = new LittleEndianDataOutputStream(new BufferedOutputStream(socket_.getOutputStream()));
            @SuppressWarnings("resource")
            ExtendedDataInput input = new LittleEndianDataInputStream(new BufferedInputStream(socket_.getInputStream()));
            String body = "connect\n";
            out_.writeBytes("API 0 ");
            out_.writeBytes(String.valueOf(body.length()));
            int flag = generateRequestFlag(false);
            out_.writeBytes(" / " + String.valueOf(flag) + "_1_" + String.valueOf(4) + "_" + String.valueOf(2));
            out_.writeByte('\n');
            out_.writeBytes(body);
            out_.flush();

            String line = input.readLine();
            int endPos = line.indexOf(' ');
            if (endPos <= 0) {
                close();
                return false;
            }
            sessionID_ = line.substring(0, endPos);

            int startPos = endPos + 1;
            endPos = line.indexOf(' ', startPos);
            if (endPos != line.length() - 2) {
                close();
                return false;
            }

            isConnected_ = true;

            if (line.charAt(endPos + 1) == '0') {
                remoteLittleEndian_ = false;
                out_ = new BigEndianDataOutputStream(new BufferedOutputStream(socket_.getOutputStream()));
            } else
                remoteLittleEndian_ = true;

            in_ = remoteLittleEndian_ ? new LittleEndianDataInputStream(new BufferedInputStream(socket_.getInputStream())) :
                    new BigEndianDataInputStream(new BufferedInputStream(socket_.getInputStream()));

            if (!userId_.isEmpty() && !pwd_.isEmpty()) {
                if (asynTask_) {
                    login(userId_, pwd_, false);
                } else {
                    login();
                }
            }

            compareRequiredAPIVersion();
            return true;
        }

        public void login(String userId, String password, boolean enableEncryption) throws IOException {
            lock_.lock();
            try {
                this.userId_ = userId;
                this.pwd_ = password;
                this.encrypted_ = enableEncryption;
                login();
            } finally {
                lock_.unlock();
            }
        }

        private void login() throws IOException {
            List<Entity> args = new ArrayList<>();
            if (encrypted_) {
                BasicString keyCode = (BasicString) run("getDynamicPublicKey", new ArrayList<Entity>());
                PublicKey key = RSAUtils.getPublicKey(keyCode.getString());
                byte[] usr = RSAUtils.encryptByPublicKey(userId_.getBytes(), key);
                byte[] pass = RSAUtils.encryptByPublicKey(pwd_.getBytes(), key);


                args.add(new BasicString(Base64.getMimeEncoder().encodeToString(usr)));
                args.add(new BasicString(Base64.getMimeEncoder().encodeToString(pass)));
                args.add(new BasicBoolean(true));
            } else {
                args.add(new BasicString(userId_));
                args.add(new BasicString(pwd_));
            }
            run("login", args);
        }

        public Entity run(String script) throws IOException {
            return run(script, DEFAULT_PRIORITY, DEFAULT_PARALLELISM);
        }

        public Entity run(String script, String scriptType) throws IOException {
            List<Entity> args = new ArrayList<>();
            return run(script, scriptType, (ProgressListener)null, args, DEFAULT_PRIORITY, DEFAULT_PARALLELISM, 0, false);
        }

        public Entity run(String script,  int priority, int parallelism) throws IOException {
            return run( script, (ProgressListener)null, priority, parallelism, 0,false);
        }

        public Entity run(String script, ProgressListener listener,int priority, int parallelism, int fetchSize, boolean clearMemory) throws IOException{
            List<Entity> args = new ArrayList<>();
            return run(script, "script", (ProgressListener)null, args, priority, parallelism, fetchSize, clearMemory);
        }

        public Entity run(String function, List<Entity> arguments) throws IOException {
            return run(function, arguments, DEFAULT_PRIORITY, DEFAULT_PARALLELISM);
        }

        public Entity run(String function, String scriptType, List<Entity> arguments)throws IOException{
            return run(function, scriptType, (ProgressListener)null, arguments, DEFAULT_PRIORITY, DEFAULT_PARALLELISM, 0, false);
        }

        public Entity run(String function, List<Entity> args, int priority, int parallelism) throws IOException {
            return run(function, (ProgressListener)null, args, priority, parallelism, 0, false);
        }

        public Entity run(String function, ProgressListener listener,List<Entity> args, int priority, int parallelism, int fetchSize, boolean clearMemory) throws IOException{
            return run(function, "function", (ProgressListener)null, args, priority, parallelism, fetchSize, clearMemory);
        }

        private Entity run(String script, String scriptType, ProgressListener listener, List<Entity> args, int priority, int parallelism, int fetchSize, boolean clearMemory) throws IOException{
            if (!isConnected_)
                throw new IOException("Couldn't send script/function to the remote host because the connection has been closed");

            if (fetchSize > 0 && fetchSize < 8192)
                throw new IOException("fetchSize must be greater than 8192");

            if (socket_ == null || !socket_.isConnected() || socket_.isClosed()) {
                if (sessionID_.isEmpty())
                    throw new IOException("Database connection is not established yet.");
                else {
                    socket_ = new Socket(hostName_, port_);
                    socket_.setKeepAlive(true);
                    socket_.setTcpNoDelay(true);
                    out_ = new LittleEndianDataOutputStream(new BufferedOutputStream(socket_.getOutputStream()));
                    in_ = remoteLittleEndian_ ? new LittleEndianDataInputStream(new BufferedInputStream(socket_.getInputStream())) :
                            new BigEndianDataInputStream(new BufferedInputStream(socket_.getInputStream()));
                }
            }

            StringBuilder body = new StringBuilder();
            int argCount = args.size();
            if (scriptType == "script")
                body.append("script\n" + script) ;
            else {
                body.append(scriptType + "\n" + script);
                body.append("\n" + String.valueOf(argCount));
                body.append("\n");
                body.append(remoteLittleEndian_ ? "1" : "0");
            }

            try {
                out_.writeBytes((listener != null ? "API2 " : "API ") + sessionID_ + " ");
                out_.writeBytes(String.valueOf(body.length()));
                short flag = 0;
                if (asynTask_)
                    flag += 4;
                if (clearMemory)
                    flag += 16;
                if (compress_)
                    flag += 64;
                out_.writeBytes(" / " + String.valueOf(flag) + "_1_" + String.valueOf(priority) + "_" + String.valueOf(parallelism));
                if (fetchSize > 0)
                    out_.writeBytes("__" + String.valueOf(fetchSize));
                out_.writeByte('\n');
                out_.writeBytes(body.toString());

                if (argCount > 0){
                    for (int i = 0; i < args.size(); ++i) {
                        if (compress_ && args.get(i).isTable()) {
                            args.get(i).writeCompressed(out_); //TODO: which compress method to use
                        } else
                            args.get(i).write(out_);
                    }

                    out_.flush();
                }else {
                    out_.flush();
                }
            }catch (IOException ex){
                isConnected_ = false;
                socket_ = null;
                throw new IOException("Couldn't send script/function to the remote host because the connection has been closed");
            }

            if (asynTask_)
                return null;

            ExtendedDataInput in = remoteLittleEndian_ ? new LittleEndianDataInputStream(new BufferedInputStream(socket_.getInputStream())) :
                    new BigEndianDataInputStream(new BufferedInputStream(socket_.getInputStream()));
            String header = null;
            try {
                header = in.readLine();
            }catch (IOException ex){
                isConnected_ = false;
                socket_ = null;
                throw new IOException("Failed to read response header from the socket with IO error " + ex.getMessage());
            }

            String[] headers = header.split(" ");
            if (headers.length != 3){
                isConnected_ = false;
                socket_ = null;
                throw new IOException("Received invalid header");
            }

            sessionID_ = headers[0];
            int numObject = Integer.parseInt(headers[1]);

            try {
                header = in.readLine();
            }catch (IOException ex){
                isConnected_ = false;
                socket_ = null;
                throw new IOException("Failed to read response header from the socket with IO error " + ex.getMessage());
            }

            if (!header.equals("OK")){
                throw new IOException(hostName_+":"+port_+" Server response: '" + header + "' script: '" + script + "'");
            }

            if (numObject == 0){
                return new Void();
            }

            short flag;
            try {
                flag = in.readShort();
                int form = flag >> 8;
                int type = flag & 0xff;
                boolean extended = type >= 128;
                if(type >= 128)
                    type -= 128;

                if (form < 0 || form > MAX_FORM_VALUE)
                    throw new IOException("Invalid form value: " + form);
                if (type < 0 || type > MAX_TYPE_VALUE)
                    throw new IOException("Invalid type value: " + type);

                Entity.DATA_FORM df = Entity.DATA_FORM.values()[form];
                Entity.DATA_TYPE dt = Entity.DATA_TYPE.valueOf(type);
                if(fetchSize>0 && df == Entity.DATA_FORM.DF_VECTOR && dt == Entity.DATA_TYPE.DT_ANY){
                    return new EntityBlockReader(in);
                }
                EntityFactory factory = BasicEntityFactory.instance();
                return factory.createEntity(df, dt, in, extended);
            }catch (IOException ex){
                isConnected_ = false;
                socket_ = null;
                throw new IOException("Failed to read object flag from the socket with IO error type " + ex.getMessage());
            }
        }

        public void upload(String name, Entity obj) throws IOException{
            if (!Utils.isVariableCandidate(name))
                throw new RuntimeException(name + " is not a qualified variable name.");
            List<Entity> args = new ArrayList<>();
            args.add(obj);
            run(name, "variable", args);
        }

        public void upload(List<String> names, List<Entity> objs) throws IOException{
            if (names.size() != objs.size())
                throw new RuntimeException("the size of variable names doesn't match the size of objects.");
            if (names.isEmpty())
                return;

            StringBuilder varNames = new StringBuilder();
            for (int i = 0; i < names.size(); ++i){
                if (!Utils.isVariableCandidate(names.get(i)))
                    throw new RuntimeException(names.get(i) + " is not a qualified variable name.");
                if (i > 0)
                    varNames.append(",");
                varNames.append(names.get(i));
            }

            run(varNames.toString(), "variable", objs);
        }

        public void close(){
            lock_.lock();
            try {
                if (socket_ != null){
                    socket_.close();
                    socket_ = null;
                    sessionID_ = "";
                }
            }catch (IOException ex){
                ex.printStackTrace();
            }finally {
                lock_.unlock();
            }
            isConnected_ = false;
        }

        public boolean isConnected(){
            return isConnected_;
        }

        public void getNode(Node node){
            node.hostName = hostName_;
            node.port = port_;
        }

        public boolean getRemoteLittleEndian(){
            return this.remoteLittleEndian_;
        }
    }

    public DBConnection() {
    	this(false, false, false);
    }

    public DBConnection(boolean asynchronousTask) {
    	this(asynchronousTask, false, false);
    }
    
    public DBConnection(boolean asynchronousTask, boolean useSSL) {
       this(asynchronousTask, useSSL, false);
    }
    
    public DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress) {
         this(asynchronousTask, useSSL, compress, false);
    }

    public DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean python){
        this.asynTask_ = asynchronousTask;
        this.enableSSL_ = useSSL;
        this.compress_ = compress;
        this.python_ = python;
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, python);
        this.mutex_ = new ReentrantLock();
    }
    
    public boolean isBusy() {
        if (!mutex_.tryLock())
            return true;
        else {
            mutex_.unlock();
            return false;
        }
    }

    private int getVersionNumber(String ver) {
        try {
            String[] s = ver.split(" ");
            if (s.length >= 2) {
                String vernum = s[0].replace(".", "");
                return Integer.parseInt(vernum);
            }
        }
        catch (Exception ex) {}
        return 0;
    }
    
    private int generateRequestFlag(boolean clearSessionMemory){
    	int flag = 0;
    	if(asynTask_)
    		flag += 4;
    	if(clearSessionMemory)
    		flag += 16;
    	if(compress_)
    		flag += 64;
        if (this.python_)
            flag += 2048;
    	return flag;
    }

    public boolean connect(String hostName, int port) throws IOException {
        return connect(hostName, port, "", "", null, false, null);
    }

    public boolean connect(String hostName, int port, int timeout) throws IOException {
        this.connTimeout_ = timeout;
        return connect(hostName, port, "", "", null, false, null);
    }

    public boolean connect(String hostName, int port, String initialScript) throws IOException {
        return connect(hostName, port, "", "", initialScript, false, null);
    }

    public boolean connect(String hostName, int port, String initialScript, boolean enableHighAvailability) throws IOException {
        return connect(hostName, port, "", "", initialScript, enableHighAvailability, null);
    }

    public boolean connect(String hostName, int port, boolean enableHighAvailability) throws IOException {
        return connect(hostName, port, "", "", null, enableHighAvailability, null);
    }

    public boolean connect(String hostName, int port, String[] highAvailabilitySites) throws IOException {
        return connect(hostName, port, "", "", null, true, highAvailabilitySites);
    }

    public boolean connect(String hostName, int port, String initialScript, String[] highAvailabilitySites) throws IOException {
        return connect(hostName, port, "", "", initialScript, true, highAvailabilitySites);
    }

    public boolean connect(String hostName, int port, String userId, String password) throws IOException {
        return connect(hostName, port, userId, password, null, false, null);
    }

    public boolean connect(String hostName, int port, String userId, String password, boolean enableHighAvailability) throws IOException {
        return connect(hostName, port, userId, password, null, enableHighAvailability, null);
    }

    public boolean connect(String hostName, int port, String userId, String password, String[] highAvailabilitySites) throws IOException {
        return connect(hostName, port, userId, password, null, true, highAvailabilitySites);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript) throws IOException {
        return connect(hostName, port, userId, password, initialScript, false, null);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript, boolean enableHighAvailability) throws IOException {
        return connect(hostName, port, userId, password, initialScript, enableHighAvailability, null);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript, String[] highAvailabilitySites) throws IOException {
        return connect(hostName, port, userId, password, initialScript, true, highAvailabilitySites);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript, boolean enableHighAvailability, String[] highAvailabilitySites) throws IOException {
        return connect(hostName, port, userId, password, initialScript, enableHighAvailability, highAvailabilitySites, false);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript, boolean enableHighAvailability, String[] highAvailabilitySites, boolean reconnect) throws IOException {
        mutex_.lock();
        try {
            this.uid_ = userId;
            this.pwd_ = password;
            this.initialScript_ = initialScript;
            this.enableHighAvailability_ = enableHighAvailability;
            this.highAvailabilitySites_ = highAvailabilitySites;

            if (enableHighAvailability){
                nodes_.add(new Node(hostName, port));
                if (highAvailabilitySites != null){
                    for (String site : highAvailabilitySites) {
                        Node node = new Node(site);
                        nodes_.add(node);
                    }
                }
                Node connectedNode = new Node();
                BasicTable bt = null;
                while (!closed_){
                    while (!conn_.isConnected() && !closed_){
                        for (Node one : nodes_){
                            if (connectNode(one)){
                                connectedNode = one;
                                break;
                            }
                            try {
                                Thread.sleep(100);
                            }catch (Exception e){
                                e.printStackTrace();
                                return false;
                            }
                        }
                    }
                    try {
                        bt = (BasicTable) conn_.run("rpc(getControllerAlias(), getClusterPerf)");
                        break;
                    }catch (Exception e){
                        System.out.println("ERROR getting other data nodes, exception: " + e.getMessage());
                        Node node1 = new Node();
                        if (isConnected()){
                            ExceptionType type = parseException(e.getMessage(), node1);
                            if (type == ExceptionType.ET_IGNORE){
                                continue;
                            }else if (type == ExceptionType.ET_NEWLEADER || type == ExceptionType.ET_NODENOTAVAIL){
                                switchDataNode(node1);
                            }
                        }else {
                            switchDataNode(node1);
                        }
                    }
                }

                BasicStringVector colHost = (BasicStringVector) bt.getColumn("host");
                BasicIntVector colPort = (BasicIntVector) bt.getColumn("port");
                BasicIntVector colMode = (BasicIntVector) bt.getColumn("mode");
                BasicIntVector colmaxConnections = (BasicIntVector) bt.getColumn("maxConnections");
                BasicIntVector colconnectionNum = (BasicIntVector) bt.getColumn("connectionNum");
                BasicIntVector colworkerNum = (BasicIntVector) bt.getColumn("workerNum");
                BasicIntVector colexecutorNum = (BasicIntVector) bt.getColumn("executorNum");
                double load;
                for (int i = 0; i < colMode.rows(); i++){
                    if (colMode.getInt(i) == 0){
                        Node nodex = new Node(colHost.getString(i), colPort.getInt(i));
                        Node pexistNode = null;
                        if (highAvailabilitySites != null){
                            for (Node node : nodes_){
                                if (node.hostName.equals(nodex.hostName) && node.port == nodex.port){
                                    pexistNode = node;
                                    break;
                                }
                            }
                            //node is out of highAvailabilitySites
                            if (pexistNode == null)
                                continue;
                        }

                        if (colconnectionNum.getInt(i) < colmaxConnections.getInt(i)){
                            load = (colconnectionNum.getInt(i) + colworkerNum.getInt(i) + colexecutorNum.getInt(i)) / 3.0;
                        }else {
                            load = Double.MAX_VALUE;
                        }

                        if (pexistNode != null){
                            pexistNode.load = load;
                        }else {
                            nodes_.add(new Node(colHost.getString(i), colPort.getInt(i), load));
                        }
                    }
                }

                Node pMinNode = null;
                for (Node one : nodes_){
                    if (pMinNode == null || pMinNode.load == -1 || pMinNode.load > one.load){
                        pMinNode = one;
                    }
                }

                if (!pMinNode.isEqual(connectedNode)){
                    System.out.println("Connect to min load node: " + pMinNode.hostName + ":" + pMinNode.port);
                    conn_.close();
                    switchDataNode(pMinNode);
                    return true;
                }
            }else {
                if (reconnect){
                    nodes_.add(new Node(hostName, port));
                    switchDataNode(new Node(hostName, port));
                }else {
                    if (!connectNode(new Node(hostName, port)))
                        return false;
                }
            }

            if (initialScript_!=null && initialScript_.length() > 0){
                run(initialScript_);
            }
            return true;
        }catch (IOException e){
            throw e;
        }finally {
            mutex_.unlock();
        }
    }

    public void switchDataNode(Node node) throws IOException{
        boolean connected = false;
        do {
            if (node.hostName != null && node.hostName.length() > 0){
                if (connectNode(node)){
                    connected = true;
                    break;
                }
            }
            if (nodes_.isEmpty()){
                throw new RuntimeException("Failed to connect to " + node.hostName + ":" + node.port);
            }
            for (int i = nodes_.size()-1; i >= 0; i--){
                lastConnNodeIndex_ = (lastConnNodeIndex_ + 1) % nodes_.size();
                if (connectNode(nodes_.get(lastConnNodeIndex_))){
                    connected = true;
                    break;
                }
            }
            try {
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
                return;
            }
        }while (!connected && !closed_);
        if (initialScript_ != null && initialScript_.length() > 0){
            run(initialScript_);
        }
    }

    public boolean connectNode(Node node) throws IOException{
        System.out.println("Connect to " + node.hostName + ":" + node.port + ".");
        while (!closed_){
            try {
                return conn_.connect(node.hostName, node.port, uid_, pwd_, enableSSL_, asynTask_, compress_);
            }catch (Exception e){
                if (isConnected()){
                    ExceptionType type = parseException(e.getMessage(), node);
                    if (type != ExceptionType.ET_NEWLEADER){
                        if (type == ExceptionType.ET_IGNORE)
                            return true;
                        else if (type == ExceptionType.ET_NODENOTAVAIL)
                            return false;
                        else
                            throw e;
                    }
                }else {
                    return false;
                }
            }
            try {
                Thread.sleep(100);
            }catch (Exception e){
                e.printStackTrace();
                return false;
            }
        }
        return false;
    }

    public ExceptionType parseException(String msg, Node node){
        int index = msg.indexOf("<NotLeader>");
        if (index != -1){
            index = msg.indexOf(">");
            String ipport = msg.substring(index + 1);
            parseIpPort(ipport, node);
            System.out.println("New leader is " + node.hostName + ":" + node.port);
            return ExceptionType.ET_NEWLEADER;
        }else {
            index = msg.indexOf("<DataNodeNotAvail>");
            if (index == -1){
                return ExceptionType.ET_UNKNOW;
            }
            index = msg.indexOf(">");
            String ipport = msg.substring(index + 1);
            Node nanode = new Node();
            parseIpPort(ipport, nanode);
            Node lastnode = new Node();
            conn_.getNode(lastnode);
            if (lastnode.hostName == nanode.hostName && lastnode.port == nanode.port){
                System.out.println("This node " + nanode.hostName + ":" + nanode.port + " is not avail.");
                return ExceptionType.ET_NODENOTAVAIL;
            }else {
                System.out.println("Other node " + nanode.hostName + ":" + nanode.port + " is not avail.");
                return ExceptionType.ET_IGNORE;
            }
        }
    }

    public void parseIpPort(String ipport, Node node){
        String[] v = ipport.split(":");
        if (v.length < 2){
            throw new RuntimeException("The ipPort '" + ipport + "' is invalid.");
        }
        node.hostName = v[0];
        node.port = Integer.parseInt(v[1]);
    }

    public boolean connected(){
        try {
            BasicInt ret= (BasicInt) conn_.run("1+1");
            return !ret.isNull() && (ret.getInt() == 2);
        }catch (Exception e){
            return false;
        }
    }

    public void login(String userID, String password, boolean enableEncryption)throws IOException{
        conn_.login(userID, password, enableEncryption);
        uid_ = userID;
        pwd_ = password;
    }

    public boolean getRemoteLittleEndian() {
        return this.conn_.getRemoteLittleEndian();
    }

    public Entity tryRun(String script) throws IOException {
        return tryRun(script, DEFAULT_PRIORITY, DEFAULT_PARALLELISM);
    }

    public Entity tryRun(String script, int priority, int parallelism) throws IOException {
        return tryRun(script, priority, parallelism, 0, false);
    }

    public Entity tryRun(String script, int priority, int parallelism, boolean clearSessionMemory) throws IOException {
        return tryRun(script, priority, parallelism, 0, clearSessionMemory);
    }

    public Entity tryRun(String script, int priority, int parallelism,int fetchSize, boolean clearSessionMemory) throws IOException {
        if (!mutex_.tryLock())
            return null;
        try {
            return run(script, (ProgressListener) null, priority, parallelism, fetchSize, clearSessionMemory);
        } finally {
            mutex_.unlock();
        }
    }

    public Entity run(String script) throws IOException {
        return run(script, (ProgressListener) null, DEFAULT_PRIORITY, DEFAULT_PARALLELISM);
    }

    public Entity run(String script, int priority) throws IOException {
        return run(script, (ProgressListener) null, priority, DEFAULT_PARALLELISM);
    }

    public Entity run(String script, int priority, int parallelism) throws IOException {
        return run(script, (ProgressListener) null, priority, parallelism);
    }

    public Entity run(String script, ProgressListener listener) throws IOException {
        return run(script, listener, DEFAULT_PRIORITY, DEFAULT_PARALLELISM);
    }

    public Entity run(String script, ProgressListener listener, boolean clearSessionMemory) throws IOException {
        return run(script, listener, DEFAULT_PRIORITY, DEFAULT_PARALLELISM, 0, clearSessionMemory);
    }

    public Entity run(String script, int priority, boolean clearSessionMemory) throws IOException {
        return run( script, (ProgressListener)null,  priority, DEFAULT_PARALLELISM, 0, clearSessionMemory);
    }

    public Entity run(String script, ProgressListener listener, int priority, int parallelism) throws IOException {
        return run( script, listener,  priority, parallelism, 0,false);
    }

    public Entity run(String script, ProgressListener listener, int priority, int parallelism, boolean clearSessionMemory) throws IOException {
        return run( script, listener,  priority, parallelism, 0,clearSessionMemory);
    }

    public Entity run(String script, int priority, int parallelism, boolean clearSessionMemory) throws IOException {
        return run(script, (ProgressListener)null,  priority, parallelism, 0,clearSessionMemory);
    }

    public Entity run(String script, ProgressListener listener, int priority, int parallelism, int fetchSize) throws IOException {
        return run( script, listener,  priority, parallelism, fetchSize,false);
    }

    public Entity tryRun(String script, boolean clearSessionMemory) throws IOException {
        if (!mutex_.tryLock())
            return null;
        try {
            return run(script, (ProgressListener) null, DEFAULT_PRIORITY, DEFAULT_PARALLELISM, 0, clearSessionMemory);
        } finally {
            mutex_.unlock();
        }
    }

    public Entity run(String script, boolean clearSessionMemory) throws IOException {
        return run(script, (ProgressListener) null, DEFAULT_PRIORITY, DEFAULT_PARALLELISM,0, clearSessionMemory);
    }

    public Entity run(String script, ProgressListener listener, int priority, int parallelism, int fetchSize, boolean clearSessionMemory) throws IOException{
        mutex_.lock();
        try {
            if (!nodes_.isEmpty()) {
                while (!closed_) {
                    try {
                        return conn_.run(script, listener, priority, parallelism, fetchSize, clearSessionMemory);
                    } catch (IOException e) {
                        Node node = new Node();
                        if (connected()) {
                            ExceptionType type = parseException(e.getMessage(), node);
                            if (type == ExceptionType.ET_IGNORE)
                                return new Void();
                            else if (type == ExceptionType.ET_UNKNOW)
                                throw e;
                        }
                        switchDataNode(node);
                    }
                }
                return null;
            } else {
                return conn_.run(script, listener, priority, parallelism, fetchSize, clearSessionMemory);
            }
        } finally {
            mutex_.unlock();
        }
    }

    public Entity tryRun(String function, List<Entity> arguments) throws IOException {
        return tryRun(function, arguments, DEFAULT_PRIORITY, DEFAULT_PARALLELISM);
    }

    public Entity tryRun(String function, List<Entity> arguments, int priority, int parallelism) throws IOException {
            return tryRun(function, arguments, priority, parallelism,0);
    }
    public Entity tryRun(String function, List<Entity> arguments, int priority, int parallelism, int fetchSize) throws IOException {
        if (!mutex_.tryLock())
            return null;
        try {
            return run(function, arguments, priority, parallelism, fetchSize );
        } finally {
            mutex_.unlock();
        }
    }

    public Entity run(String function, List<Entity> arguments) throws IOException {
        return run(function, arguments, DEFAULT_PRIORITY, DEFAULT_PARALLELISM);
    }

    public Entity run(String function, List<Entity> arguments, int priority) throws IOException {
        return run(function, arguments, priority, DEFAULT_PARALLELISM);
    }

    public Entity run(String function, List<Entity> arguments, int priority, int parallelism) throws IOException {
        return run(function, arguments, priority, parallelism, 0);
    }

    public Entity run(String function, List<Entity> arguments, int priority, int parallelism, int fetchSize) throws IOException {
        mutex_.lock();
        try {
            if (!nodes_.isEmpty()){
                while (!closed_){
                    try {
                        return conn_.run(function, (ProgressListener)null, arguments, priority, parallelism, fetchSize, false);
                    }catch (IOException e){
                        Node node = new Node();
                        if (connected()){
                            ExceptionType type = parseException(e.getMessage(), node);
                            if (type == ExceptionType.ET_IGNORE)
                                return new Void();
                            else if (type == ExceptionType.ET_UNKNOW)
                                throw e;
                        }
                        switchDataNode(node);
                    }
                }
                return null;
            }else {
                return conn_.run(function, (ProgressListener)null, arguments, priority, parallelism, fetchSize, false);
            }
        }finally {
            mutex_.unlock();
        }
    }



    public void tryUpload(final Map<String, Entity> variableObjectMap) throws IOException {
        if (!mutex_.tryLock())
            throw new IOException("The connection is in use.");
        try {
            upload(variableObjectMap);
        } finally {
            mutex_.unlock();
        }
    }

    public void upload(final Map<String, Entity> variableObjectMap) throws IOException {
        mutex_.lock();
        try {
            List<String> keys = new ArrayList<>();
            List<Entity> objs = new ArrayList<>();
            if (!nodes_.isEmpty()){
                while (!closed_){
                    try {
                        for (String key : variableObjectMap.keySet()){
                            if (variableObjectMap.size() == 1){
                                Entity obj = variableObjectMap.get(key);
                                conn_.upload(key, obj);
                            }else {
                                keys.add(key);
                                objs.add(variableObjectMap.get(key));
                            }
                        }
                        if (variableObjectMap.size() > 1)
                            conn_.upload(keys, objs);
                    }catch (Exception e){
                        Node node = new Node();
                        if (connected()){
                            ExceptionType type = parseException(e.getMessage(), node);
                            if (type == ExceptionType.ET_IGNORE)
                                continue;
                            else if (type == ExceptionType.ET_UNKNOW)
                                throw e;
                        }
                        switchDataNode(node);
                    }
                }
            }else {
                for (String key : variableObjectMap.keySet()){
                    if (variableObjectMap.size() == 1){
                        Entity obj = variableObjectMap.get(key);
                        conn_.upload(key, obj);
                    }else {
                        keys.add(key);
                        objs.add(variableObjectMap.get(key));
                    }
                }
                if (variableObjectMap.size() > 1)
                    conn_.upload(keys, objs);
            }
        }finally {
            mutex_.unlock();
        }
    }

    public void close() {
        mutex_.lock();
        try {
            closed_ = true;
            conn_.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            mutex_.unlock();
        }
    }

    public String getHostName() {
        return this.conn_.hostName_;
    }

    public int getPort() {
        return this.conn_.port_;
    }

    public String getSessionID() {
        return this.conn_.sessionID_;
    }

    public InetAddress getLocalAddress() {
        return this.conn_.socket_.getLocalAddress();
    }

    public boolean isConnected() {
        return this.conn_.socket_ != null && this.conn_.socket_.isConnected();
    }

    private SSLSocketFactory getSSLSocketFactory(){
        try {
            SSLContext context = SSLContext.getInstance("SSL");
            context.init(null,
                    new TrustManager[]{new X509TrustManager() {
                        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

                        }

                        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

                        }

                        public X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }
                    }
                    },
                    new java.security.SecureRandom());
            return context.getSocketFactory();
        }catch (Exception ex){
            ex.printStackTrace();
            return null;
        }
     }
     private void compareRequiredAPIVersion() throws IOException {
        try {
            Entity ret = run("getRequiredAPIVersion(`java)");
            if (localAPIVersion < ((BasicInt)((BasicAnyVector) ret).get(0)).getInt()) {
                throw new IOException("API version is too low and needs to be upgraded");
            }
        }catch (IOException e){
            if(!e.getMessage().equals("Syntax Error: [line #1] Cannot recognize the token getRequiredAPIVersion")){
                throw e;
            }
        }
     }
}
