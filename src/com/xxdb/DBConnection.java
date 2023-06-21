package com.xxdb;

import java.io.*;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import com.xxdb.comm.SqlStdEnum;
import com.xxdb.data.*;
import com.xxdb.data.Void;
import com.xxdb.io.*;

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
    private static final int MAX_TYPE_VALUE = Entity.DATA_TYPE.DT_DECIMAL128_ARRAY.getValue();
    private static final int DEFAULT_PRIORITY = 4;
    private static final int DEFAULT_PARALLELISM = 2;

    private ReentrantLock mutex_;
    private DBConnectionImpl conn_;
    private String uid_;
    private String pwd_;
    private String initialScript_ = null;
    private boolean enableHighAvailability_;
    private List<Node> nodes_ = new ArrayList<>();
    private Random nodeRandom_ = new Random();
    private int connTimeout_ = 0;
    private boolean closed_ = false;
    private boolean loadBalance_ = true;
    private String runClientId_ = null;
    private long runSeqNo_ = 0;
    private int[] serverVersion_;
    private boolean isReverseStreaming_ = false;


    private enum ServerExceptionState {
        NEW_LEADER, WAIT, CONN_FAIL, OTHER_EXCEPTION, DATA_NODE_NOT_AVAILABLE
    }

    private enum ExceptionType{
        ET_IGNORE(0),
        ET_UNKNOW(1),
        ET_NEWLEADER(2),
        ET_NODENOTAVAIL(3),
        ET_NOINITIALIZED(4),
        ET_NOTLEADER(5);

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

    enum Property{
        flag,//1
        cancel,//2
        priority,//3
        parallelism,//4
        jobId,//5
        fetchSize,//6
        offset,//7
        clientId,//8
        seqNo,//9
    }

    private class DBConnectionImpl{
        private Socket socket_;
        private String sessionID_;
        private String hostName_;
        private int port_;
        private String userId_;
        private String pwd_;
        private boolean encrypted_ = true;
        private boolean isConnected_;
        private boolean sslEnable_ = false;
        private boolean asynTask_ = false;
        private boolean compress_ = false;
        private boolean ifUrgent_ = false;
        private int connTimeout_ = 0;
        private ExtendedDataOutput out_;
        private boolean remoteLittleEndian_;
        private ReentrantLock lock_;
        private boolean isReverseStreaming_ = false;
        private boolean python_ = false;
        private SqlStdEnum sqlStd_;


        private DBConnectionImpl(boolean asynTask, boolean sslEnable, boolean compress, boolean python, boolean ifUrgent, boolean isReverseStreaming, SqlStdEnum sqlStd){
            sessionID_ = "";
            this.sslEnable_ = sslEnable;
            this.asynTask_ = asynTask;
            this.compress_ = compress;
            this.ifUrgent_ = ifUrgent;
            this.python_ = python;
            this.isReverseStreaming_ = isReverseStreaming;
            this.sqlStd_ = sqlStd;
            this.lock_ = new ReentrantLock();
        }

        private boolean connect(String hostName, int port, String userId, String password, int connTimeout) throws IOException{
            this.hostName_ = hostName;
            this.port_ = port;
            this.userId_ = userId;
            this.pwd_ = password;
            this.connTimeout_ = connTimeout;
            return connect();
        }

        private boolean connect()throws IOException{
            this.isConnected_ = false;

            try {
                if(sslEnable_)
                    socket_ = getSSLSocketFactory().createSocket();
                else
                    socket_ = new Socket();
                if (this.connTimeout_ > 0){
                    socket_.connect(new InetSocketAddress(hostName_,port_), connTimeout_);
                }else {
                    socket_.connect(new InetSocketAddress(hostName_,port_), 3000);
                }
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

            if (!userId_.isEmpty() && !pwd_.isEmpty()) {
                if (asynTask_) {
                    login(userId_, pwd_, false);
                } else {
                    login();
                }
            }

            return true;
        }

        private int generateRequestFlag(boolean clearSessionMemory){
            int flag = 0;
            if (this.ifUrgent_)
                flag += 1;
            if(this.asynTask_)
                flag += 4;
            if(clearSessionMemory)
                flag += 16;
            if(this.compress_)
                flag += 64;
            if (this.python_)
                flag += 2048;
            if (this.isReverseStreaming_)
                flag += 131072;
            if (Objects.nonNull(this.sqlStd_)) {
                flag += sqlStd_.getCode()<<19;
            }
            return flag;
        }

        private void login(String userId, String password, boolean enableEncryption) throws IOException {
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
                BasicString keyCode = (BasicString) run("getDynamicPublicKey", new ArrayList<Entity>(),0);
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
            run("login", args, 0);
        }

        private Entity run(String script,long seqNum) throws IOException {
            List<Entity> args = new ArrayList<>();
            return run(script, "script", (ProgressListener)null, args, DEFAULT_PRIORITY, DEFAULT_PARALLELISM, 0, false, seqNum);
        }

        private Entity run(String script, ProgressListener listener,int priority, int parallelism, int fetchSize, boolean clearMemory, String tableName, long seqNum) throws IOException{
            List<Entity> args = new ArrayList<>();
            return run(script, "script", listener, args, priority, parallelism, fetchSize, clearMemory, tableName,seqNum);
        }
        private Entity run(String function, List<Entity> arguments,long seqNum) throws IOException {
            return run(function,"function", (ProgressListener)null, arguments, DEFAULT_PRIORITY, DEFAULT_PARALLELISM, 0, false, seqNum);
        }

        private Entity run(String function, String scriptType, List<Entity> arguments,long seqNum)throws IOException{
            return run(function, scriptType, (ProgressListener)null, arguments, DEFAULT_PRIORITY, DEFAULT_PARALLELISM, 0, false, seqNum);
        }

        private Entity run(String function, ProgressListener listener,List<Entity> args, int priority, int parallelism, int fetchSize, boolean clearMemory,long seqNum) throws IOException{
            return run(function, "function", listener, args, priority, parallelism, fetchSize, clearMemory,seqNum);
        }

        private Entity run(String script, String scriptType, ProgressListener listener, List<Entity> args, int priority, int parallelism, int fetchSize, boolean clearMemory,long seqNum) throws IOException{
            return run(script, scriptType, listener, args, priority, parallelism, fetchSize, clearMemory, "", seqNum);
        }
        //flag) + "_1_" + String.valueOf(priority) + "_" + String.valueOf(parallelism));
        //                if (fetchSize > 0)
        //                    out_.writeBytes("__" + String.valueOf(fetchSize
        private Entity run(String script, String scriptType, ProgressListener listener, List<Entity> args, int priority, int parallelism, int fetchSize, boolean clearMemory, String tableName, long seqNum) throws IOException{
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
                }
            }


            if (!tableName.equals("")){
                script = tableName + "=" + script;
                run(script,0);
                BasicDictionary schema = (BasicDictionary) run(tableName + ".schema()",0);
                BasicTable colDefs = (BasicTable)schema.get(new BasicString("colDefs"));
                BasicStringVector colDefsName = (BasicStringVector)colDefs.getColumn("name");
                BasicIntVector colDefsTypeInt = (BasicIntVector)colDefs.getColumn("typeInt");
                int cols = colDefs.rows();
                int rows = ((BasicInt) run("rows(" + tableName + ")",0)).getInt();
                Map<Integer, Entity.DATA_TYPE> types2Index = new HashMap<>();
                Map<Integer, String> name2Index = new HashMap<>();
                for (int i = 0; i < cols; i++){
                    types2Index.put(types2Index.size(), Entity.DATA_TYPE.valueOf(colDefsTypeInt.getInt(i)));
                    name2Index.put(name2Index.size(), colDefsName.getString(i));
                }
                return new BasicTableSchema(types2Index, name2Index, rows, cols, tableName,DBConnection.this);
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
                out_.writeBytes(String.valueOf(AbstractExtendedDataOutputStream.getUTFlength(body.toString(), 0, 0)));
                int flag = generateRequestFlag(clearMemory);
                Map<Property,Object> properties=new HashMap<>();
                properties.put(Property.flag, flag);
                properties.put(Property.cancel,1);
                properties.put(Property.priority, priority);
                properties.put(Property.parallelism, parallelism);
                //String propInfo=" / " + String.valueOf(flag) + "_1_" + String.valueOf(priority) + "_" + String.valueOf(parallelism);
                //out_.writeBytes(" / " + String.valueOf(flag) + "_1_" + String.valueOf(priority) + "_" + String.valueOf(parallelism));
                //if (fetchSize > 0)
                //    out_.writeBytes("__" + String.valueOf(fetchSize));
                //out_.writeByte('\n');
                if (fetchSize > 0)
                    properties.put(Property.fetchSize, fetchSize);
                if(enableHighAvailability_ && runClientId_ != null && seqNum != 0){
                    properties.put(Property.clientId, runClientId_);
                    properties.put(Property.seqNo, seqNum);
                }
                {//write properties
                    int lastNotNullValue = -1;
                    StringBuilder sbProp = new StringBuilder(" / ");
                    for (Property key : Property.values()) {
                        Object value = properties.get(key);
                        if (value != null) {
                            sbProp.append(value);
                            lastNotNullValue = sbProp.length();
                        }
                        sbProp.append("_");
                    }
                    sbProp.delete(lastNotNullValue, sbProp.length());
                    sbProp.append('\n');
                    out_.writeBytes(sbProp.toString());
                }
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
                while (header.equals("MSG")) {
                    //read intermediate message to indicate the progress
                    String msg = in.readString();
                    if (listener != null)
                        listener.progress(msg);
                    header = in.readLine();
                }
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


            int numObject = Integer.parseInt(headers[1]);

            try {
                header = in.readLine();
            }catch (IOException ex){
                isConnected_ = false;
                socket_ = null;
                throw new IOException("Failed to read response header from the socket with IO error " + ex.getMessage());
            }

            if (!header.equals("OK")){
                if (scriptType == "script")
                    throw new IOException(hostName_+":"+port_+" Server response: '" + header + "' script: '" + script + "'");
                else
                    throw new IOException(hostName_+":"+port_+" Server response: '" + header + "' " + scriptType + ": '" + script + "'");
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

        public void upload(String name, Entity obj,long seqNum) throws IOException{
            if (!Utils.isVariableCandidate(name))
                throw new RuntimeException(name + " is not a qualified variable name.");
            List<Entity> args = new ArrayList<>();
            args.add(obj);
            run(name, "variable", args, seqNum);
        }

        public void upload(List<String> names, List<Entity> objs,long seqNum) throws IOException{
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

            run(varNames.toString(), "variable", objs, seqNum);
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

    public DBConnection(SqlStdEnum sqlStd) {
        this(false, false, false, false, false, false, sqlStd);
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

    public DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean usePython){
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, usePython, false, false, SqlStdEnum.DolphinDB);
        this.mutex_ = new ReentrantLock();
    }


    public DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean usePython, SqlStdEnum sqlStd){
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, usePython, false, false, sqlStd);
        this.mutex_ = new ReentrantLock();
    }

    public DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean usePython, boolean isUrgent){
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, usePython, isUrgent, false, SqlStdEnum.DolphinDB);
        this.mutex_ = new ReentrantLock();
    }

    public DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean usePython, boolean isUrgent, boolean isReverseStreaming, SqlStdEnum sqlStd){
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, usePython, isUrgent, isReverseStreaming, sqlStd);
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

    public void setLoadBalance(boolean loadBalance){
        this.loadBalance_ = loadBalance;
    }

    public boolean connect(String hostName, int port) throws IOException {
        return connect(hostName, port, "", "", null, false, null);
    }

    public boolean connect(String hostName, int port, int timeout) throws IOException {
        this.connTimeout_ = timeout;
        return connect(hostName, port, "", "", null, false, null);
    }

    public boolean connect(String hostName, int port, int timeout, boolean reconnect) throws IOException {
        this.connTimeout_ = timeout;
        return connect(hostName, port, "", "", null, false, null, reconnect);
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
                        bt = (BasicTable) conn_.run("select host,port,(memoryUsed/1024/1024/1024)/maxMemSize as memLoad,ratio(connectionNum,maxConnections) as connLoad,avgLoad from rpc(getControllerAlias(),getClusterPerf) where mode=0",0);
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

                if ( bt!=null && bt.getDataForm() != Entity.DATA_FORM.DF_TABLE){
                    throw new IOException("Run getClusterPerf() failed.");
                }

                if (bt!=null && loadBalance_){
                    //ignore very high load nodes, rand one in low load nodes
                    List<Node> lowLoadNodes=new ArrayList<>();
                    BasicStringVector colHost = (BasicStringVector) bt.getColumn("host");
                    BasicIntVector colPort = (BasicIntVector) bt.getColumn("port");
                    BasicDoubleVector memLoad = (BasicDoubleVector) bt.getColumn("memLoad");
                    BasicDoubleVector connLoad = (BasicDoubleVector) bt.getColumn("connLoad");
                    BasicDoubleVector avgLoad = (BasicDoubleVector) bt.getColumn("avgLoad");
                    for (int i = 0; i < colHost.rows(); i++){
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
                        double load=(memLoad.getDouble(i)+connLoad.getDouble(i)+avgLoad.getDouble(i))/3.0;
                        if (pexistNode != null){
                            pexistNode.load = load;
                        }else {
                            pexistNode=new Node(colHost.getString(i), colPort.getInt(i), load);
                            nodes_.add(pexistNode);
                        }
                        //low load
                        if(memLoad.getDouble(i)<0.8&&
                                connLoad.getDouble(i)<0.9&&
                                avgLoad.getDouble(i)<0.8){
                            lowLoadNodes.add(pexistNode);
                        }
                    }
                    Node pMinNode;
                    if(lowLoadNodes.isEmpty()==false){
                        pMinNode=lowLoadNodes.get(nodeRandom_.nextInt(lowLoadNodes.size()));
                    }else{
                        pMinNode=nodes_.get(nodeRandom_.nextInt(nodes_.size()));
                    }
                    if (pMinNode != null && !pMinNode.isEqual(connectedNode)){
                        System.out.println("Switch to node: " + pMinNode.hostName + ":" + pMinNode.port);
                        conn_.close();
                        switchDataNode(pMinNode);
                    }
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
            InitConnection();
            return true;
        } finally {
            mutex_.unlock();
        }
    }

    private void InitConnection() throws IOException{
        runClientId_ = null;
        if(enableHighAvailability_){
            if(getServerVersion()) {
                if (checkClientIdValid()) {
                    runClientId_ = BasicUuid.random().getString();
                    runSeqNo_ = 0;
                }
            }
        }
        if (initialScript_!=null && initialScript_.length() > 0){
            run(initialScript_);
        }
    }

    public void switchDataNode(Node node) throws IOException{
        do {
            if (node.hostName != null && node.hostName.length() > 0){
                if (connectNode(node)){
                    break;
                }
            }
            if (nodes_.isEmpty()){
                throw new RuntimeException("Failed to connect to " + node.hostName + ":" + node.port);
            }
            int index = nodeRandom_.nextInt(nodes_.size());
            if (connectNode(nodes_.get(index))){
                break;
            }
            try {
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
                return;
            }
        }while (!closed_);
        if (initialScript_!=null && initialScript_.length() > 0){
            run(initialScript_);
        }
    }

    public boolean connectNode(Node node) throws IOException{
        System.out.println("Connect to " + node.hostName + ":" + node.port + ".");
        while (!closed_){
            try {
                return conn_.connect(node.hostName, node.port, uid_, pwd_, connTimeout_);
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
                    System.out.println(e.getMessage());
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
            if (!Pattern.matches("\\d+", ipport)) {
                System.out.println("The control node you are accessing is not the leader node of the highly available (raft) cluster.");
                return ExceptionType.ET_NOTLEADER;
            } else {
                parseIpPort(ipport, node);
                System.out.println("New leader is " + node.hostName + ":" + node.port);
                return ExceptionType.ET_NEWLEADER;
            }
        }else if ((index = msg.indexOf("<DataNodeNotAvail>")) != -1){
            index = msg.indexOf(">");
            String ipport = msg.substring(index + 1);
            Node newNode = new Node();
            parseIpPort(ipport, newNode);
            Node lastNode = new Node();
            conn_.getNode(lastNode);
            node.hostName = "";
            node.port = 0;
            System.out.println(msg);
            return ExceptionType.ET_NODENOTAVAIL;
        }else if ((index = msg.indexOf("The datanode isn't initialized yet. Please try again later")) != -1){
            node.hostName = "";
            node.port = 0;
            return ExceptionType.ET_NOINITIALIZED;
        }else {
            node.hostName = "";
            node.port = 0;
            return ExceptionType.ET_UNKNOW;
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
            BasicInt ret= (BasicInt) conn_.run("1+1",0);
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

    public Entity run(String script, String tableName) throws IOException{
        return run(script, (ProgressListener) null, DEFAULT_PRIORITY, DEFAULT_PARALLELISM, 0, false, tableName);
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
        return run(script, listener, priority, parallelism, fetchSize, clearSessionMemory, "");
    }

    public Entity run(String script, ProgressListener listener, int priority, int parallelism, int fetchSize, boolean clearSessionMemory, String tableName) throws IOException{
        mutex_.lock();
        try {
            if (!nodes_.isEmpty()) {
                long curSeqNo = newSeqNo();
                while (!closed_) {
                    try {
                        return conn_.run(script, listener, priority, parallelism, fetchSize, clearSessionMemory, tableName, curSeqNo);
                    } catch (IOException e) {
                        if(curSeqNo>0)
                            curSeqNo = -curSeqNo;
                        Node node = new Node();
                        if (connected()) {
                            ExceptionType type = parseException(e.getMessage(), node);
                            if (type == ExceptionType.ET_IGNORE)
                                return new Void();
                            else if (type == ExceptionType.ET_UNKNOW)
                                throw e;
                        }else {
                            parseException(e.getMessage(), node);
                        }
                        switchDataNode(node);
                    }
                }
                return null;
            } else {
                return conn_.run(script, listener, priority, parallelism, fetchSize, clearSessionMemory, tableName, 0);
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
    private long newSeqNo(){
        mutex_.lock();
        runSeqNo_++;
        if(runSeqNo_ <= 0){
            runSeqNo_ = 1;
        }
        long res=runSeqNo_;
        mutex_.unlock();
        return res;
    }

    public Entity run(String function, List<Entity> arguments, int priority, int parallelism, int fetchSize) throws IOException {
        mutex_.lock();
        try {
            if (!nodes_.isEmpty()){
                long seqNo = newSeqNo();
                while (!closed_){
                    try {
                        return conn_.run(function, (ProgressListener)null, arguments, priority, parallelism, fetchSize, false,seqNo);
                    }catch (IOException e){
                        if(seqNo > 0)
                            seqNo = -seqNo;
                        Node node = new Node();
                        if (connected()){
                            ExceptionType type = parseException(e.getMessage(), node);
                            if (type == ExceptionType.ET_IGNORE)
                                return new Void();
                            else if (type == ExceptionType.ET_UNKNOW)
                                throw e;
                        }else {
                            parseException(e.getMessage(), node);
                        }
                        switchDataNode(node);
                    }
                }
                return null;
            }else {
                return conn_.run(function, (ProgressListener)null, arguments, priority, parallelism, fetchSize, false, 0);
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
                                conn_.upload(key, obj,0);
                            }else {
                                keys.add(key);
                                objs.add(variableObjectMap.get(key));
                            }
                        }
                        if (variableObjectMap.size() > 1)
                            conn_.upload(keys, objs,0);
                        break;
                    }catch (Exception e){
                        Node node = new Node();
                        if (connected()){
                            ExceptionType type = parseException(e.getMessage(), node);
                            if (type == ExceptionType.ET_IGNORE)
                                continue;
                            else if (type == ExceptionType.ET_UNKNOW)
                                throw e;
                        }else {
                            parseException(e.getMessage(), node);
                        }
                        switchDataNode(node);
                    }
                }
            }else {
                for (String key : variableObjectMap.keySet()){
                    if (variableObjectMap.size() == 1){
                        Entity obj = variableObjectMap.get(key);
                        conn_.upload(key, obj, 0);
                    }else {
                        keys.add(key);
                        objs.add(variableObjectMap.get(key));
                    }
                }
                if (variableObjectMap.size() > 1)
                    conn_.upload(keys, objs, 0);
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

    public String getUserID(){
        return this.conn_.userId_;
    }

    public String getPwd(){
        return this.conn_.pwd_;
    }

    public String getSessionID() {
        return this.conn_.sessionID_;
    }

    public InetAddress getLocalAddress() {
        return this.conn_.socket_.getLocalAddress();
    }

    public Socket getSocket(){return this.conn_.socket_;}


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

    private boolean getServerVersion() throws IOException {
        try {
            Entity ret = conn_.run("version()",0);
            if(ret==null){
                throw new IOException("run version failed");
            }
            String version = ret.getString();
            String[] verList=version.split(" ");
            if(verList.length > 1){
                version=verList[0];
            }
            verList = version.split("\\.");
            serverVersion_=new int[4];
            int size=verList.length;
            if(size>4)
                size=4;
            else if(size<3)
                return false;
            for(int i=0;i<size;i++){
                serverVersion_[i] = Integer.parseInt(verList[i]);
            }
        }catch (Exception e){
            throw new IOException("Run version failed error: "+e.getMessage());
        }
        return true;
    }
    //if serverVersion_ is greater or equal than version
    private boolean compareVersionGE(int[] version){
        for(int i=0;i<version.length;i++){
            if(serverVersion_[i] != version[i]){
                return serverVersion_[i] > version[i];
            }
        }
        return true;
    }
    private boolean checkClientIdValid(){
        if(serverVersion_[0]==1){//1.30.20.5
            return compareVersionGE(new int[]{1,30,20,5});
        }else if(serverVersion_[0]==2){//2.00.9
            return compareVersionGE(new int[]{2,0,9,0});
        }else
            return true;
    }
}
