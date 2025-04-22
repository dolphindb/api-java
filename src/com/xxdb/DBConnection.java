package com.xxdb;

import java.io.*;
import java.net.*;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import com.xxdb.comm.SqlStdEnum;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.data.Void;
import com.xxdb.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final int DEFAULT_PARALLELISM = 64;

    private ReentrantLock mutex_;
    private DBConnectionImpl conn_;
    private String uid_;
    private String pwd_;
    private String initialScript_ = null;
    private boolean enableHighAvailability_;
    private List<Node> nodes_ = new ArrayList<>();
    private Random nodeRandom_ = new Random();
    private int connTimeout_ = 0;
    private int connectTimeout_ = 0;
    private int readTimeout_ = 0;
    private boolean closed_ = false;
    private boolean loadBalance_ = false;
    private String runClientId_ = null;
    private long runSeqNo_ = 0;
    private int[] serverVersion_;
    private boolean isReverseStreaming_ = false;
    private int tryReconnectNums = -1;

    private static final Logger log = LoggerFactory.getLogger(DBConnection.class);

    private enum ExceptionType{
        ET_IGNORE(0),
        ET_UNKNOW(1),
        ET_NEWLEADER(2),
        ET_NODENOTAVAIL(3),
        ET_NOINITIALIZED(4),
        ET_NOTLEADER(5),
        ET_READTIMEDOUT(6),
        ET_NORESPONSEHEADER(7),
        ET_LOGINREQUIRED(8);

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

        public Node(String ipPort){
            String[] v = ipPort.split(":");
            if (v.length < 2){
                throw new RuntimeException("The ipPort '" + ipPort + "' is invalid.");
            }
            this.hostName = v[0];
            this.port = Integer.parseInt(v[1]);
            this.load = -1.0;
        }

        @Override
        public boolean equals(Object o){
            if(o instanceof Node) {
                Node node = (Node) o;
                if(node.hostName==null||hostName==null)
                    return false;
                int diff = hostName.compareTo(node.hostName);
                if (diff != 0)
                    return false;
                return port == node.port;
            }else{
                return false;
            }
        }
    }

    public static class ConnectConfig {
       private String hostName;
       private int port;
       private int connectTimeout = 0;
       private int readTimeout = 0;
       private String userId = "";
       private String password = "";
       private String initialScript = null;
       private boolean enableHighAvailability;
       private String[] highAvailabilitySites = null;
       private boolean reconnect;
       private boolean enableLoadBalance;
       private int tryReconnectNums = -1;

       // flag:
       private boolean ifUserSetEnableLoadBalance;

       private ConnectConfig() {}

       public static Builder builder() {
            return new Builder();
        }

       public static class Builder {
           private final ConnectConfig config;

           private Builder() {
               config = new ConnectConfig();
           }

           public Builder hostName(String hostName) {
               config.hostName = hostName;
               return this;
           }

           public Builder port(int port) {
               config.port = port;
               return this;
           }

           public Builder connectTimeout(int connectTimeout) {
               config.connectTimeout = connectTimeout;
               return this;
           }

           public Builder readTimeout(int readTimeout) {
               config.readTimeout = readTimeout;
               return this;
           }

           public Builder userId(String userId) {
               config.userId = userId;
               return this;
           }

           public Builder password(String password) {
               config.password = password;
               return this;
           }

           public Builder initialScript(String initialScript) {
               config.initialScript = initialScript;
               return this;
           }

           public Builder enableHighAvailability(boolean enableHighAvailability) {
               config.enableHighAvailability = enableHighAvailability;
               return this;
           }

           public Builder highAvailabilitySites(String[] highAvailabilitySites) {
               config.highAvailabilitySites = highAvailabilitySites;
               return this;
           }

           public Builder reconnect(boolean reconnect) {
               config.reconnect = reconnect;
               return this;
           }

           public Builder enableLoadBalance(boolean enableLoadBalance) {
               config.enableLoadBalance = enableLoadBalance;
               config.ifUserSetEnableLoadBalance = true;
               return this;
           }

           public Builder tryReconnectNums(int tryReconnectNums) {
               config.tryReconnectNums = tryReconnectNums;
               return this;
           }

           public ConnectConfig build() {
               validateConfig();
               return config;
           }

           private void validateConfig() {
               if (Utils.isEmpty(config.hostName))
                   throw new IllegalArgumentException("The param 'hostName' cannot be null or empty.");

               if (config.port <= 0 || config.port > 65535)
                   throw new IllegalArgumentException("The param 'port' cannot less than or equal to 0, and also cannot greater than 65535.");

               if (config.connectTimeout < 0 || config.readTimeout < 0)
                   throw new IllegalArgumentException("The param connectTimeout or readTimeout cannot less than zero.");

               if (config.enableHighAvailability && !config.ifUserSetEnableLoadBalance)
                   config.enableLoadBalance = true;
           }
       }

        public String getHostName() {
            return hostName;
        }

        public int getPort() {
            return port;
        }

        public int getConnectTimeout() {
            return connectTimeout;
        }

        public int getReadTimeout() {
            return readTimeout;
        }

        public String getUserId() {
            return userId;
        }

        public String getPassword() {
            return password;
        }

        public String getInitialScript() {
            return initialScript;
        }

        public boolean getEnableHighAvailability() {
            return enableHighAvailability;
        }

        public String[] getHighAvailabilitySites() {
            return highAvailabilitySites;
        }

        public boolean getReconnect() {
            return reconnect;
        }

        public boolean getEnableLoadBalance() {
            return enableLoadBalance;
        }

        public int getTryReconnectNums() {
            return tryReconnectNums;
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
        private boolean enableSCRAM;
        private boolean isConnected_;
        private boolean sslEnable_ = false;
        private boolean asynTask_ = false;
        private boolean compress_ = false;
        private boolean ifUrgent_ = false;
        private int connTimeout_ = 0;
        private int connectTimeout_ = 0;
        private int readTimeout_ = 0;
        private ExtendedDataInput in_;
        private ExtendedDataOutput out_;
        private boolean remoteLittleEndian_;
        private ReentrantLock lock_;
        private boolean isReverseStreaming_ = false;
        private boolean python_ = false;
        private SqlStdEnum sqlStd_;


        private DBConnectionImpl(boolean asynTask, boolean sslEnable, boolean compress, boolean python, boolean ifUrgent, boolean isReverseStreaming, SqlStdEnum sqlStd, boolean enableSCRAM) {
            sessionID_ = "";
            this.sslEnable_ = sslEnable;
            this.asynTask_ = asynTask;
            this.compress_ = compress;
            this.ifUrgent_ = ifUrgent;
            this.python_ = python;
            this.isReverseStreaming_ = isReverseStreaming;
            this.sqlStd_ = sqlStd;
            this.lock_ = new ReentrantLock();
            this.enableSCRAM = enableSCRAM;
        }

        private boolean connect(String hostName, int port, String userId, String password, int connTimeout, int connectTimeout, int readTimeout) throws IOException{
            this.hostName_ = hostName;
            this.port_ = port;
            this.userId_ = userId;
            this.pwd_ = password;
            this.connTimeout_ = connTimeout;
            this.connectTimeout_ = connectTimeout;
            this.readTimeout_ = readTimeout;
            return connect();
        }

        private boolean connect() throws IOException {
            this.isConnected_ = false;

            try {
                if (sslEnable_)
                    socket_ = getSSLSocketFactory().createSocket();
                else
                    socket_ = new Socket();

                // set 'connectTimeout' param to connect()
                if (this.connTimeout_ > 0 && this.connectTimeout_ == 0)
                    socket_.connect(new InetSocketAddress(hostName_, port_), connTimeout_);
                else if (this.connTimeout_ > 0 && this.connectTimeout_ > 0)
                    socket_.connect(new InetSocketAddress(hostName_, port_), connectTimeout_);
                else if (this.connTimeout_ == 0 && this.connectTimeout_ > 0)
                    socket_.connect(new InetSocketAddress(hostName_, port_), connectTimeout_);
                else if (this.connTimeout_ == 0 && this.connectTimeout_ == 0)
                    socket_.connect(new InetSocketAddress(hostName_, port_), 3000);
            } catch (IOException ex) {
                log.error("Connect to " + this.hostName_ + ":" + this.port_ + " failed.");
                throw ex;
            }

            // set 'readTimeout' param to setSoTimeout
            if (this.connTimeout_ > 0 && this.readTimeout_ == 0)
                socket_.setSoTimeout(this.connTimeout_);
            else if (this.connTimeout_ > 0 && this.readTimeout_ > 0)
                socket_.setSoTimeout(this.readTimeout_);
            else if (this.connTimeout_ == 0 && this.readTimeout_ > 0)
                socket_.setSoTimeout(this.readTimeout_);


            socket_.setKeepAlive(true);
            socket_.setTcpNoDelay(true);
            out_ = new LittleEndianDataOutputStream(new BufferedOutputStream(socket_.getOutputStream()));
            in_ = new LittleEndianDataInputStream(new BufferedInputStream(socket_.getInputStream()));
            String body = "connect\n";
            out_.writeBytes("API 0 ");
            out_.writeBytes(String.valueOf(body.length()));
            int flag = generateRequestFlag(false);
            out_.writeBytes(" / " + String.valueOf(flag) + "_1_" + String.valueOf(4) + "_" + String.valueOf(2));
            out_.writeByte('\n');
            out_.writeBytes(body);
            out_.flush();

            String line = in_.readLine();
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

            String msg = in_.readLine();

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
                    login(userId_, pwd_, encrypted_);
                }
            }

            log.info("Connect to " + this.hostName_ + ":" + this.port_ + " successfully.");
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
            this.userId_ = userId;
            this.pwd_ = password;

            if (enableSCRAM) {
                try {
                    scramLogin();
                } finally {
                    lock_.unlock();
                }
            } else {
                try {
                    scramLogin();
                } catch (IOException e) {
                    // try scram login first, if failed use normal login quietly
                }

                try {
                    this.encrypted_ = enableEncryption;
                    login();
                } finally {
                    lock_.unlock();
                }
            }
        }

        private void scramLogin() throws IOException {
            if (asynTask_)
                throw new IOException("SCRAM login is not supported in async mode.");

            List<Entity> args = new ArrayList<>();
            args.add(new BasicString(this.userId_));
            String clientNonce = CryptoUtils.generateNonce(16);
            args.add(new BasicString(clientNonce));

            Entity result;
            try {
                result = run("scramClientFirst", args, 0);
            } catch (IOException e) {
                if (e.getMessage().contains("Can't recognize function name scramClientFirst"))
                    throw new IOException("SCRAM login is unavailble on current server.");
                else if (e.getMessage().contains("sha256 authMode doesn't support scram authMode"))
                    throw new IOException("user '" + this.userId_ + "' doesn't support scram authMode.");
                else
                    throw e;
            }

            if (!result.isVector() || result.rows() != 3)
                throw new IOException("SCRAM login failed, server error: get server nonce failed.");

            String saltString = ((Vector) result).get(0).getString();
            byte[] salt = CryptoUtils.base64Decode(saltString, false);
            int iterCount = ((BasicInt) ((Vector) result).get(1)).getInt();
            String combinedNonce = ((Vector) result).get(2).getString();

            // calculate saltedPassword
            byte[] saltedPassword = CryptoUtils.pbkdf2HmacSha256(this.pwd_, salt, iterCount);

            // calculate clientKey
            byte[] clientKey = CryptoUtils.computeClientKey(saltedPassword);

            // calculate storedKey
            byte[] storedKey = CryptoUtils.computeStoredKey(clientKey);

            // calculate auth
            String authMessage = "n=" + userId_ + ",r=" + clientNonce + ","
                    + "r=" + combinedNonce + ",s=" + saltString + ",i=" + iterCount + ","
                    + "c=biws,r=" + combinedNonce;

            // calculate client signature
            byte[] clientSignature = CryptoUtils.computeClientSignature(storedKey, authMessage);

            // calculate client proof
            byte[] proof = CryptoUtils.computeProof(clientKey, clientSignature);

            // call scramClientFinal
            args.clear();
            args.add(new BasicString(userId_));
            args.add(new BasicString(combinedNonce));
            args.add(new BasicString(CryptoUtils.base64Encode(proof, false)));
            result = run("scramClientFinal", args, 0);
            if (!result.isScalar() || result.getDataType() != Entity.DATA_TYPE.DT_STRING)
                throw new IOException("SCRAM login failed, server error: get server signature failed.");

            // calculate serverKey
            byte[] serverKey = CryptoUtils.computeServerKey(saltedPassword);

            // check server signature
            byte[] serverSignature = CryptoUtils.computeServerSignature(serverKey, authMessage);

            if (!result.getString().isEmpty() && !CryptoUtils.base64Encode(serverSignature, false).equals(result.getString())) {
                close(); // if check failed, close connection
                throw new IOException("SCRAM login failed, Invalid server signature");
            }

            log.info("SCRAM login succeed.");
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

            String header = null;
            try {
                header = in_.readLine();
                while (header.equals("MSG")) {
                    //read intermediate message to indicate the progress
                    String msg = in_.readString();
                    if (listener != null)
                        listener.progress(msg);
                    header = in_.readLine();
                }
            }catch (IOException ex){
                if (ex instanceof SocketTimeoutException) {
                    // isConnected_ = true;
                    throw ex;
                } else {
                    isConnected_ = false;
                    socket_ = null;
                    throw new IOException("Failed to read response header from the socket with IO error " + ex.getMessage());
                }
            }

            String[] headers = header.split(" ");
            if (headers.length != 3){
                isConnected_ = false;
                socket_ = null;
                throw new IOException("Received invalid header");
            }


            int numObject = Integer.parseInt(headers[1]);

            try {
                header = in_.readLine();
            }catch (IOException ex){
                isConnected_ = false;
                socket_ = null;
                throw new IOException("Failed to read response header from the socket with IO error " + ex.getMessage());
            }

            if (!header.equals("OK")){
                if (scriptType == "script")
                    throw new IOException(hostName_+":"+port_+" Server response: " + header + " script: " + script + "");
                else
                    throw new IOException(hostName_+":"+port_+" Server response: " + header + " " + scriptType + ": " + script + "");
            }

            if (numObject == 0){
                return new Void();
            }

            short flag;
            try {
                flag = in_.readShort();
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
                    return new EntityBlockReader(in_);
                }
                EntityFactory factory = BasicEntityFactory.instance();
                return factory.createEntity(df, dt, in_, extended);
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

        public ExtendedDataInput getDataInputStream() {
            return in_;
        }
    }

    private DBConnectionImpl createEnableReverseStreamingDBConnectionImpl(boolean asynTask, boolean sslEnable, boolean compress, boolean python, boolean ifUrgent, SqlStdEnum sqlStd) {
        return new DBConnectionImpl(asynTask, sslEnable, compress, python, ifUrgent, true, sqlStd, false);
    }

    public DBConnection() {
    	this(false, false, false);
    }

    public DBConnection(SqlStdEnum sqlStd) {
        this(false, false, false, false, false, sqlStd);
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
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, usePython, false, false, SqlStdEnum.DolphinDB, false);
        this.mutex_ = new ReentrantLock();
    }


    public DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean usePython, SqlStdEnum sqlStd){
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, usePython, false, false, sqlStd, false);
        this.mutex_ = new ReentrantLock();
    }

    public DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean usePython, boolean isUrgent){
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, usePython, isUrgent, false, SqlStdEnum.DolphinDB, false);
        this.mutex_ = new ReentrantLock();
    }

    public DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean usePython, boolean isUrgent, SqlStdEnum sqlStd){
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, usePython, isUrgent, false, sqlStd, false);
        this.mutex_ = new ReentrantLock();
    }

    public DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean usePython, boolean isUrgent, SqlStdEnum sqlStd, boolean enableSCRAM){
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, usePython, isUrgent, false, sqlStd, enableSCRAM);
        this.mutex_ = new ReentrantLock();
    }

    /**
     * This method has been deprecated since 'Java API 1.30.22.4'.
     * @param asynchronousTask
     * @param useSSL
     * @param compress
     * @param usePython
     * @param isUrgent
     * @param isReverseStreaming
     * @param sqlStd
     */
    @Deprecated
    private DBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean usePython, boolean isUrgent, boolean isReverseStreaming, SqlStdEnum sqlStd){
        this.conn_ = new DBConnectionImpl(asynchronousTask, useSSL, compress, usePython, isUrgent, isReverseStreaming, sqlStd, false);
        this.mutex_ = new ReentrantLock();
    }

    public static DBConnection internalCreateEnableReverseStreamingDBConnection(boolean asynchronousTask, boolean useSSL, boolean compress, boolean usePython, boolean isUrgent, SqlStdEnum sqlStd) {
        return new DBConnection(asynchronousTask, useSSL, compress, usePython, isUrgent, true, sqlStd);
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

    public boolean connect(String hostName, int port, int connectTimeout, int readTimeout) throws IOException {
        if (connectTimeout < 0 || readTimeout < 0) {
            log.error("The param connectTimeout or readTimeout cannot less than zero.");
            return false;
        }

        this.connectTimeout_ = connectTimeout;
        this.readTimeout_ = readTimeout;
        return connect(hostName, port, "", "", null, false, null);
    }

    public boolean connect(String hostName, int port, int timeout, boolean reconnect) throws IOException {
        this.connTimeout_ = timeout;
        return connect(hostName, port, "", "", null, false, null, reconnect);
    }

    public boolean connect(String hostName, int port, int connectTimeout, int readTimeout, boolean reconnect) throws IOException {
        if (connectTimeout < 0 || readTimeout < 0) {
            log.error("The param connectTimeout or readTimeout cannot less than zero.");
            return false;
        }

        this.connectTimeout_ = connectTimeout;
        this.readTimeout_ = readTimeout;
        return connect(hostName, port, "", "", null, false, null, reconnect);
    }

    public boolean connect(String hostName, int port, int timeout, boolean reconnect, int tryReconnectNums) throws IOException {
        this.connTimeout_ = timeout;
        return connect(hostName, port, "", "", null, false, null, reconnect, tryReconnectNums);
    }

    public boolean connect(String hostName, int port, int connectTimeout, int readTimeout, boolean reconnect, int tryReconnectNums) throws IOException {
        if (connectTimeout < 0 || readTimeout < 0) {
            log.error("The param connectTimeout or readTimeout cannot less than zero.");
            return false;
        }

        this.connectTimeout_ = connectTimeout;
        this.readTimeout_ = readTimeout;
        return connect(hostName, port, "", "", null, false, null, reconnect, tryReconnectNums);
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
        if (enableHighAvailability)
            return connect(hostName, port, userId, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect, true);
        else
            return connect(hostName, port, userId, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect, false);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript, boolean enableHighAvailability, String[] highAvailabilitySites, boolean reconnect, int tryReconnectNums) throws IOException {
        if (enableHighAvailability)
            return connect(hostName, port, userId, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect, true, tryReconnectNums);
        else
            return connect(hostName, port, userId, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect, false, tryReconnectNums);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript, boolean enableHighAvailability, String[] highAvailabilitySites, boolean reconnect, boolean enableLoadBalance) throws IOException {
        return connect(hostName, port, userId, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect, enableLoadBalance, -1);
    }

    public boolean connect(ConnectConfig connectConfig) throws IOException {
        this.connectTimeout_ = connectConfig.getConnectTimeout();
        this.readTimeout_ = connectConfig.getReadTimeout();
        return connect(connectConfig.getHostName(), connectConfig.getPort(), connectConfig.getUserId(), connectConfig.getPassword(), connectConfig.getInitialScript(), connectConfig.getEnableHighAvailability(), connectConfig.getHighAvailabilitySites(), connectConfig.getReconnect(), connectConfig.getEnableLoadBalance(), connectConfig.getTryReconnectNums());
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript, boolean enableHighAvailability, String[] highAvailabilitySites, boolean reconnect, boolean enableLoadBalance, int tryReconnectNums) throws IOException {
        mutex_.lock();
        try {
            this.uid_ = userId;
            this.pwd_ = password;
            this.initialScript_ = initialScript;
            this.enableHighAvailability_ = enableHighAvailability;
            this.loadBalance_ = enableLoadBalance;
            if (tryReconnectNums <= 0)
                this.tryReconnectNums = -1;
            else
                this.tryReconnectNums = tryReconnectNums;

            if (this.loadBalance_ && !this.enableHighAvailability_)
                throw new RuntimeException("Cannot only enable loadbalance but not enable highAvailablity.");

            if (enableHighAvailability) {
                nodes_.add(new Node(hostName, port));
                if (highAvailabilitySites != null) {
                    for (String site : highAvailabilitySites) {
                        Node node = new Node(site);
                        if (!nodes_.contains(node))
                            nodes_.add(node);
                    }
                }

                Node connectedNode = new Node();
                BasicTable bt = null;

                while (!closed_) {
                    int totalConnectAttemptNums = this.tryReconnectNums * nodes_.size();
                    int attempt = 0;
                    while (!conn_.isConnected() && !closed_) {
                        if (this.tryReconnectNums > 0) {
                            // finite try to connect.
                            for (Node one : nodes_) {
                                attempt ++;
                                // System.out.println("Current init connect node: " + one.hostName + ":" + one.port);
                                if (connectNode(one)) {
                                    connectedNode = one;
                                    break;
                                }

                                try {
                                    Thread.sleep(100);
                                } catch (Exception e){
                                    e.printStackTrace();
                                    return false;
                                }
                            }

                            if (attempt == totalConnectAttemptNums) {
                                log.error("Connect failed after " + tryReconnectNums + " reconnect attempts for every node in high availability sites.");
                                return false;
                            }
                        } else {
                            // infinite try to connect.
                            for (Node one : nodes_) {
                                if (connectNode(one)) {
                                    connectedNode = one;
                                    break;
                                }

                                try {
                                    Thread.sleep(100);
                                } catch (Exception e){
                                    e.printStackTrace();
                                    return false;
                                }
                            }
                        }
                    }

                    try {
                        BasicBoolean isClientAuth = null;
                        try {
                            isClientAuth = (BasicBoolean) conn_.run("isClientAuth", new ArrayList<>(), 0);
                        } catch (Exception e) {
                            if (e.getMessage().contains("Can't recognize function name isClientAuth"))
                                bt = (BasicTable) conn_.run("rpc(getControllerAlias(),getClusterPerf)", 0);
                            else
                                throw e;
                            break;
                        }

                        if (isClientAuth.getBoolean())
                            bt = (BasicTable) conn_.run("getClusterPerf", new ArrayList<>(), 0);
                        else
                            bt = (BasicTable) conn_.run("rpc(getControllerAlias(),getClusterPerf)", 0);
                        break;
                    } catch (Exception e) {
                        log.error("ERROR getting other data nodes, exception: " + e.getMessage());
                        Node node1 = new Node();
                        if (isConnected()) {
                            ExceptionType type = parseException(e.getMessage(), node1);
                            if (type == ExceptionType.ET_IGNORE){
                                continue;
                            } else if (type == ExceptionType.ET_NEWLEADER || type == ExceptionType.ET_NODENOTAVAIL){
                                switchDataNode(node1);
                            }
                        } else {
                            switchDataNode(node1);
                        }
                    }
                }

                if (closed_)
                    return false;

                if ( bt!=null && bt.getDataForm() != Entity.DATA_FORM.DF_TABLE)
                    throw new IOException("Run getClusterPerf() failed.");

                if (bt!=null) {
                    if (!loadBalance_) {
                        if (highAvailabilitySites == null) {
                            BasicStringVector colHost = (BasicStringVector) bt.getColumn("host");
                            BasicIntVector colPort = (BasicIntVector) bt.getColumn("port");
                            for (int i = 0; i < colHost.rows(); i++) {
                                Node curNode = new Node(colHost.getString(i), colPort.getInt(i));
                                if (!(curNode.hostName.equals(hostName) && curNode.port == port))
                                    nodes_.add(curNode);
                            }
                        }
                    } else {
                        // enable loadBalance
                        //ignore very high load nodes, rand one in low load nodes
                        List<Node> lowLoadNodes = new ArrayList<>();
                        BasicIntVector mode = (BasicIntVector) bt.getColumn("mode");
                        BasicStringVector colHost = (BasicStringVector) bt.getColumn("host");
                        BasicIntVector colPort = (BasicIntVector) bt.getColumn("port");
                        BasicLongVector memoryUsed = (BasicLongVector) bt.getColumn("memoryUsed");
                        BasicDoubleVector maxMemSize = (BasicDoubleVector) bt.getColumn("maxMemSize");
                        BasicIntVector connectionNum = (BasicIntVector) bt.getColumn("connectionNum");
                        BasicIntVector maxConnections = (BasicIntVector) bt.getColumn("maxConnections");
                        BasicDoubleVector avgLoad = (BasicDoubleVector) bt.getColumn("avgLoad");
                        for (int i = 0; i < colHost.rows(); i++) {
                            if (mode.getInt(i) != 0)
                                continue;

                            Node nodex = new Node(colHost.getString(i), colPort.getInt(i));
                            Node pexistNode = null;
                            if (highAvailabilitySites != null) {
                                for (Node node : nodes_) {
                                    if ((node.hostName.equals(nodex.hostName) || nodex.hostName.equals("localhost")) && node.port == nodex.port){
                                        pexistNode = node;
                                        break;
                                    }
                                }
                                //node is out of highAvailabilitySites
                                if (pexistNode == null)
                                    continue;
                            }
                            double memLoad = (memoryUsed.getLong(i) / 1024.0 / 1024.0 /1024.0) / maxMemSize.getDouble(i);
                            double connLoad = connectionNum.getInt(i) * 1.0 / maxConnections.getInt(i);
                            double load=(memLoad + connLoad + avgLoad.getDouble(i)) / 3.0;
                            if (pexistNode != null) {
                                pexistNode.load = load;
                            } else {
                                pexistNode=new Node(colHost.getString(i), colPort.getInt(i), load);
                                nodes_.add(pexistNode);
                            }
                            // low load
                            if (memLoad < 0.8 && connLoad < 0.9 && avgLoad.getDouble(i) < 0.8)
                                lowLoadNodes.add(pexistNode);
                        }

                        Node pMinNode;
                        if (!lowLoadNodes.isEmpty()) {
                            pMinNode=lowLoadNodes.get(nodeRandom_.nextInt(lowLoadNodes.size()));
                        } else {
                            pMinNode=nodes_.get(nodeRandom_.nextInt(nodes_.size()));
                        }

                        if (pMinNode != null && !pMinNode.equals(connectedNode)){
                            log.info("Switch to node: " + pMinNode.hostName + ":" + pMinNode.port);
                            conn_.close();
                            switchDataNode(pMinNode);
                        }
                    }
                }
            } else {
                if (reconnect) {
                    nodes_.clear();
                    nodes_.add(new Node(hostName, port));
                    switchDataNode(new Node(hostName, port));
                } else {
                    if (!connectNode(new Node(hostName, port)))
                        return false;
                }
            }

            initConnection();
            return true;
        } finally {
            mutex_.unlock();
        }
    }

    private void initConnection() throws IOException{
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
        int attempt = 0;
        boolean isConnected = false;
        do {
            attempt ++;
            if (node.hostName != null && node.hostName.length() > 0) {
                if (connectNode(node)) {
                    if (nodes_.size() > 1)
                        log.info("Switch to node: " + node.hostName + ":" + node.port + " successfully.");
                    isConnected = true;
                    break;
                }
            }

            if (nodes_.isEmpty()){
                log.error("Connect to " + node.hostName + ":" + node.port + " failed.");
                throw new RuntimeException("Connect to " + node.hostName + ":" + node.port + " failed.");
            }

            if (nodes_.size() > 1) {
                int index = nodeRandom_.nextInt(nodes_.size());
                if (connectNode(nodes_.get(index))){
                    log.info("Switch to node: " + nodes_.get(index).hostName + ":" + nodes_.get(index).port + " successfully.");
                    isConnected = true;
                    break;
                }
            }

            try {
                Thread.sleep(1000);
            } catch (Exception e){
                e.printStackTrace();
                return;
            }
        } while (!closed_ && (tryReconnectNums == -1 || attempt < tryReconnectNums));

        if (!closed_ && !isConnected) {
            log.error("Connect to " + node.hostName + ":" + node.port + " failed after " + attempt + " reconnect attempts.");
            throw new RuntimeException("Connect to " + node.hostName + ":" + node.port + " failed after " + attempt + " reconnect attempts.");
        }

        if (initialScript_!=null && initialScript_.length() > 0){
            run(initialScript_);
        }
    }

    public boolean connectNode(Node node) throws IOException {
        log.info("Connect to " + node.hostName + ":" + node.port + ".");
        while (!closed_){
            try {
                boolean connected = conn_.connect(node.hostName, node.port, uid_, pwd_, connTimeout_, connectTimeout_, readTimeout_);
                if (!connected) {
                    return false;
                }

                boolean nodeInited;
                try {
                    nodeInited = ((BasicBoolean) conn_.run("isNodeInitialized", new ArrayList<>(), 0)).getBoolean();
                } catch (Exception e) {
                    log.error("Server does not support the initialization check. Please upgrade to a newer version.");
                    nodeInited = true;
                }

                return nodeInited;
            } catch (Exception e) {
                if (isConnected()){
                    Node tmpNode = new Node();
                    tmpNode.hostName = node.hostName;
                    tmpNode.port = node.port;
                    ExceptionType type = parseException(e.getMessage(), tmpNode);
                    if (type != ExceptionType.ET_NEWLEADER){
                        if (type == ExceptionType.ET_IGNORE)
                            return true;
                        else if (type == ExceptionType.ET_NODENOTAVAIL)
                            return false;
                        else
                            throw e;
                    }
                } else {
                    if (Objects.nonNull(e.getMessage()))
                        log.error(e.getMessage());
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

    public ExceptionType parseException(String msg, Node node) {
        if(msg==null){
            node.hostName = "";
            node.port = 0;
            return ExceptionType.ET_UNKNOW;
        }
        int index = msg.indexOf("<NotLeader>");
        if (index != -1){
            index = msg.indexOf(">");
            String ipport = msg.substring(index + 1);
            if (!Pattern.matches("\\d+", ipport)) {
                log.error("The control node you are accessing is not the leader node of the highly available (raft) cluster.");
                return ExceptionType.ET_NOTLEADER;
            } else {
                parseIpPort(ipport, node);
                log.info("New leader is " + node.hostName + ":" + node.port);
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
            log.info(msg);
            return ExceptionType.ET_NODENOTAVAIL;
        }else if ((index = msg.indexOf("The datanode isn't initialized yet. Please try again later")) != -1){
            node.hostName = "";
            node.port = 0;
            return ExceptionType.ET_NOINITIALIZED;
        } else if (msg.contains("Read timed out")) {
            conn_.getNode(node);
            return ExceptionType.ET_READTIMEDOUT;
        } else if (msg.contains("Failed to read response header from the socket with IO error null")) {
            conn_.getNode(node);
            return ExceptionType.ET_NORESPONSEHEADER;
        } else if (msg.contains("Login is required for script execution with client authentication enabled")) {
            conn_.getNode(node);
            return ExceptionType.ET_LOGINREQUIRED;
        } else {
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
            conn_.run("version", new ArrayList<>(), 0);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    public void login(String userId, String password, boolean enableEncryption)throws IOException{
        conn_.login(userId, password, enableEncryption);
        uid_ = userId;
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

    public Entity run(String script, ProgressListener listener, int priority, int parallelism, int fetchSize, boolean clearSessionMemory, String tableName, boolean enableSeqNo) throws IOException{
        mutex_.lock();
        try {
            if (!nodes_.isEmpty()) {
                long currentSeqNo;
                if (enableSeqNo)
                    currentSeqNo = newSeqNo();
                else
                    currentSeqNo = 0;

                while (!closed_) {
                    try {
                        return conn_.run(script, listener, priority, parallelism, fetchSize, clearSessionMemory, tableName, currentSeqNo);
                    } catch (IOException e) {
                        if (currentSeqNo > 0)
                            currentSeqNo = -currentSeqNo;
                        Node node = new Node();
                        if (connected()) {
                            ExceptionType type = parseException(e.getMessage(), node);
                            if (type == ExceptionType.ET_IGNORE)
                                return new Void();
                            else if (type == ExceptionType.ET_UNKNOW || type == ExceptionType.ET_LOGINREQUIRED)
                                throw e;
                            else if (type == ExceptionType.ET_READTIMEDOUT)
                                cancelConsoleJob(enableSeqNo, currentSeqNo, e);
                        } else {
                            ExceptionType type = parseException(e.getMessage(), node);
                            if (type == ExceptionType.ET_READTIMEDOUT)
                                cancelConsoleJob(enableSeqNo, currentSeqNo, e);
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

    private void cancelConsoleJob(boolean enableSeqNo, long currentSeqNo, IOException e) throws IOException {
        String cancelConsoleJobScript =
                "jobs = exec rootJobId from getConsoleJobs() where sessionId = " + conn_.sessionID_ + "\n" +
                        (conn_.python_ ? "if size(jobs):\n" : "if (size(jobs))\n") +
                        "    cancelConsoleJob(jobs)\n";
        conn_.ifUrgent_ = true;
        // conn_.asynTask_ = true;

        if (enableSeqNo)
            currentSeqNo = newSeqNo();

        try {
            conn_.run(cancelConsoleJobScript, currentSeqNo);
            conn_.ifUrgent_ = false;
        } catch (IOException ioe) {
            conn_.ifUrgent_ = false;
            throw new RuntimeException("Execute cancelConsoleJob failed after current connnection read timed out. ");
        }

        log.error(e.getMessage());
        throw e;
    }

    public Entity run(String script, ProgressListener listener, int priority, int parallelism, int fetchSize, boolean clearSessionMemory, String tableName) throws IOException{
        return run(script, listener, priority, parallelism, fetchSize, clearSessionMemory, tableName, true);
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
            return run(function, arguments, priority, parallelism, fetchSize);
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

    public Entity run(String function, List<Entity> arguments, int priority, int parallelism, int fetchSize, boolean enableSeqNo) throws IOException {
        return run(function, arguments, priority, parallelism, fetchSize, enableSeqNo, null);
    }

    public Entity run(String function, List<Entity> arguments, int priority, int parallelism, int fetchSize, boolean enableSeqNo, ProgressListener listener) throws IOException {
        mutex_.lock();
        try {
            if (!nodes_.isEmpty()) {
                long currentSeqNo;
                if (enableSeqNo)
                    currentSeqNo = newSeqNo();
                else
                    currentSeqNo = 0;

                while (!closed_) {
                    try {
                        return conn_.run(function, listener, arguments, priority, parallelism, fetchSize, false, currentSeqNo);
                    } catch (IOException e) {
                        if (currentSeqNo > 0)
                            currentSeqNo = -currentSeqNo;
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
            } else {
                return conn_.run(function, listener, arguments, priority, parallelism, fetchSize, false, 0);
            }
        } finally {
            mutex_.unlock();
        }
    }

    public Entity run(String function, List<Entity> arguments, int priority, int parallelism, int fetchSize) throws IOException {
        return run(function, arguments, priority, parallelism, fetchSize, true);
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

    public String getUserId(){
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

    public ExtendedDataInput getDataInputStream()
    {
        return conn_.getDataInputStream();
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

    private boolean getServerVersion() throws IOException {
        try {
            Entity ret = conn_.run("version", new ArrayList<>(), 0);
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
