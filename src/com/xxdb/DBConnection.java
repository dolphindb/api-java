package com.xxdb;

import java.io.*;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import com.xxdb.data.*;
import com.xxdb.data.Void;
import com.xxdb.io.AbstractExtendedDataOutputStream;
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
    private enum NotLeaderStatus {
        NEW_LEADER, WAIT, CONN_FAIL, OTHER_EXCEPTION
    }

    private static final int MAX_FORM_VALUE = Entity.DATA_FORM.values().length - 1;
    private static final int MAX_TYPE_VALUE = Entity.DATA_TYPE.values().length - 1;
    private static final int DEFAULT_PRIORITY = 4;
    private static final int DEFAULT_PARALLELISM = 2;

    private String ServerVersion = "";
    private ReentrantLock mutex;
    private String sessionID;
    private Socket socket;
    private boolean remoteLittleEndian;
    private ExtendedDataOutput out;
    private ExtendedDataInput in;
    private EntityFactory factory;
    private String hostName;
    private int port;
    private String mainHostName;
    private int mainPort;
    private String userId;
    private String password;
    private String initialScript = null;
    private boolean encrypted;
    private String controllerHost = null;
    private int controllerPort;
    private boolean highAvailability;
    private String[] highAvailabilitySites = null;
    private boolean HAReconnect = false;
    private int connTimeout = 0;
    private boolean asynTask = false;
    private boolean isUseSSL = false;
    public DBConnection() {
        factory = new BasicEntityFactory();
        mutex = new ReentrantLock();
        sessionID = "";
    }

    public DBConnection(boolean asynchronousTask) {
        factory = new BasicEntityFactory();
        mutex = new ReentrantLock();
        sessionID = "";
        asynTask = asynchronousTask;
    }
    public DBConnection(boolean asynchronousTask, boolean useSSL) {
        factory = new BasicEntityFactory();
        mutex = new ReentrantLock();
        sessionID = "";
        asynTask = asynchronousTask;
        isUseSSL = useSSL;
    }
    public boolean isBusy() {
        if (!mutex.tryLock())
            return true;
        else {
            mutex.unlock();
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

    public boolean connect(String hostName, int port) throws IOException {
        return connect(hostName, port, "", "", null, false, null);
    }

    public boolean connect(String hostName, int port, int timeout) throws IOException {
        this.connTimeout = timeout;
        return connect(hostName, port, "", "", null, false, null);
    }

    public boolean connect(String hostName, int port, String initialScript) throws IOException {
        return connect(hostName, port, "", "", initialScript, false, null);
    }

    public boolean connect(String hostName, int port, String initialScript, boolean highAvailability) throws IOException {
        return connect(hostName, port, "", "", initialScript, highAvailability, null);
    }

    public boolean connect(String hostName, int port, boolean highAvailability) throws IOException {
        return connect(hostName, port, "", "", null, highAvailability, null);
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

    public boolean connect(String hostName, int port, String userId, String password, boolean highAvailability) throws IOException {
        return connect(hostName, port, userId, password, null, highAvailability, null);
    }

    public boolean connect(String hostName, int port, String userId, String password, String[] highAvailabilitySites) throws IOException {
        return connect(hostName, port, userId, password, null, true, highAvailabilitySites);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript) throws IOException {
        return connect(hostName, port, userId, password, initialScript, false, null);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript, boolean highAvailability) throws IOException {
        return connect(hostName, port, userId, password, initialScript, highAvailability, null);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript, String[] highAvailabilitySites) throws IOException {
        return connect(hostName, port, userId, password, initialScript, true, highAvailabilitySites);
    }

    public boolean connect(String hostName, int port, String userId, String password, String initialScript, boolean highAvailability, String[] highAvailabilitySites) throws IOException {
        mutex.lock();
        try {
            if (!sessionID.isEmpty()) {
                mutex.unlock();
                return true;
            }

            this.hostName = hostName;
            this.mainHostName = hostName;
            this.port = port;
            this.mainPort = port;
            this.userId = userId;
            this.password = password;
            this.encrypted = true;
            this.initialScript = initialScript;
            this.highAvailability = highAvailability;
            this.highAvailabilitySites = highAvailabilitySites;
            if (highAvailabilitySites != null) {
                for (String site : highAvailabilitySites) {
                    String HASite[] = site.split(":");
                    if (HASite.length != 2)
                        throw new IllegalArgumentException("The site '" + site + "' is invalid.");
                }
            }
            assert (highAvailabilitySites == null || highAvailability);

            return connect();
        } finally {
            mutex.unlock();
        }
    }

    private boolean connect() throws IOException {
        try {
            if(isUseSSL)
                socket = getSSLSocketFactory().createSocket(hostName,port);
            else
                socket = new Socket(hostName, port);
        } catch (ConnectException ex) {
            if (HAReconnect)
                return false;
            if (switchToRandomAvailableSite())
                return true;
            throw ex;
        }
        if (this.connTimeout > 0) {
            socket.setSoTimeout(this.connTimeout);
        }
        socket.setKeepAlive(true);
        socket.setTcpNoDelay(true);
        out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        @SuppressWarnings("resource")
        ExtendedDataInput input = new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
        String body = "connect\n";
        out.writeBytes("API 0 ");
        out.writeBytes(String.valueOf(body.length()));
        if(asynTask){
            out.writeBytes(" / 4_1_" + String.valueOf(4) + "_" + String.valueOf(2));
        }else{
            out.writeBytes(" / 0_1_" + String.valueOf(4) + "_" + String.valueOf(2));
        }
        out.writeByte('\n');
        out.writeBytes(body);
        out.flush();

        String line = input.readLine();
        int endPos = line.indexOf(' ');
        if (endPos <= 0) {
            close();
            if (switchToRandomAvailableSite())
                return true;
            return false;
        }
        sessionID = line.substring(0, endPos);

        int startPos = endPos + 1;
        endPos = line.indexOf(' ', startPos);
        if (endPos != line.length() - 2) {
            close();
            if (switchToRandomAvailableSite())
                return true;
            return false;
        }

        if (line.charAt(endPos + 1) == '0') {
            remoteLittleEndian = false;
            out = new BigEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        } else
            remoteLittleEndian = true;
        in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
                new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));

        if (!userId.isEmpty() && !password.isEmpty()) {
            if (asynTask) {
                login(userId, password, false);
            } else {
                login();
            }
        }

        if (initialScript != null && initialScript.length() > 0)
            run(initialScript);

        if (highAvailability && highAvailabilitySites == null) {
            try {
                controllerHost = ((BasicString) run("rpc(getControllerAlias(), getNodeHost)")).getString();
                controllerPort = ((BasicInt) run("rpc(getControllerAlias(), getNodePort)")).getInt();
            } catch (Exception e) {
            }
        }

        return true;
    }

    public void login(String userId, String password, boolean enableEncryption) throws IOException {
        mutex.lock();
        try {
            this.userId = userId;
            this.password = password;
            this.encrypted = enableEncryption;

            login();
        } finally {
            mutex.unlock();
        }
    }

    private void login() throws IOException {
        List<Entity> args = new ArrayList<>();
        if (encrypted) {
            BasicString keyCode = (BasicString) run("getDynamicPublicKey", new ArrayList<Entity>());
            PublicKey key = RSAUtils.getPublicKey(keyCode.getString());
            byte[] usr = RSAUtils.encryptByPublicKey(userId.getBytes(), key);
            byte[] pass = RSAUtils.encryptByPublicKey(password.getBytes(), key);


            args.add(new BasicString(Base64.getMimeEncoder().encodeToString(usr)));
            args.add(new BasicString(Base64.getMimeEncoder().encodeToString(pass)));
            args.add(new BasicBoolean(true));
        } else {
            args.add(new BasicString(userId));
            args.add(new BasicString(password));
        }
        run("login", args);
    }

    public boolean getRemoteLittleEndian() {
        return this.remoteLittleEndian;
    }

    private boolean switchToRandomAvailableSite() throws IOException {
        if (!highAvailability)
            return false;

        int tryCount = 0;
        while (true) {
            HAReconnect = true;
            if (highAvailabilitySites != null) {
                if (tryCount < 3) {
                    hostName = mainHostName;
                    port = mainPort;
                } else {
                    int rnd = new Random().nextInt(highAvailabilitySites.length);
                    String site[] = highAvailabilitySites[rnd].split(":");
                    hostName = site[0];
                    port = new Integer(site[1]);
                }
                tryCount++;
            } else {
                if (controllerHost == null)
                    return false;
                DBConnection tmp = new DBConnection();
                tmp.connect(controllerHost, controllerPort);
                BasicStringVector availableSites = (BasicStringVector) tmp.run("getClusterLiveDataNodes(false)");
                tmp.close();
                int size = availableSites.rows();
                if (size <= 0)
                    return false;
                String site[] = availableSites.getString(0).split(":");
                hostName = site[0];
                port = new Integer(site[1]);
            }
            try {
                System.out.println("Trying to reconnect to " + hostName + ":" + port);
                if (connect()) {
                    HAReconnect = false;
                    System.out.println("Successfully reconnected to " + hostName + ":" + port);
                    return true;
                }
            } catch (Exception e) {
            }
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
            }
        }
    }

    private NotLeaderStatus handleNotLeaderException(Exception ex, String function) {
        String errMsg = ex.getMessage();
        if (ServerExceptionUtils.isNotLeader(errMsg)) {
            String newLeaderString = ServerExceptionUtils.newLeader(errMsg);
            String[] newLeader = newLeaderString.split(":");
            String newHostName = newLeader[0];
            int newPort = new Integer(newLeader[1]);
            if (hostName.equals(newHostName) && port == newPort) {
                System.out.println("Got NotLeader exception. Waiting for new leader.");
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
                return NotLeaderStatus.WAIT;
            }
            hostName = newHostName;
            port = newPort;
            try {
                System.out.println("Got NotLeader exception. Switching to " + hostName + ":" + port);
                if (connect())
                    return NotLeaderStatus.NEW_LEADER;
                else
                    return NotLeaderStatus.CONN_FAIL;
            } catch (IOException e) {
                return NotLeaderStatus.CONN_FAIL;
            }
        }
        return NotLeaderStatus.OTHER_EXCEPTION;
    }

    public Entity tryRun(String script) throws IOException {
        return tryRun(script, DEFAULT_PRIORITY, DEFAULT_PARALLELISM);
    }

    public Entity tryRun(String script, int priority, int parallelism) throws IOException {
        return tryRun(script, priority, parallelism, 0);
    }
    public Entity tryRun(String script, int priority, int parallelism,int fetchSize) throws IOException {
        if (!mutex.tryLock())
            return null;
        try {
            return run(script, (ProgressListener) null, priority, parallelism, fetchSize, false);
        } finally {
            mutex.unlock();
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

    public Entity run(String script, ProgressListener listener, int priority, int parallelism) throws IOException {
        return run( script, listener, priority, parallelism, 0);
    }

    public Entity run(String script, ProgressListener listener, int priority, int parallelism, int fetchSize) throws IOException {
        return run( script, listener, priority, parallelism, 0, false);
    }

    public Entity tryRun(String script, boolean clearSessionMemory) throws IOException {
        if (!mutex.tryLock())
            return null;
        try {
            return run(script, (ProgressListener) null, DEFAULT_PRIORITY, DEFAULT_PARALLELISM, 0, false);
        } finally {
            mutex.unlock();
        }
    }

    public Entity run(String script, boolean clearSessionMemory) throws IOException {
        return run(script, (ProgressListener) null, DEFAULT_PRIORITY, DEFAULT_PARALLELISM,0, clearSessionMemory);
    }

    public Entity run(String script, ProgressListener listener, int priority, int parallelism, int fetchSize, boolean clearSessionMemory) throws IOException {
        if(fetchSize>0){
            if(fetchSize<8192){
                throw new IOException("fetchSize must be greater than 8192");
            }
        }
        mutex.lock();
        boolean isUrgentCancelJob = false;
        if (script.startsWith("cancelJob(") || script.startsWith("cancelConsoleJob("))
            isUrgentCancelJob = true;

        try {
            boolean reconnect = false;
            InputStream is = null;
            if (socket == null || !socket.isConnected() || socket.isClosed()) {
                if (sessionID.isEmpty())
                    throw new IOException("Database connection is not established yet.");
                else {
                    socket = new Socket(hostName, port);
                    socket.setKeepAlive(true);
                    socket.setTcpNoDelay(true);
                    out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                    is = socket.getInputStream();
                    BufferedInputStream bis = new BufferedInputStream(is);
                    in = remoteLittleEndian ? new LittleEndianDataInputStream(bis) :
                            new BigEndianDataInputStream(new BufferedInputStream(bis));
                }
            }
            String body = "script\n" + script;
            String header = null;
            try {

                out.writeBytes((listener != null ? "API2 " : "API ") + sessionID + " ");
                out.writeBytes(String.valueOf(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0)));
                if(isUrgentCancelJob){
                    out.writeBytes(" / 1_1_8_8");
                }
                else{
                    int flag = 0;
                    if (asynTask)
                        flag += 4;
                    if (clearSessionMemory)
                        flag += 16;
                    if (priority != DEFAULT_PRIORITY || parallelism != DEFAULT_PARALLELISM) {
                        out.writeBytes(" / " + String.valueOf(flag) + "_1_" + String.valueOf(priority) + "_" + String.valueOf(parallelism));
                    } else {
                        out.writeBytes(" / " + String.valueOf(flag) + "_1_" + String.valueOf(DEFAULT_PRIORITY) + "_" + String.valueOf(DEFAULT_PARALLELISM));
                    }
                }
                if(fetchSize>0){
                    out.writeBytes("__" + String.valueOf(fetchSize));
                }
                out.writeByte('\n');
                out.writeBytes(body);
                out.flush();
                if(asynTask) return null;
                header = in.readLine();
            } catch (IOException ex) {
                if (reconnect) {
                    socket = null;
                    throw ex;
                }

                NotLeaderStatus status = handleNotLeaderException(ex, null);
                if (status == NotLeaderStatus.NEW_LEADER)
                    return run(script, listener, priority, parallelism);
                else if (status == NotLeaderStatus.WAIT) {
                    if (!HAReconnect) {
                        HAReconnect = true;
                        while (true) {
                            try {
                                Entity re = run(script, listener, priority, parallelism);
                                HAReconnect = false;
                                return re;
                            } catch (Exception e) {
                            }
                        }
                    }
                    throw ex;
                }

                try {
                    connect();
                    out.writeBytes((listener != null ? "API2 " : "API ") + sessionID + " ");
                    out.writeBytes(String.valueOf(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0)));
                    if(isUrgentCancelJob){
                        out.writeBytes(" / 1_1_8_8");
                    }
                    else{
                        int flag = 0;
                        if (asynTask)
                            flag += 4;
                        if (clearSessionMemory)
                            flag += 16;
                        if (priority != DEFAULT_PRIORITY || parallelism != DEFAULT_PARALLELISM) {
                            out.writeBytes(" / " + String.valueOf(flag) + "_1_" + String.valueOf(priority) + "_" + String.valueOf(parallelism));
                        } else {
                            out.writeBytes(" / " + String.valueOf(flag) + "_1_" + String.valueOf(DEFAULT_PRIORITY) + "_" + String.valueOf(DEFAULT_PARALLELISM));
                        }
                    }
                    if(fetchSize>0){
                        out.writeBytes("__" + String.valueOf(fetchSize));
                    }
                    out.writeByte('\n');
                    out.writeBytes(body);
                    out.flush();
                    header = in.readLine();
                    reconnect = true;
                } catch (Exception e) {
                    socket = null;
                    throw e;
                }
            }

            while (header.equals("MSG")) {
                //read intermediate message to indicate the progress
                String msg = in.readString();
                if (listener != null)
                    listener.progress(msg);
                header = in.readLine();
            }

            String[] headers = header.split(" ");
            if (headers.length != 3) {
                socket = null;
                throw new IOException("Received invalid header: " + header);
            }
            if (reconnect)
                sessionID = headers[0];
            int numObject = Integer.parseInt(headers[1]);

            String msg = in.readLine();
            if (!msg.equals("OK")) {
                if (reconnect && ServerExceptionUtils.isNotLogin(msg)) {
                    if (userId.length() > 0 && password.length() > 0)
                        login();
                } else {
                    throw new IOException(msg);
                }
            }

            if (numObject == 0)
                return new Void();
            try {
                short flag = in.readShort();
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
                Entity.DATA_TYPE dt = Entity.DATA_TYPE.values()[type];
                if(fetchSize>0 && df == Entity.DATA_FORM.DF_VECTOR && dt == Entity.DATA_TYPE.DT_ANY){
                    return new EntityBlockReader(in);
                }
                return factory.createEntity(df, dt, in, extended);
            } catch (IOException ex) {
                socket = null;
                throw ex;
            }
        } catch (Exception ex) {
            NotLeaderStatus status = handleNotLeaderException(ex, null);
            if (status == NotLeaderStatus.NEW_LEADER)
                return run(script, listener, priority, parallelism);
            else if (status == NotLeaderStatus.WAIT) {
                if (!HAReconnect) {
                    HAReconnect = true;
                    while (true) {
                        try {
                            Entity re = run(script, listener, priority, parallelism);
                            HAReconnect = false;
                            return re;
                        } catch (Exception e) {
                        }
                    }
                }
                throw ex;
            }
            if (socket != null || !highAvailability)
                throw ex;
            if (switchToRandomAvailableSite())
                return run(script, listener, priority, parallelism);
            else
                throw ex;
        } finally {
            mutex.unlock();
        }
    }

    public Entity tryRun(String function, List<Entity> arguments) throws IOException {
        return tryRun(function, arguments, DEFAULT_PRIORITY, DEFAULT_PARALLELISM);
    }

    public Entity tryRun(String function, List<Entity> arguments, int priority, int parallelism) throws IOException {
            return tryRun(function, arguments, priority, parallelism,0);
    }
    public Entity tryRun(String function, List<Entity> arguments, int priority, int parallelism, int fetchSize) throws IOException {
        if (!mutex.tryLock())
            return null;
        try {
            return run(function, arguments, priority, parallelism, fetchSize);
        } finally {
            mutex.unlock();
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
        if(fetchSize>0){
            if(fetchSize<8192){
                throw new IOException("fetchSize must be greater than 8192");
            }
        }
        mutex.lock();
        try {
            boolean reconnect = false;

            if (socket == null || !socket.isConnected() || socket.isClosed()) {
                if (sessionID.isEmpty())
                    throw new IOException("Database connection is not established yet.");
                else {
                    socket = new Socket(hostName, port);
                    socket.setKeepAlive(true);
                    socket.setTcpNoDelay(true);
                    out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                    in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
                            new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
                }
            }

            String body = "function\n" + function;
            body += ("\n" + arguments.size() + "\n");
            body += remoteLittleEndian ? "1" : "0";

            String[] headers = null;
            try {
                out.writeBytes("API " + sessionID + " ");
                out.writeBytes(String.valueOf(body.length()));
                if (priority != DEFAULT_PRIORITY || parallelism != DEFAULT_PARALLELISM) {
                    if(asynTask) {
                        out.writeBytes(" / 4_1_" + String.valueOf(priority) + "_" + String.valueOf(parallelism));
                    }else{
                        out.writeBytes(" / 0_1_" + String.valueOf(priority) + "_" + String.valueOf(parallelism));
                    }
                }else if(asynTask){
                    out.writeBytes(" / 4_1_" + String.valueOf(DEFAULT_PRIORITY) + "_" + String.valueOf(DEFAULT_PARALLELISM));
                }
                if(fetchSize>0){
                    out.writeBytes("__" + String.valueOf(fetchSize));
                }
                out.writeByte('\n');
                out.writeBytes(body);
                for (int i = 0; i < arguments.size(); ++i)
                    arguments.get(i).write(out);
                out.flush();

                if (asynTask) return null;

                headers = in.readLine().split(" ");
            } catch (IOException ex) {
                if (reconnect) {
                    socket = null;
                    throw ex;
                }

                NotLeaderStatus status = handleNotLeaderException(ex, null);
                if (status == NotLeaderStatus.NEW_LEADER)
                    return run(function, arguments, priority, parallelism);
                else if (status == NotLeaderStatus.WAIT) {
                    if (!HAReconnect) {
                        HAReconnect = true;
                        while (true) {
                            try {
                                Entity re = run(function, arguments, priority, parallelism);
                                HAReconnect = false;
                                return re;
                            } catch (Exception e) {
                            }
                        }
                    }
                    throw ex;
                }

                try {
                    connect();
                    out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                    out.writeBytes("API " + sessionID + " ");
                    out.writeBytes(String.valueOf(body.length()));
                    if (priority != DEFAULT_PRIORITY || parallelism != DEFAULT_PARALLELISM) {
                        if(asynTask) {
                            out.writeBytes(" / 4_1_" + String.valueOf(priority) + "_" + String.valueOf(parallelism));
                        }else{
                            out.writeBytes(" / 0_1_" + String.valueOf(priority) + "_" + String.valueOf(parallelism));
                        }
                    }else if(asynTask){
                        out.writeBytes(" / 4_1_" + String.valueOf(DEFAULT_PRIORITY) + "_" + String.valueOf(DEFAULT_PARALLELISM));
                    }
                    out.writeByte('\n');
                    out.writeBytes(body);
                    for (int i = 0; i < arguments.size(); ++i)
                        arguments.get(i).write(out);
                    out.flush();

                    if (asynTask) return null;

                    in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
                            new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
                    headers = in.readLine().split(" ");

                    reconnect = true;
                } catch (Exception e) {
                    socket = null;
                    throw e;
                }
            }

            if (headers.length != 3) {
                socket = null;
                throw new IOException("Received invalid header.");
            }

            if (reconnect)
                sessionID = headers[0];
            int numObject = Integer.parseInt(headers[1]);

            String msg = in.readLine();
            if (!msg.equals("OK"))
                throw new IOException(msg);

            if (numObject == 0)
                return new Void();

            try {
                short flag = in.readShort();
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
                Entity.DATA_TYPE dt = Entity.DATA_TYPE.values()[type];
                if(fetchSize>0 && df == Entity.DATA_FORM.DF_VECTOR && dt == Entity.DATA_TYPE.DT_ANY){
                    return new EntityBlockReader(in);
                }
                return factory.createEntity(df, dt, in, extended);
            } catch (IOException ex) {
                socket = null;
                throw ex;
            }
        } catch (Exception ex) {
            NotLeaderStatus status = handleNotLeaderException(ex, null);
            if (status == NotLeaderStatus.NEW_LEADER)
                return run(function, arguments, priority, parallelism);
            else if (status == NotLeaderStatus.WAIT) {
                if (!HAReconnect) {
                    HAReconnect = true;
                    while (true) {
                        try {
                            Entity re = run(function, arguments, priority, parallelism);
                            HAReconnect = false;
                            return re;
                        } catch (Exception e) {
                        }
                    }
                }
                throw ex;
            }

            if (socket != null || !highAvailability)
                throw ex;
            if (switchToRandomAvailableSite())
                return run(function, arguments, priority, parallelism);
            else
                throw ex;
        } finally {
            mutex.unlock();
        }
    }

    public void tryUpload(final Map<String, Entity> variableObjectMap) throws IOException {
        if (!mutex.tryLock())
            throw new IOException("The connection is in use.");
        try {
            upload(variableObjectMap);
        } finally {
            mutex.unlock();
        }
    }

    public void upload(final Map<String, Entity> variableObjectMap) throws IOException {
        if(asynTask) throw new IOException("Asynchronous upload is not allowed");
        if (variableObjectMap == null || variableObjectMap.isEmpty())
            return;

        mutex.lock();
        try {
            boolean reconnect = false;
            if (socket == null || !socket.isConnected() || socket.isClosed()) {
                if (sessionID.isEmpty())
                    throw new IOException("Database connection is not established yet.");
                else {
                    reconnect = true;
                    socket = new Socket(hostName, port);
                    socket.setKeepAlive(true);
                    socket.setTcpNoDelay(true);
                    out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                    in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(socket.getInputStream())) :
                            new BigEndianDataInputStream(new BufferedInputStream(socket.getInputStream()));
                }
            }

            List<Entity> objects = new ArrayList<Entity>();

            String body = "variable\n";
            for (String key : variableObjectMap.keySet()) {
                if (!isVariableCandidate(key))
                    throw new IllegalArgumentException("'" + key + "' is not a good variable name.");
                body += key + ",";
                objects.add(variableObjectMap.get(key));
            }
            body = body.substring(0, body.length() - 1);
            body += ("\n" + objects.size() + "\n");
            body += remoteLittleEndian ? "1" : "0";

            try {
                out.writeBytes("API " + sessionID + " ");
                out.writeBytes(String.valueOf(body.length()));
                out.writeByte('\n');
                out.writeBytes(body);
                for (int i = 0; i < objects.size(); ++i)
                    objects.get(i).write(out);
                out.flush();
            } catch (IOException ex) {
                if (reconnect) {
                    socket = null;
                    throw ex;
                }

                try {
                    socket = new Socket(hostName, port);
                    socket.setKeepAlive(true);
                    socket.setTcpNoDelay(true);
                    out = new LittleEndianDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                    out.writeBytes("API " + sessionID + " ");
                    out.writeBytes(String.valueOf(body.length()));
                    out.writeByte('\n');
                    out.writeBytes(body);
                    for (int i = 0; i < objects.size(); ++i)
                        objects.get(i).write(out);
                    out.flush();
                    reconnect = true;
                } catch (Exception e) {
                    socket = null;
                    throw e;
                }
            }

            String[] headers = in.readLine().split(" ");
            if (headers.length != 3) {
                socket = null;
                throw new IOException("Received invalid header.");
            }

            if (reconnect)
                sessionID = headers[0];

            int numObject = Integer.parseInt(headers[1]);

            String msg = in.readLine();
            if (!msg.equals("OK"))
                throw new IOException(msg);

            if (numObject > 0) {
                try {
                    short flag = in.readShort();
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
                    Entity.DATA_TYPE dt = Entity.DATA_TYPE.values()[type];
                    Entity re =  factory.createEntity(df, dt, in, extended);
                } catch (IOException ex) {
                    socket = null;
                    throw ex;
                }
            }
        } catch (Exception ex) {
            if (socket != null || !highAvailability)
                throw ex;
            if (switchToRandomAvailableSite()) {
                mutex.unlock();
                upload(variableObjectMap);
            } else
                throw ex;
        } finally {
            mutex.unlock();
        }
    }

    public void close() {
        mutex.lock();
        try {
            if (socket != null) {
                socket.close();
                sessionID = "";
                socket = null;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            mutex.unlock();
        }
    }

    private boolean isVariableCandidate(String word) {
        char cur = word.charAt(0);
        if ((cur < 'a' || cur > 'z') && (cur < 'A' || cur > 'Z'))
            return false;
        for (int i = 1; i < word.length(); i++) {
            cur = word.charAt(i);
            if ((cur < 'a' || cur > 'z') && (cur < 'A' || cur > 'Z') && (cur < '0' || cur > '9') && cur != '_')
                return false;
        }
        return true;
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    public String getSessionID() {
        return sessionID;
    }

    public InetAddress getLocalAddress() {
        return socket.getLocalAddress();
    }

    public boolean isConnected() {
        return socket != null && socket.isConnected();
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
}
