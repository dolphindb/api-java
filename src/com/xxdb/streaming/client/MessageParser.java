package com.xxdb.streaming.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MessageParser implements Runnable {
    private final int MAX_FORM_VALUE = Entity.DATA_FORM.values().length - 1;
    private final int MAX_TYPE_VALUE = Entity.DATA_TYPE.DT_OBJECT.getValue();

    private static final Logger log = LoggerFactory.getLogger(MessageParser.class);

    BufferedInputStream bis = null;
    Socket socket = null;
    DBConnectionAndSocket dBConnectionAndSocket;
    MessageDispatcher dispatcher;
    String topic;
    HashMap<String, Integer> nameToIndex = null;
    int listeningPort;

    ConcurrentHashMap<String, HashMap<String, Integer>> topicNameToIndex = null;

    public MessageParser(DBConnectionAndSocket dBConnectionAndSocket, MessageDispatcher dispatcher, int listeningPort) {
        this.dBConnectionAndSocket = dBConnectionAndSocket;
        this.dispatcher = dispatcher;
        this.topicNameToIndex = new ConcurrentHashMap<>();
        this.listeningPort = listeningPort;
    }

    private Boolean isListenMode(){
        return listeningPort > 0;
    }

    public void run() {
        Map<String, StreamDeserializer> subinfos = dispatcher.getSubInfos();
        ConcurrentHashMap<String, AbstractClient.Site[]> topicToSites = dispatcher.getTopicToSites();
        Socket socket = null;
        try {
            DBConnection conn;
            ExtendedDataInput in = null;
            Boolean isReverseStreaming;

            if (this.dBConnectionAndSocket == null ) {
                throw new Exception("dBConnectionAndSocket is null!");
            } else {
                if (this.dBConnectionAndSocket.socket != null) {
                    if (this.dBConnectionAndSocket.conn != null)
                        throw new Exception("Either conn or socket must be null!");
                    socket = this.dBConnectionAndSocket.socket;
                    bis = new BufferedInputStream(socket.getInputStream());
                    isReverseStreaming = false;
                } else if (this.dBConnectionAndSocket.conn != null) {
                    conn = this.dBConnectionAndSocket.conn;
                    in = conn.getDataInputStream();
                    socket = conn.getSocket();
                    isReverseStreaming = true;
                } else {
                    throw new Exception("Both conn and socket is null!");
                }
            }


            while (!dispatcher.isClose()) {
                if (!isReverseStreaming) {
                    Boolean isLittle = bis.read() != 0;
                    if (isLittle == true)
                        in = new LittleEndianDataInputStream(bis);
                    else
                        in = new BigEndianDataInputStream(bis);
                } else {
                    in.readBoolean();
                }

                in.readLong();
                long msgid = in.readLong();

                topic = in.readString();

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
                Entity.DATA_TYPE dt = Entity.DATA_TYPE.valueOf(type);
                Entity body;

                body = BasicEntityFactory.instance().createEntity(df, dt, in, extended);
                if (body.isTable() && body.rows() == 0) {
                    for (String t : topic.split(",")) {
                        dispatcher.setNeedReconnect(t, 0);
                    }
                    assert (body.rows() == 0);
                    nameToIndex = new HashMap<>();
                    BasicTable schema = (BasicTable) body;
                    int columns = schema.columns();
                    for (int i = 0; i < columns; i++) {
                        String name = schema.getColumnName(i);
                        nameToIndex.put(name.toLowerCase(), i);
                    }
                    topicNameToIndex.put(topic, nameToIndex);
                }
                else if (body.isVector()) {
                    BasicAnyVector dTable = (BasicAnyVector) body;

                    AbstractClient.Site[] sites = topicToSites.get(topic);
                    int colSize = dTable.rows();
                    int rowSize = dTable.getEntity(0).rows();
                    if (sites != null && sites[0].msgAstable == true) {
                        BasicMessage rec = new BasicMessage(msgid - rowSize + 1, topic, dTable, topicNameToIndex.get(topic.split(",")[0]));
                        dispatcher.dispatch(rec);
                    } else {
                        if (rowSize >= 1) {
                            if (isListenMode() && rowSize == 1) {
                                BasicMessage rec = new BasicMessage(msgid, topic, dTable, topicNameToIndex.get(topic.split(",")[0]));
                                if (subinfos.get(topic) != null)
                                    rec = subinfos.get(topic).parse(rec);
                                dispatcher.dispatch(rec);
                            } else {
                                List<IMessage> messages = new ArrayList<>(rowSize);
                                long startMsgId = msgid - rowSize + 1;
                                for (int i = 0; i < rowSize; i++) {
                                    BasicAnyVector row = new BasicAnyVector(colSize);
                                    for (int j = 0; j < colSize; j++) {
                                        AbstractVector vector = (AbstractVector) dTable.getEntity(j);
                                        Entity entity = vector.get(i);
                                        row.setEntity(j, entity);
                                    }
                                    BasicMessage rec = new BasicMessage(startMsgId + i, topic, row, topicNameToIndex.get(topic.split(",")[0]));
                                    if (subinfos.get(topic) != null)
                                        rec = subinfos.get(topic).parse(rec);
                                    messages.add(rec);
                                }
                                dispatcher.batchDispatch(messages);
                            }
                        }
                    }
                    dispatcher.setMsgId(topic, msgid);
                } else {
                        log.error("message body has an invalid format. Vector or table is expected");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (dispatcher.isClosed(topic)) {
                log.error("check " + topic + " is unsubscribed");
                return;
            } else {
                dispatcher.setNeedReconnect(topic, 1);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            dispatcher.setNeedReconnect(topic, 1);
        } finally {
            try {
                if (socket != null) {
                    socket.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class DBConnectionAndSocket {
        public DBConnection conn;
        public Socket socket;
    }

}