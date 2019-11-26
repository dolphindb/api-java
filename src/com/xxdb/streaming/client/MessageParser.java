package com.xxdb.streaming.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.xxdb.FileLog;
import com.xxdb.data.*;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;

class MessageParser implements Runnable {
    private final int MAX_FORM_VALUE = Entity.DATA_FORM.values().length - 1;
    private final int MAX_TYPE_VALUE = Entity.DATA_TYPE.values().length - 1;

    BufferedInputStream bis = null;
    Socket socket = null;
    MessageDispatcher dispatcher;
    String topic;
    HashMap<String, Integer> nameToIndex = null;

    ConcurrentHashMap<String,HashMap<String, Integer>> topicNameToIndex = null;

    public MessageParser(Socket socket, MessageDispatcher dispatcher) {
        this.socket = socket;
        this.dispatcher = dispatcher;
        this.topicNameToIndex = new ConcurrentHashMap<>();
    }

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public void run() {
        Socket socket = this.socket;
        try {
            if (bis == null) bis = new BufferedInputStream(socket.getInputStream());
            ExtendedDataInput in = null;

            while (true) {
                    if (in == null) {
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
                    EntityFactory factory = new BasicEntityFactory();
                    int form = flag >> 8;

                    int type = flag & 0xff;

                    if (form < 0 || form > MAX_FORM_VALUE)
                        throw new IOException("Invalid form value: " + form);
                    if (type < 0 || type > MAX_TYPE_VALUE) {
                        throw new IOException("Invalid type value: " + type);

                    }

                    Entity.DATA_FORM df = Entity.DATA_FORM.values()[form];
                    Entity.DATA_TYPE dt = Entity.DATA_TYPE.values()[type];
                    Entity body;

                    body = factory.createEntity(df, dt, in);
                    if (body.isTable() && body.rows()==0) {
                        for(String t : topic.split(",")){
                            System.out.println("set ");
                            dispatcher.setNeedReconnect(t,0);
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
                    } else if (body.isVector()) {
                        BasicAnyVector dTable = (BasicAnyVector) body;

                        int colSize = dTable.rows();
                        int rowSize = dTable.getEntity(0).rows();
                        if (rowSize >= 1) {
                            if (rowSize == 1) {
                                BasicMessage rec = new BasicMessage(msgid, topic, dTable, topicNameToIndex.get(topic.split(",")[0]));
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
                                    messages.add(rec);
                                }
                                dispatcher.batchDispatch(messages);
                            }
                        }
                        dispatcher.setMsgId(topic, msgid);
                    } else {
                        System.out.println("message body has an invalid format. Vector or table is expected");
                    }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            if (dispatcher.isClosed(topic)) {
                System.out.println("check " + topic + " is unsubscribed");
                return;
            } else {
//                List<String> allTopicOfThisSite = dispatcher.getAllTopicsOnSite(topic);
//                for(String tp : allTopicOfThisSite){
//                    System.out.println("set " + tp + " reconnect state = 1");
                    dispatcher.setNeedReconnect(topic, 1);
//                }

            }
        } catch (Throwable t) {
            t.printStackTrace();
//            List<String> allTopicOfThisSite = dispatcher.getAllTopicsOnSite(topic);
//            for(String tp : allTopicOfThisSite){
//                System.out.println("set " + tp + " reconnect state = 1");
                dispatcher.setNeedReconnect(topic, 1);
//            }
         }
    }
}