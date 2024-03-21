package com.xxdb.streaming.cep;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.io.*;
import com.xxdb.streaming.client.BasicMessage;
import com.xxdb.streaming.client.IMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

public class EventHandler {

    private Map<String, EventInfo> eventInfos;
    private boolean isNeedEventTime;
    private int outputColNums;
    private int commonKeySize;

    public EventHandler(List<EventScheme> eventSchemes, List<String> eventTimeKeys, List<String> commonKeys) {
        this.isNeedEventTime = false;
        this.outputColNums = 0;
        this.commonKeySize = 0;
        this.eventInfos = new HashMap<>();

        String funcName = "createEventSender";
        // check eventSchemes
        if (eventSchemes.isEmpty()) {
            throw new IllegalArgumentException(funcName + " eventSchemes must not be empty");
        }
        List<EventScheme> expandEventSchemes = new ArrayList<>(eventSchemes);
        for (EventScheme event : expandEventSchemes) {
            if (Utils.isEmpty(event.getEventType()))
                throw new IllegalArgumentException(funcName + " the eventType cannot be empty");

            int length = event.getAttrKeys().size();
            if (event.getAttrExtraParams().isEmpty()) {
                event.setAttrExtraParams(Collections.nCopies(length, 0));
            }
            if (length == 0) {
                throw new IllegalArgumentException(funcName + " the eventKey in eventScheme must not be empty");
            }
            if ((!event.getAttrExtraParams().isEmpty() && length != event.getAttrExtraParams().size()) || length != event.getAttrForms().size() || length != event.getAttrTypes().size()) {
                throw new IllegalArgumentException(funcName + " the number of eventKey, eventTypes, eventForms and eventExtraParams (if set) must have the same length.");
            }
        }
        int eventNum = eventSchemes.size();

        // check eventTimeKeys
        List<String> expandTimeKeys = new ArrayList<>();
        if (!eventTimeKeys.isEmpty()) {
            // if eventTimeKeys only contain one element, it means every event has this key
            if (eventTimeKeys.size() == 1) {
                expandTimeKeys = Collections.nCopies(eventNum, eventTimeKeys.get(0));
            } else {
                if (eventTimeKeys.size() != eventNum) {
                    throw new IllegalArgumentException(funcName + "the number of eventTimeKey is inconsistent with the number of events in eventSchemes.");
                }
                expandTimeKeys = new ArrayList<>(eventTimeKeys);
            }
            isNeedEventTime = true;
        }

        // prepare eventInfos
        StringBuilder errMsg = new StringBuilder();
        if (!checkSchema(expandEventSchemes, expandTimeKeys, commonKeys, errMsg))
            throw new IllegalArgumentException(funcName + " " + errMsg);

        this.commonKeySize = commonKeys.size();
    }

    public boolean checkInputTable(String tableName, BasicTable outputTable, StringBuilder errMsg) {
        outputColNums = isNeedEventTime ? (3 + commonKeySize) : (2 + commonKeySize);
        if (outputColNums != outputTable.columns()) {
            errMsg.append("Incompatible ")
                    .append(tableName)
                    .append(" columns, expected: ")
                    .append(outputColNums)
                    .append(", got: ")
                    .append(outputTable.columns());
            return false;
        }
        int colIdx = 0;
        if (this.isNeedEventTime) {
            if (Entity.typeToCategory(outputTable.getColumn(0).getDataType()) != Entity.DATA_CATEGORY.TEMPORAL) {
                errMsg.append("First column of outputTable should be temporal if specified eventTimeKey.");
                return false;
            }
            colIdx++;
        }
        int typeIdx = colIdx++;
        int blobIdx_ = colIdx++;

        if (outputTable.getColumn(typeIdx).getDataType() != Entity.DATA_TYPE.DT_STRING &&
                outputTable.getColumn(typeIdx).getDataType() != Entity.DATA_TYPE.DT_SYMBOL) {
            errMsg.append("The eventType column must be a string or symbol column");
            return false;
        }

        if (outputTable.getColumn(blobIdx_).getDataType() != Entity.DATA_TYPE.DT_BLOB) {
            errMsg.append("The event column must be a blob column");
            return false;
        }

        return true;
    }

    public boolean serializeEvent(String eventType, List<Entity> attributes, List<Entity> serializedEvent, StringBuilder errMsg) {
        EventInfo info = eventInfos.get(eventType);
        if (info == null) {
            errMsg.append("unknown eventType ").append(eventType);
            return false;
        }

        if (attributes.size() != info.getAttributeSerializers().size()) {
            errMsg.append("the num of attributes is not match with ").append(eventType);
            return false;
        }

        for (int i = 0; i < attributes.size(); ++i) {
            if (info.getEventScheme().getScheme().getAttrTypes().get(i) != attributes.get(i).getDataType()) {
                errMsg.append("the type of ").append(i + 1).append("th attribute of ").append(eventType)
                        .append(" should be ").append(info.getEventScheme().getScheme().getAttrTypes().get(i).toString())
                        .append(" but now it is ").append(attributes.get(i).getDataType().toString());
                return false;
            }
            if (info.getEventScheme().getScheme().getAttrForms().get(i) != attributes.get(i).getDataForm()) {
                errMsg.append("the form of ").append(i + 1).append("th attribute of ").append(eventType)
                        .append(" should be ").append(info.getEventScheme().getScheme().getAttrForms().get(i).toString())
                        .append(" but now it is ").append(attributes.get(i).getDataForm().toString());
                return false;
            }
        }

        if (isNeedEventTime) {
            try {
                serializedEvent.add(attributes.get(info.getEventScheme().getTimeIndex()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try {
            serializedEvent.add(new BasicString(eventType));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ByteArrayOutputStream memoryStream = new ByteArrayOutputStream();
        ExtendedDataOutput out = new LittleEndianDataOutputStream(memoryStream);
        for (int i = 0; i < attributes.size(); ++i) {
            try {
                info.getAttributeSerializers().get(i).serialize(attributes.get(i), out);
            } catch (IOException e) {
                errMsg.append("serialize ").append(i + 1).append("th attributes fail, ret ").append(e.getMessage());
                throw new RuntimeException(e);
            }
        }

        try {
            serializedEvent.add(new BasicString(memoryStream.toByteArray(), true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (int commonIndex : info.getEventScheme().getCommonKeyIndex()) {
            try {
                serializedEvent.add(attributes.get(commonIndex));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return true;
    }

    public boolean deserializeEvent(List<IMessage> msgs, List<String> eventTypes, List<List<Entity>> attributes, ErrorCodeInfo errorInfo) {
        int x = 0;
        for (IMessage msg : msgs) {
            ++x;
            Entity obj = ((BasicMessage) msg).getMsg();
            int eventTypeIndex = this.isNeedEventTime ? 1 : 0;
            int blobIndex = this.isNeedEventTime ? 2 : 1;
            Entity eventType = ((BasicAnyVector) obj).get(eventTypeIndex);
            Entity blob = ((BasicAnyVector) obj).get(blobIndex);

            EventInfo eventInfo = eventInfos.get(eventType.getString());
            if (eventInfo == null) {
                errorInfo.set(ErrorCodeInfo.Code.EC_InvalidParameter, "Unknown eventType" + eventType);
                return false;
            }

            byte[] blobValues = ((BasicString) blob).getBlobValue();
            ByteArrayInputStream memoryStream = new ByteArrayInputStream(blobValues);
            ExtendedDataInput input = new LittleEndianDataInputStream(memoryStream);

            EventScheme scheme = eventInfo.getEventScheme().getScheme();
            int attrCount = scheme.getAttrTypes().size();
            List<Entity> datas = new ArrayList<>(attrCount);

            for (int i = 0; i < attrCount; ++i) {
                Entity.DATA_FORM form = scheme.getAttrForms().get(i);
                Entity.DATA_TYPE type = scheme.getAttrTypes().get(i);
                int extraParam = scheme.getAttrExtraParams().get(i);

                try {
                    if (form == Entity.DATA_FORM.DF_SCALAR) {
                        if (type == Entity.DATA_TYPE.DT_ANY) {
                            datas.add(deserializeAny(type, form, input));
                        } else {
                            datas.add(deserializeScalar(type, extraParam, input));
                        }
                    } else if (form == Entity.DATA_FORM.DF_VECTOR) {
                        if (type.getValue() < 64 && type != Entity.DATA_TYPE.DT_SYMBOL && type != Entity.DATA_TYPE.DT_STRING) {
                            datas.add(deserializeFastArray(type, extraParam, input));
                        } else {
                            datas.add(deserializeAny(type, form, input));
                        }
                    } else {
                        datas.add(deserializeAny(type, form, input));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                if (datas.get(i) == null) {
                    errorInfo.set(ErrorCodeInfo.Code.EC_InvalidObject, "Deserialize blob error.");
                    return false;
                }
            }

            eventTypes.add(eventType.getString());
            attributes.add(datas);
        }

        return true;
    }

    private boolean checkSchema(List<EventScheme> eventSchemes, List<String> expandTimeKeys, List<String> commonKeys, StringBuilder errMsg) {
        int index = 0;
        for (EventScheme scheme : eventSchemes) {
            if (eventInfos.containsKey(scheme.getEventType())) {
                errMsg.append("eventType must be unique");
                return false;
            }

            EventSchemeEx schemeEx = new EventSchemeEx();
            schemeEx.setScheme(scheme);

            if (isNeedEventTime) {
                int timeIndex = scheme.getAttrKeys().indexOf(expandTimeKeys.get(index));
                if (timeIndex == -1) {
                    errMsg.append("event ").append(scheme.getEventType()).append(" doesn't contain eventTimeKey ").append(expandTimeKeys.get(index));
                    return false;
                }
                schemeEx.setTimeIndex(timeIndex);
            }

            for (String commonKey : commonKeys) {
                int commonKeyIndex = scheme.getAttrKeys().indexOf(commonKey);
                if (commonKeyIndex == -1) {
                    errMsg.append("event ").append(scheme.getEventType()).append(" doesn't contain commonKey ").append(commonKey);
                    return false;
                }
                schemeEx.getCommonKeyIndex().add(commonKeyIndex);
            }

            List<AttributeSerializer> serls = new ArrayList<>();
            int length = scheme.getAttrKeys().size();
            for (int j = 0; j < length; ++j) {
                Entity.DATA_TYPE type = scheme.getAttrTypes().get(j);
                Entity.DATA_FORM form = scheme.getAttrForms().get(j);

                // todo 因为目前server代码不能正确序列化symbol vector,所以API暂时先不支持
//                if (type < 0 || type > Entity.DATA_TYPE.DT_OBJECT_ARRAY || type == Entity.DATA_TYPE.DT_SYMBOL) {
                if (type == Entity.DATA_TYPE.DT_SYMBOL) {
                    errMsg.append("not support DT_SYMBOL type.");
                    return false;
                }

                if ((form == Entity.DATA_FORM.DF_SCALAR || form == Entity.DATA_FORM.DF_VECTOR) && type.getValue() < Entity.DATA_TYPE.DT_DECIMAL128_ARRAY.getValue() && type != Entity.DATA_TYPE.DT_ANY) {
                    if (type.getValue() > 0) {
                        if (form == Entity.DATA_FORM.DF_SCALAR) {
                            serls.add(new ScalarAttributeSerializer(1));
                        } else {
                            serls.add(new FastArrayAttributeSerializer(1));
                        }
                        continue;
                    }

                    // BLOB STRING
                    // todo unitlen 如何获取，以及后续是否有用，这里先写 1
                    int unitLen = AbstractVector.getUnitLength(type);
                    if (unitLen > 0) {
                        if (form == Entity.DATA_FORM.DF_SCALAR) {
                            serls.add(new ScalarAttributeSerializer(unitLen));
                        } else {
                            serls.add(new FastArrayAttributeSerializer(unitLen));
                        }
                        continue;
                    }

                    // BLOB STRING
                    if (unitLen < 0 && form != Entity.DATA_FORM.DF_VECTOR) {
                        serls.add(new StringScalarAttributeSerializer(type == Entity.DATA_TYPE.DT_BLOB));
                        continue;
                    }
                }
                serls.add(new AttributeSerializer(0, form));
            }

            EventInfo info = new EventInfo(serls, schemeEx);
            eventInfos.put(scheme.getEventType(), info);
            index++;
        }
        return true;
    }

    private Entity deserializeScalar(Entity.DATA_TYPE type, int extraParam, ExtendedDataInput input) throws IOException {
        BasicEntityFactory factory = new BasicEntityFactory();
        return factory.createEntity(Entity.DATA_FORM.DF_SCALAR, type, input, false);
    }

    private Entity deserializeFastArray(Entity.DATA_TYPE type, int extraParam, ExtendedDataInput input) throws IOException {
        BasicEntityFactory factory = new BasicEntityFactory();
        return factory.createEntity(Entity.DATA_FORM.DF_VECTOR, type, input, false);
    }

    private Entity deserializeAny(Entity.DATA_TYPE type, Entity.DATA_FORM form, ExtendedDataInput input) throws IOException {
        BasicEntityFactory factory = new BasicEntityFactory();
        return factory.createEntity(form, type, input, false);
    }

}
