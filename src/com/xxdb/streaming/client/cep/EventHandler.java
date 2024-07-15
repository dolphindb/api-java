package com.xxdb.streaming.client.cep;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.io.*;
import com.xxdb.streaming.client.BasicMessage;
import com.xxdb.streaming.client.IMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.Set;

public class EventHandler {

    private Map<String, EventInfo> eventInfos;
    private boolean isNeedEventTime;
    private int outputColNums;
    private int commonKeySize;

    public EventHandler(List<EventSchema> eventSchemas, List<String> eventTimeKeys, List<String> commonKeys) {
        this.isNeedEventTime = false;
        this.outputColNums = 0;
        this.commonKeySize = 0;
        this.eventInfos = new HashMap<>();

        String funcName = "createEventSender";
        // check eventSchemas
        if (Objects.isNull(eventSchemas) || eventSchemas.isEmpty())
            throw new IllegalArgumentException("eventSchema must be non-null and non-empty.");

        List<EventSchema> expandEventSchemas = new ArrayList<>(eventSchemas);
        for (EventSchema event : expandEventSchemas) {
            if (Utils.isEmpty(event.getEventType()))
                throw new IllegalArgumentException("eventType must be non-empty.");

            // check schema fieldNames
            Set<String> set = new HashSet<>();
            for (String fieldName : event.getFieldNames()) {
                if (Utils.isEmpty(fieldName))
                    throw new IllegalArgumentException("fieldName must be non-null and non-empty.");

                // check if has duplicate key in fieldName
                if (!set.add(fieldName))
                    throw new IllegalArgumentException("EventSchema cannot has duplicated fieldName in fieldNames.");
            }

            // check schema fieldForms
            for (Entity.DATA_FORM fieldForm : event.getFieldForms()) {
                if (Objects.isNull(fieldForm))
                    throw new IllegalArgumentException("fieldForm must be non-null.");
                if (fieldForm != Entity.DATA_FORM.DF_SCALAR && fieldForm != Entity.DATA_FORM.DF_VECTOR)
                    throw new IllegalArgumentException("fieldForm only can be DF_SCALAR or DF_VECTOR.");
            }

            int length = event.getFieldNames().size();
            if (event.getFieldExtraParams().isEmpty()) {
                event.setFieldExtraParams(Collections.nCopies(length, 0));
            }
            if (length == 0) {
                throw new IllegalArgumentException("eventKey in eventSchema must be non-empty.");
            }
            if ((!event.getFieldExtraParams().isEmpty() && length != event.getFieldExtraParams().size()) || length != event.getFieldForms().size() || length != event.getFieldTypes().size()) {
                throw new IllegalArgumentException("the number of eventKey, eventTypes, eventForms and eventExtraParams (if set) must have the same length.");
            }

            // check if fieldExtraParams valid
            if (Objects.nonNull(event.getFieldExtraParams()) && !event.getFieldExtraParams().isEmpty()) {
                for (int i = 0; i < event.getFieldTypes().size(); i++) {
                    Entity.DATA_TYPE fieldType = event.getFieldTypes().get(i);
                    Integer scale = event.getFieldExtraParams().get(i);
                    if (fieldType == Entity.DATA_TYPE.DT_DECIMAL32 && (scale < 0 || scale > 9))
                        throw new IllegalArgumentException(fieldType + " scale " + scale + " is out of bounds, it must be in [0,9].");
                    else if (fieldType == Entity.DATA_TYPE.DT_DECIMAL64 && (scale < 0 || scale > 18))
                        throw new IllegalArgumentException(fieldType + " scale " + scale + " is out of bounds, it must be in [0,18].");
                    else if (fieldType == Entity.DATA_TYPE.DT_DECIMAL128 && (scale < 0 || scale > 38))
                        throw new IllegalArgumentException(fieldType + " scale " + scale + " is out of bounds, it must be in [0,38].");
                }
            }
        }
        int eventNum = eventSchemas.size();

        // check eventTimeKeys
        List<String> expandTimeKeys = new ArrayList<>();
        if (!eventTimeKeys.isEmpty()) {
            // if eventTimeKeys only contain one element, it means every event has this key
            if (eventTimeKeys.size() == 1) {
                expandTimeKeys = Collections.nCopies(eventNum, eventTimeKeys.get(0));
            } else {
                if (eventTimeKeys.size() != eventNum) {
                    throw new IllegalArgumentException(funcName + "the number of eventTimeKey is inconsistent with the number of events in eventSchemas.");
                }
                expandTimeKeys = new ArrayList<>(eventTimeKeys);
            }
            isNeedEventTime = true;
        }

        // prepare eventInfos
        StringBuilder errMsg = new StringBuilder();
        if (!checkSchema(expandEventSchemas, expandTimeKeys, commonKeys, errMsg))
            throw new IllegalArgumentException(errMsg.toString());

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
            errMsg.append("the number of event values does not match ").append(eventType);
            return false;
        }

        for (int i = 0; i < attributes.size(); ++i) {
            if (info.getEventSchema().getSchema().getFieldTypes().get(i) != attributes.get(i).getDataType()) {
                // An exception: when the type in schema is symbol, you can pass a string attribute
                if (info.getEventSchema().getSchema().getFieldTypes().get(i) == Entity.DATA_TYPE.DT_SYMBOL && attributes.get(i).getDataType() == Entity.DATA_TYPE.DT_STRING)
                    continue;

                errMsg.append("Expected type for the field ").append(info.getEventSchema().getSchema().getFieldNames().get(i)).append(" of ").append(eventType).append(":")
                        .append(info.getEventSchema().getSchema().getFieldTypes().get(i).toString())
                        .append(", but now it is ").append(attributes.get(i).getDataType().toString());
                return false;
            }

            if (info.getEventSchema().getSchema().getFieldForms().get(i) != attributes.get(i).getDataForm()) {
                errMsg.append("Expected form for the field ").append(info.getEventSchema().getSchema().getFieldNames().get(i)).append(" of ").append(eventType).append(":")
                        .append(", but now it is ").append(attributes.get(i).getDataForm().toString());
                return false;
            }

            // check schema fieldExtraParams with decimal attribute.
            EventInfo eventInfo = this.eventInfos.get(eventType);
            EventSchemaEx eventSchema = eventInfo.getEventSchema();
            EventSchema schema = eventSchema.getSchema();
            List<Integer> fieldExtraParams = schema.getFieldExtraParams();
            if (!fieldExtraParams.isEmpty()) {
                Entity attribute = attributes.get(i);
                if (attribute.isScalar()) {
                    if ((attribute.getDataType() == Entity.DATA_TYPE.DT_DECIMAL32 && ((BasicDecimal32) attribute).getScale() != fieldExtraParams.get(i))
                            || (attribute.getDataType() == Entity.DATA_TYPE.DT_DECIMAL64 && ((BasicDecimal64) attribute).getScale() != fieldExtraParams.get(i))
                            || (attribute.getDataType() == Entity.DATA_TYPE.DT_DECIMAL128 && ((BasicDecimal128) attribute).getScale() != fieldExtraParams.get(i)))
                        throw new IllegalArgumentException("The decimal attribute' scale doesn't match to schema fieldExtraParams scale.");
                } else if (attribute.isVector()) {
                    if ((attribute.getDataType() == Entity.DATA_TYPE.DT_DECIMAL32 && ((BasicDecimal32Vector) attribute).getScale() != fieldExtraParams.get(i))
                            || (attribute.getDataType() == Entity.DATA_TYPE.DT_DECIMAL64 && ((BasicDecimal64Vector) attribute).getScale() != fieldExtraParams.get(i))
                            || (attribute.getDataType() == Entity.DATA_TYPE.DT_DECIMAL128 && ((BasicDecimal128Vector) attribute).getScale() != fieldExtraParams.get(i)))
                        throw new IllegalArgumentException("The decimal attribute' scale doesn't match to schema fieldExtraParams scale.");
                }
            }
        }

        if (isNeedEventTime) {
            try {
                serializedEvent.add(attributes.get(info.getEventSchema().getTimeIndex()));
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
                // errMsg.append("serialize ").append(i + 1).append("th attributes fail, ret ").append(e.getMessage());
                errMsg.append("Failed to serialize the field ").append(info.getEventSchema().getSchema().getFieldNames().get(i))
                        .append(", ").append(e);
                throw new RuntimeException(e);
            }
        }

        try {
            serializedEvent.add(new BasicString(memoryStream.toByteArray(), true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (int commonIndex : info.getEventSchema().getCommonKeyIndex()) {
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

            EventSchema schema = eventInfo.getEventSchema().getSchema();
            int attrCount = schema.getFieldTypes().size();
            List<Entity> datas = new ArrayList<>(attrCount);

            for (int i = 0; i < attrCount; ++i) {
                Entity.DATA_FORM form = schema.getFieldForms().get(i);
                Entity.DATA_TYPE type = schema.getFieldTypes().get(i);
                int extraParam;
                if (Objects.nonNull(schema.getFieldExtraParams().get(i)))
                    extraParam = schema.getFieldExtraParams().get(i);
                else
                    extraParam = -1;

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

    private boolean checkSchema(List<EventSchema> eventSchemas, List<String> expandTimeKeys, List<String> commonKeys, StringBuilder errMsg) {
        int index = 0;
        for (EventSchema schema : eventSchemas) {
            if (eventInfos.containsKey(schema.getEventType())) {
                errMsg.append("EventType must be unique.");
                return false;
            }

            EventSchemaEx schemaEx = new EventSchemaEx();
            schemaEx.setSchema(schema);

            if (isNeedEventTime) {
                int timeIndex = schema.getFieldNames().indexOf(expandTimeKeys.get(index));
                if (timeIndex == -1) {
                    errMsg.append("Event ").append(schema.getEventType()).append(" doesn't contain eventTimeKey ").append(expandTimeKeys.get(index)).append(".");
                    return false;
                }
                schemaEx.setTimeIndex(timeIndex);
            }

            for (String commonKey : commonKeys) {
                int commonKeyIndex = schema.getFieldNames().indexOf(commonKey);
                if (commonKeyIndex == -1) {
                    errMsg.append("Event ").append(schema.getEventType()).append(" doesn't contain commonKey ").append(commonKey);
                    return false;
                }
                schemaEx.getCommonKeyIndex().add(commonKeyIndex);
            }

            List<AttributeSerializer> serls = new ArrayList<>();
            int length = schema.getFieldNames().size();
            for (int j = 0; j < length; ++j) {
                Entity.DATA_TYPE type = schema.getFieldTypes().get(j);
                Entity.DATA_FORM form = schema.getFieldForms().get(j);

                if (Objects.isNull(type)) {
                    errMsg.append("fieldType must be non-null.");
                    return false;
                }

                if (type.getValue() < Entity.DATA_TYPE.DT_VOID.getValue() || type.getValue() > Entity.DATA_TYPE.DT_DECIMAL128_ARRAY.getValue()) {
                    errMsg.append("Invalid data type for the field " + schema.getFieldNames().get(j) + " of event " + schema.getEventType());
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

                    // todo unitlen 如何获取，以及后续是否有用，这里先写 1
                    int unitLen = AbstractVector.getUnitLength(type);
                    if(type == Entity.DATA_TYPE.DT_SYMBOL){
                        // the size of symbol is 4, but it need to be serialized as a string
                        unitLen = -1;
                    }

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

            EventInfo info = new EventInfo(serls, schemaEx);
            eventInfos.put(schema.getEventType(), info);
            index++;
        }
        return true;
    }

    private Entity deserializeScalar(Entity.DATA_TYPE type, int extraParam, ExtendedDataInput input) throws IOException {
        BasicEntityFactory factory = new BasicEntityFactory();
        if (type == Entity.DATA_TYPE.DT_DECIMAL32)
            return new BasicDecimal32(input, extraParam);
        else if (type == Entity.DATA_TYPE.DT_DECIMAL64)
            return new BasicDecimal64(input, extraParam);
        else if (type == Entity.DATA_TYPE.DT_DECIMAL128)
            return new BasicDecimal128(input, extraParam);
        else
            return factory.createEntity(Entity.DATA_FORM.DF_SCALAR, type, input, false);
    }

    private Entity deserializeFastArray(Entity.DATA_TYPE type, int extraParam, ExtendedDataInput input) throws IOException {
        BasicEntityFactory factory = new BasicEntityFactory();
        input.readShort();
        return factory.createEntity(Entity.DATA_FORM.DF_VECTOR, type, input, false);
    }

    private Entity deserializeAny(Entity.DATA_TYPE type, Entity.DATA_FORM form, ExtendedDataInput input) throws IOException {
        BasicEntityFactory factory = new BasicEntityFactory();
        return factory.createEntity(form, type, input, false);
    }

}
