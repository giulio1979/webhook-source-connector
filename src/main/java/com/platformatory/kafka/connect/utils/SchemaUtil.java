package com.platformatory.kafka.connect.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.platformatory.kafka.connect.WebhookSourceTask;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SchemaUtil {
    static final Logger log = LoggerFactory.getLogger(SchemaUtil.class);

    public static Schema inferSchema(String jsonString) {
        JSONObject json = new JSONObject(jsonString);
        SchemaBuilder structBuilder = SchemaBuilder.struct();

        // Extract field names and types from the JSON string
        for (String key : json.keySet()) {
            Object value = json.get(key);
            if (value instanceof String) {
                structBuilder = structBuilder.field(key, Schema.STRING_SCHEMA);
            } else if (value instanceof Long) {
                structBuilder = structBuilder.field(key, Schema.INT64_SCHEMA);
            } else if (value instanceof Double) {
                structBuilder = structBuilder.field(key, Schema.FLOAT64_SCHEMA);
            } else if (value instanceof Boolean) {
                structBuilder = structBuilder.field(key, Schema.BOOLEAN_SCHEMA);
            } else {
                // Other data types??
                structBuilder = structBuilder.field(key, Schema.STRING_SCHEMA);
            }
        }

        return structBuilder.build();
    }


    public static Schema convertJsonSchemaToKafkaConnectSchema(JsonNode jsonSchema) {
        log.info("JSONSchema - {}", jsonSchema);
        // Initialize a new SchemaBuilder based on the type
        SchemaBuilder schemaBuilder;
        if(jsonSchema != null) {
            // Get the type of the JSON schema
            String type = jsonSchema.get("type").asText();

            switch (type) {
                case "boolean":
                    schemaBuilder = SchemaBuilder.bool();
                    break;
                case "integer":
                    schemaBuilder = SchemaBuilder.int32();
                    break;
                case "number":
                    schemaBuilder = SchemaBuilder.float64();
                    break;
                case "string":
                case "null":
                    schemaBuilder = SchemaBuilder.string();
                    break;
                case "array":
                    schemaBuilder = SchemaBuilder.array(
                            convertJsonSchemaToKafkaConnectSchema(jsonSchema.get("items"))
                    );
                    break;
                case "object":
                    schemaBuilder = SchemaBuilder.struct();
                    JsonNode properties = jsonSchema.get("properties");
                    Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> property = fields.next();
                        schemaBuilder.field(
                                property.getKey(),
                                convertJsonSchemaToKafkaConnectSchema(property.getValue())
                        );
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported JSON schema type: " + type);
            }

            // Handle nullable property
            if (jsonSchema.get("nullable") != null && jsonSchema.get("nullable").asBoolean()) {
                schemaBuilder.optional();
            }
        }  else {
            schemaBuilder = SchemaBuilder.string();
            schemaBuilder.optional();
        }

        return schemaBuilder.build();
    }

    public static Struct convertJsonToKafkaConnectStruct(JsonNode jsonNode, Schema schema) {
        Struct struct = new Struct(schema);
        for (Field field : schema.fields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            JsonNode fieldValue = jsonNode.get(fieldName);
            if (fieldValue == null) {
                struct.put(fieldName, null);
            } else {
                switch (fieldSchema.type()) {
                    case BOOLEAN:
                        struct.put(fieldName, fieldValue.asBoolean());
                        break;
                    case INT32:
                        struct.put(fieldName, fieldValue.asInt());
                        break;
                    case INT64:
                        struct.put(fieldName, fieldValue.asLong());
                        break;
                    case FLOAT32:
                        struct.put(fieldName, (float) fieldValue.asDouble());
                        break;
                    case FLOAT64:
                        struct.put(fieldName, fieldValue.asDouble());
                        break;
                    case STRING:
                        struct.put(fieldName, fieldValue.asText());
                        break;
                    case ARRAY:
                        struct.put(fieldName,
                                convertJsonArrayToKafkaConnectArray(fieldValue, fieldSchema));
                        break;
                    case STRUCT:
                        struct.put(fieldName,
                                convertJsonToKafkaConnectStruct(fieldValue, fieldSchema));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported schema type: " +
                                fieldSchema.type());
                }
            }
        }
        return struct;
    }

    public static List<Object> convertJsonArrayToKafkaConnectArray(JsonNode jsonArray, Schema schema) {
        List<Object> array = new ArrayList<>();
        for (JsonNode jsonElement : jsonArray) {
            switch (schema.valueSchema().type()) {
                case BOOLEAN:
                    array.add(jsonElement.asBoolean());
                    break;
                case INT32:
                    array.add(jsonElement.asInt());
                    break;
                case INT64:
                    array.add(jsonElement.asLong());
                    break;
                case FLOAT32:
                    array.add((float) jsonElement.asDouble());
                    break;
                case FLOAT64:
                    array.add(jsonElement.asDouble());
                    break;
                case STRING:
                    array.add(jsonElement.asText());
                    break;
                case ARRAY:
                    array.add(convertJsonArrayToKafkaConnectArray(jsonElement, schema.valueSchema()));
                    break;
                case STRUCT:
                    array.add(convertJsonToKafkaConnectStruct(jsonElement, schema.valueSchema()));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported schema type: " +
                            schema.valueSchema().type());
            }
        }
        return array;
    }
}
