package bq.avro;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMultimap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Modified from https://raw.githubusercontent.com/apache/beam/v2.15.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryAvroUtils.java
 */
public class BigQueryAvroUtilsV2 {

    static final String BIGQUERY_AVRO_NAMESPACE = "Root";

    public static final ImmutableMultimap<String, Type> BIG_QUERY_TO_AVRO_TYPES =
            ImmutableMultimap.<String, Type>builder()
                    .put("STRING", Type.STRING)
                    .put("GEOGRAPHY", Type.STRING)
                    .put("BYTES", Type.BYTES)
                    .put("INTEGER", Type.LONG)
                    .put("FLOAT", Type.DOUBLE)
                    .put("NUMERIC", Type.BYTES)
                    .put("BOOLEAN", Type.BOOLEAN)
                    .put("TIMESTAMP", Type.LONG)
                    .put("RECORD", Type.RECORD)
                    .put("DATE", Type.STRING)
                    .put("DATE", Type.INT)
                    .put("DATETIME", Type.STRING)
                    .put("TIME", Type.STRING)
                    .put("TIME", Type.LONG)
                    .build();

    public static Schema toGenericAvroSchema(String schemaName, List<TableFieldSchema> fieldSchemas) {
        return toGenericAvroSchema(BIGQUERY_AVRO_NAMESPACE, fieldSchemas, BIGQUERY_AVRO_NAMESPACE,  true);
    }

    private static Schema toGenericAvroSchema(String schemaName, List<TableFieldSchema> fieldSchemas, String namespace, boolean isTopLevel) {
        List<Field> avroFields = new ArrayList<>();
        for (TableFieldSchema bigQueryField : fieldSchemas) {
            avroFields.add(convertField(bigQueryField, namespace));
        }

        if (isTopLevel) {
            return Schema.createRecord(
                    schemaName,
                    null,
                    null,
                    false,
                    avroFields);
        }

        // Remove the last part of namespace
        String[] tmp = namespace.split("\\.");
        String[] removedTmp = Arrays.copyOfRange(tmp, 0, tmp.length - 1);
        String result = String.join(".", removedTmp);

        return Schema.createRecord(
                schemaName,
                null,
                result.toLowerCase(),
                false,
                avroFields);
    }

    /**
     * From some_random_name to Some_Random_Name
     * */
    private static String convertToCapital(String str){
        StringBuilder builder = new StringBuilder();
        builder.append(Character.toUpperCase(str.charAt(0)));
        for (int i = 1; i < str.length(); ++i){
            builder.append(str.charAt(i));
            if (str.charAt(i) == '_') {
                builder.append(Character.toUpperCase(str.charAt(i + 1)));
                i++;
            }
        }

        return builder.toString();
    }

    private static Field convertField(TableFieldSchema bigQueryField, String namespace) {
        Type avroType = BIG_QUERY_TO_AVRO_TYPES.get(bigQueryField.getType()).iterator().next();
        Schema elementSchema;
        if (avroType == Type.RECORD) {
            String converted = convertToCapital(bigQueryField.getName());
            elementSchema = toGenericAvroSchema(converted, bigQueryField.getFields(), namespace.toLowerCase() + "." + converted.toLowerCase(), false);
        } else {
            elementSchema = Schema.create(avroType);
        }
        Schema fieldSchema;
        if (bigQueryField.getMode() == null || "NULLABLE".equals(bigQueryField.getMode())) {
            fieldSchema = Schema.createUnion(Schema.create(Type.NULL), elementSchema);
        } else if ("REQUIRED".equals(bigQueryField.getMode())) {
            fieldSchema = elementSchema;
        } else if ("REPEATED".equals(bigQueryField.getMode())) {
            fieldSchema = Schema.createArray(elementSchema);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unknown BigQuery Field Mode: %s", bigQueryField.getMode()));
        }
        return new Field(
                bigQueryField.getName(),
                fieldSchema,
                bigQueryField.getDescription(),
                (Object) null /* Cast to avoid deprecated JsonNode constructor. */);
    }
}

