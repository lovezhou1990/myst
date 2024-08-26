/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.wandainfo;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import lombok.SneakyThrows;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.wanda.source.config.WandaSourceParameter;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.wandainfo.http.WandaInfoHttpClientProvider;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

public class WandaInfoTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "WandaInfo";

    private final WandaInfoTransformConfig config;
    private final WandaSourceParameter wandaSourceParameter;

    private final CatalogTable wandaCatalogTable;


    public WandaInfoTransform(WandaInfoTransformConfig copyTransformConfig, CatalogTable catalogTable) {
        super(catalogTable);
        this.config = copyTransformConfig;
        this.wandaSourceParameter = new WandaSourceParameter();
        this.wandaSourceParameter.buildWithConfig(copyTransformConfig.getBaseConfig(), null);
        this.wandaCatalogTable = CatalogTableUtil.buildWithConfig(copyTransformConfig.getBaseConfig());
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Column[] getOutputColumns() {
        Map<String, Column> catalogTableColumns =
                wandaCatalogTable.getTableSchema().getColumns().stream()
                        .collect(Collectors.toMap(column -> column.getName(), column -> column));

        List<Column> columns = new ArrayList<>();

        for (Column srcColumn : catalogTableColumns.values()) {
            PhysicalColumn destColumn =
                    PhysicalColumn.of(
                            srcColumn.getName(),
                            srcColumn.getDataType(),
                            srcColumn.getColumnLength(),
                            srcColumn.isNullable(),
                            srcColumn.getDefaultValue(),
                            srcColumn.getComment());
            columns.add(destColumn);
        }
        return columns.toArray(new Column[0]);
    }
    private String getPartOfJson(String data) {
        String jsonString = JsonUtils.toJsonString(data);
        return jsonString;
    }
    @SneakyThrows
    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {

        WandaInfoHttpClientProvider  httpClient= new WandaInfoHttpClientProvider(wandaSourceParameter);
        HttpResponse response =
                httpClient.execute(
                        this.wandaSourceParameter.getUrl(),
                        this.wandaSourceParameter.getMethod().getMethod(),
                        this.wandaSourceParameter.getHeaders(),
                        this.wandaSourceParameter.getParams(),
                        this.wandaSourceParameter.getBody());
        if (response.getCode() >= 200 && response.getCode() <= 207) {
            String content = response.getContent();
            Option[] DEFAULT_OPTIONS = {
                    Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL
            };

            ReadContext jsonReadContext = JsonPath.using(
                    Configuration.defaultConfiguration().addOptions(DEFAULT_OPTIONS)
            ).parse(content);
            String bodyString = JsonUtils.toJsonString(jsonReadContext.read(JsonPath.compile(
                    this.config.getBaseConfig().getString("content_field"))));


            JsonNode data = JsonUtils.stringToJsonNode(bodyString);
            Column[] outputColumns = this.getOutputColumns();
            Object[] fieldValues = new Object[outputColumns.length];
            if (data.isArray()) {
                for (int i = 0; i < data.size(); i++) {
                    JsonNode jsonNode = data.get(i);
                    for (int j = 0; j < outputColumns.length; j++) {
                        Column outputColumn = outputColumns[j];

                        String name = outputColumn.getName();
//                        JsonNode jsonNode = data.get(name);
                        fieldValues[j] = getValueByName(jsonNode, outputColumn);

                    }
                }
            }



            for (int i = 0; i < outputColumns.length; i++) {
                Column outputColumn = outputColumns[i];

                String name = outputColumn.getName();
                JsonNode jsonNode = data.get(name);
                fieldValues[i] = getValueByName(jsonNode, outputColumn);

            }
            return fieldValues;
        } else {
            String msg =
                    String.format(
                            "http client execute exception, http response status code:[%s], content:[%s]",
                            response.getCode(), response.getContent());
            throw new HttpConnectorException(HttpConnectorErrorCode.REQUEST_FAILED, msg);
        }
    }

    public Object getValueByName(JsonNode jsonNode, Column outputColumn){
        if (jsonNode == null) {
            return null;
        }else if (Objects.equals(outputColumn.getDataType().getSqlType(), SqlType.STRING)) {
            return jsonNode.asText();
        }else if (Objects.equals(outputColumn.getDataType().getSqlType(), SqlType.BOOLEAN)) {
            return jsonNode.asBoolean();
        }else if (Objects.equals(outputColumn.getDataType().getSqlType(), SqlType.INT)) {
            return jsonNode.asInt();
        }else if (Objects.equals(outputColumn.getDataType().getSqlType(), SqlType.BIGINT)) {
            return jsonNode.asLong();
        }else if (Objects.equals(outputColumn.getDataType().getSqlType(), SqlType.DOUBLE)) {
            return jsonNode.asDouble();
        }else {
            return jsonNode.textValue();
        }
    }

    private Object clone(String field, SeaTunnelDataType<?> dataType, Object value) {
        if (value == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case BOOLEAN:
            case STRING:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return value;
            case BYTES:
                byte[] bytes = (byte[]) value;
                byte[] newBytes = new byte[bytes.length];
                System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                return newBytes;
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Object[] array = (Object[]) value;
                Object newArray =
                        Array.newInstance(arrayType.getElementType().getTypeClass(), array.length);
                for (int i = 0; i < array.length; i++) {
                    Array.set(newArray, i, clone(field, arrayType.getElementType(), array[i]));
                }
                return newArray;
            case MAP:
                MapType mapType = (MapType) dataType;
                Map map = (Map) value;
                Map<Object, Object> newMap = new HashMap<>();
                for (Object key : map.keySet()) {
                    newMap.put(
                            clone(field, mapType.getKeyType(), key),
                            clone(field, mapType.getValueType(), map.get(key)));
                }
                return newMap;
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                SeaTunnelRow row = (SeaTunnelRow) value;

                Object[] newFields = new Object[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    newFields[i] =
                            clone(
                                    rowType.getFieldName(i),
                                    rowType.getFieldType(i),
                                    row.getField(i));
                }
                SeaTunnelRow newRow = new SeaTunnelRow(newFields);
                newRow.setRowKind(row.getRowKind());
                newRow.setTableId(row.getTableId());
                return newRow;
            case NULL:
                return null;
            default:
                throw CommonError.unsupportedDataType(
                        getPluginName(), dataType.getSqlType().toString(), field);
        }
    }
}
