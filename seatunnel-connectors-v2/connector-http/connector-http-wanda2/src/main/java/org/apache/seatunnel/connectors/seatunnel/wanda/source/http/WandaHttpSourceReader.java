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

package org.apache.seatunnel.connectors.seatunnel.wanda.source.http;

import com.github.rholder.retry.*;
import com.google.common.base.Strings;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONArray;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.source.DeserializationCollector;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
@Setter
@Getter
public class WandaHttpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    protected final SingleSplitReaderContext context;
    protected final HttpParameter httpParameter;
    protected HttpClientProvider httpClient;
    private final DeserializationCollector deserializationCollector;
    private static final Option[] DEFAULT_OPTIONS = {
        Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL
    };
    private JsonPath[] jsonPaths;
    private final JsonField jsonField;
    private final String contentJson;
    private final Configuration jsonConfiguration =
            Configuration.defaultConfiguration().addOptions(DEFAULT_OPTIONS);
    private boolean noMoreElementFlag = true;
    private Optional<PageInfo> pageInfoOptional = Optional.empty();
    private List<Map<String, Object>> dbsourceResult;
    private Config pluginConfig;

    public WandaHttpSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            JsonField jsonField,
            String contentJson,
            PageInfo pageInfo,List<Map<String, Object>> dbsourceResult,Config pluginConfig) {
        this.context = context;
        this.httpParameter = httpParameter;
        this.deserializationCollector = new DeserializationCollector(deserializationSchema);
        this.jsonField = jsonField;
        this.contentJson = contentJson;
        this.pageInfoOptional = Optional.ofNullable(pageInfo);
        this.dbsourceResult = dbsourceResult;
        this.pluginConfig = pluginConfig;
    }

    @Override
    public void open() {
        httpClient = new HttpClientProvider(httpParameter);
        Retryer retryer = RetryerBuilder.<CloseableHttpResponse>newBuilder()
                .retryIfResult(r-> {
                    return r.getStatusLine().getStatusCode() != HttpStatus.SC_OK;
                })
                .retryIfException(ex -> {
                    return ExceptionUtils.indexOfType(ex, IOException.class) != -1;
                })
                .withStopStrategy(StopStrategies.stopAfterAttempt(httpParameter.getRetry()))
                .withWaitStrategy(
                        WaitStrategies.fibonacciWait(
                                httpParameter.getRetryBackoffMultiplierMillis(),
                                httpParameter.getRetryBackoffMaxMillis(),
                                TimeUnit.MILLISECONDS))
                .withRetryListener(
                        new RetryListener() {
                            @Override
                            public <V> void onRetry(Attempt<V> attempt) {
                                if (attempt.hasException()) {
                                    log.error(
                                            String.format(
                                                    "请求异常重试， 尝试次数:[%d] ",
                                                    attempt.getAttemptNumber()),
                                            attempt.getExceptionCause());
                                }
                            }
                        })
                .build();
        httpClient.setRetryer(retryer);
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }

    public static String fillExpression(String expression, Map<String, Object> valuesMap) {
        if (MapUtils.isEmpty(valuesMap)) {
            return expression;
        }
        for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            String key = "${" + entry.getKey() + "}";
            if (entry.getValue() != null) {
                String value = entry.getValue().toString();
                expression = expression.replace(key, value);
            }

        }
        return expression;
    }
    public void pollAndCollectData(Collector<SeaTunnelRow> output,Map<String, Object> dataMap) throws Exception {
        if (MapUtils.isNotEmpty(dataMap)) {
            String queryParaParam = pluginConfig.getString("queryPara");
            Map<String, String> queryParamMap = JsonUtils.toMap(queryParaParam);
            for (String queryParamKey : queryParamMap.keySet()) {
                String queryParamValueStr = queryParamMap.get(queryParamKey);
                queryParamValueStr = fillExpression(queryParamValueStr,dataMap);
                queryParamMap.put(queryParamKey, queryParamValueStr);
            }
            this.httpParameter.getParams().put("queryPara", JsonUtils.toJsonString(queryParamMap));
        }
        HttpResponse response =
                httpClient.execute(
                        this.httpParameter.getUrl(),
                        this.httpParameter.getMethod().getMethod(),
                        this.httpParameter.getHeaders(),
                        this.httpParameter.getParams(),
                        this.httpParameter.getBody());
        if (response.getCode() >= 200 && response.getCode() <= 207) {
            String content = response.getContent();
            if (!Strings.isNullOrEmpty(content)) {
                if (this.httpParameter.isEnableMultilines()) {
                    StringReader stringReader = new StringReader(content);
                    BufferedReader bufferedReader = new BufferedReader(stringReader);
                    String lineStr;
                    while ((lineStr = bufferedReader.readLine()) != null) {
                        collect(output, lineStr);
                    }
                } else {
                    collect(output, content);
                }
            }
            log.debug(
                    "http client execute success request param:[{}], http response status code:[{}], content:[{}]",
                    httpParameter.getParams(),
                    response.getCode(),
                    response.getContent());
        } else {
            String msg =
                    String.format(
                            "http client execute exception, http response status code:[%s], content:[%s]",
                            response.getCode(), response.getContent());
            throw new HttpConnectorException(HttpConnectorErrorCode.REQUEST_FAILED, msg);
        }
    }

    private void updateRequestParam(PageInfo pageInfo) {
        if (this.httpParameter.getParams() == null) {
            httpParameter.setParams(new HashMap<>());
        }
        this.httpParameter
                .getParams()
                .put(pageInfo.getPageField(), pageInfo.getPageIndex().toString());
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            if (pageInfoOptional.isPresent()) {
                if (CollectionUtils.isNotEmpty(this.getDbsourceResult())) {
                    for (Map<String, Object> dataMap : this.getDbsourceResult()) {
                        noMoreElementFlag = false;
                        Long pageIndex = 1L;
                        while (!noMoreElementFlag) {
                            log.error("开始加载第{}页的数据", pageIndex);
                            PageInfo info = pageInfoOptional.get();
                            // increment page
                            info.setPageIndex(pageIndex);
                            // set request param
                            updateRequestParam(info);
                            pollAndCollectData(output, dataMap);
                            pageIndex += 1;
                            Thread.sleep(10);
                        }
                    }
                } else {
                    // 按逻辑俩说这里不会能为空，  为空的话 就是没有配置前置数据源
                    boolean hasQueryDb = this.getPluginConfig().hasPath("queryParaDbsource");
                    if (!hasQueryDb) {
                        noMoreElementFlag = false;
                        Long pageIndex = 1L;
                        while (!noMoreElementFlag) {
                            log.error("开始加载第{}页的数据", pageIndex);
                            PageInfo info = pageInfoOptional.get();
                            // increment page
                            info.setPageIndex(pageIndex);
                            // set request param
                            updateRequestParam(info);
                            pollAndCollectData(output, null);
                            pageIndex += 1;
                            Thread.sleep(10);
                        }
                    }
                }


            } else {
                boolean hasQueryDb = this.getPluginConfig().hasPath("queryParaDbsource");
                if (!hasQueryDb) {
                    pollAndCollectData(output, null);
                } else {
                    if (CollectionUtils.isNotEmpty(this.getDbsourceResult())) {
                        for (Map<String, Object> dataMap : this.getDbsourceResult()) {
                            pollAndCollectData(output, dataMap);
                        }
                    }
                }



            }
        } finally {
            if (Boundedness.BOUNDED.equals(context.getBoundedness()) && noMoreElementFlag) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded http source");
                context.signalNoMoreElement();
            } else {
                if (httpParameter.getPollIntervalMillis() > 0) {
                    Thread.sleep(httpParameter.getPollIntervalMillis());
                }
            }
        }
    }

    private void collect(Collector<SeaTunnelRow> output, String bodyString) throws IOException {
        if ( pageInfoOptional.isPresent()) {
            String pageSum = JsonUtils.stringToJsonNode(getPageInfoOptional(bodyString)).toString();
            if (NumberUtils.isDigits(pageSum)) {
                pageInfoOptional.get().setTotalPageSize(Long.parseLong(pageSum));
            }
        }
        if (contentJson != null) {
            bodyString = JsonUtils.stringToJsonNode(getPartOfJson(bodyString)).toString();
        }
        if (StringUtils.isBlank(bodyString)) {
            return;
        }
//        JsonNode data = JsonUtils.stringToJsonNode(bodyString);
        // page increase
        if (pageInfoOptional.isPresent()) {
            // Determine whether the task is completed by specifying the presence of the 'total
            // page' field
            PageInfo pageInfo = pageInfoOptional.get();
            if (pageInfo.getTotalPageSize() > 0) {
                noMoreElementFlag = pageInfo.getPageIndex() >= pageInfo.getTotalPageSize();
            } else {
                // no 'total page' configured
                int readSize = JsonUtils.stringToJsonNode(bodyString).size();
                // if read size < BatchSize : read finish
                // if read size = BatchSize : read next page.
                noMoreElementFlag = readSize < pageInfo.getBatchSize();
            }
        }
        /*if (data.isArray()) {
            for (int i = 0; i < data.size(); i++) {
                JsonNode jsonNode = data.get(i);
                String s = jsonNode.toString();
                if (StringUtils.isNotBlank(s)) {
                    deserializationCollector.collect(s.getBytes(), output);
                }

            }
        } else {
            deserializationCollector.collect(bodyString.getBytes(), output);
        }*/
        deserializationCollector.collect(bodyString.getBytes(), output);

    }

    private List<Map<String, String>> parseToMap(List<List<String>> datas, JsonField jsonField) {
        List<Map<String, String>> decodeDatas = new ArrayList<>(datas.size());
        String[] keys = jsonField.getFields().keySet().toArray(new String[] {});

        for (List<String> data : datas) {
            Map<String, String> decodeData = new HashMap<>(jsonField.getFields().size());
            final int[] index = {0};
            data.forEach(
                    field -> {
                        decodeData.put(keys[index[0]], field);
                        index[0]++;
                    });
            decodeDatas.add(decodeData);
        }

        return decodeDatas;
    }


    private String getPartOfJson(String data) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
        return JsonUtils.toJsonString(jsonReadContext.read(JsonPath.compile(contentJson)));
    }
    private String getPageInfoOptional(String data) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);

        Object read = jsonReadContext.read(
                JsonPath.compile(pageInfoOptional.get().getDynamPagesumField()));
        if (read!=null && read instanceof JSONArray) {
            JSONArray result = (JSONArray)read;
            if (result.size() > 0) {
                return result.get(0).toString();
            }
        }
        return "";
    }

}
