
package org.apache.seatunnel.connectors.seatunnel.wanda.source;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.wanda.source.config.WandaSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.wanda.source.config.WandaSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.wanda.source.exception.WandaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.wanda.source.exception.WandaConnectorException;
import org.apache.seatunnel.shade.com.typesafe.config.Config;


import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

@Slf4j
public class WandaSource extends HttpSource {
    private final WandaSourceParameter wandaSourceParameter = new WandaSourceParameter();

    protected WandaSource(Config pluginConfig) {
        super(pluginConfig);
        // 先构建查询参数；
        this.wandaSourceParameter.buildWithConfig(pluginConfig, null);
    }

    @Override
    public String getPluginName() {
        return "Wanda";
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new HttpSourceReader(
                this.wandaSourceParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField,this.pageInfo);
    }

    private String getParamsFormDb(Config pluginConfig) throws UnsupportedEncodingException {

        // 先获取必要的参数 。。。。    做到可以访问配置的数据，
        // 先获取必要的参数 。。。。    做到可以访问配置的数据，
        return null;
    }
}
