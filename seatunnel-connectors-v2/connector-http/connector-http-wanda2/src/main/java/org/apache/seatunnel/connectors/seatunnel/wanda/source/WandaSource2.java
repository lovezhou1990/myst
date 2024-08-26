
package org.apache.seatunnel.connectors.seatunnel.wanda.source;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.wanda.source.config.WandaSourceParameter2;
import org.apache.seatunnel.connectors.seatunnel.wanda.source.http.WandaHttpSourceReader;
import org.apache.seatunnel.shade.com.typesafe.config.Config;


import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Slf4j
public class WandaSource2 extends HttpSource {
    private final WandaSourceParameter2 wandaSourceParameter = new WandaSourceParameter2();
    List<Map<String, Object>> dbsourceResult;
    protected WandaSource2(Config pluginConfig) {
        super(pluginConfig);
        // 先构建查询参数；
        this.wandaSourceParameter.buildWithConfig(pluginConfig, null);
        Config dbsource = pluginConfig.getConfig("dbsource");
        MySqlCatalog catalog =
                new MySqlCatalog(
                        "mysql",
                        dbsource.getString("user"),
                        dbsource.getString("password"),
                        JdbcUrlUtil.getUrlInfo(dbsource.getString("url")));
        catalog.open();
        List<Map<String, Object>> maps = catalog.querySql(dbsource.getString("querySql"));
        this.dbsourceResult = maps;
    }

    @Override
    public String getPluginName() {
        return "Wanda2";
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new WandaHttpSourceReader(
                this.wandaSourceParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField,this.pageInfo, this.dbsourceResult);
    }

    private String getParamsFormDb(Config pluginConfig) throws UnsupportedEncodingException {

        // 先获取必要的参数 。。。。    做到可以访问配置的数据，
        // 先获取必要的参数 。。。。    做到可以访问配置的数据，

        return null;
    }
}
