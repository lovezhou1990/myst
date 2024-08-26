
package org.apache.seatunnel.connectors.seatunnel.wanda.source.config;


import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;

public class WandaSourceConfig2 extends HttpConfig {
    public static final String POST = "get";
    public static final Integer pageNum = 1;
    public static final Integer Integer = 1000;
    public static final String dataType = "json";
    public static final String sign = "sign";
    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("url");
    public static final Option<String> APP_KEY =
            Options.key("app_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("app_key");
    public static final Option<String> APP_SECRET =
            Options.key("app_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("app_secret");
    public static final Option<String> queryPara =
            Options.key("queryPara")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("queryPara");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("page_size");
}
