
package org.apache.seatunnel.connectors.seatunnel.wanda.source.config;


import lombok.SneakyThrows;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class WandaSourceParameter2 extends HttpParameter {

    @SneakyThrows
    public void buildWithConfig(Config pluginConfig, String queryPara)  {
        super.buildWithConfig(pluginConfig);
        // set url
//        // set method
        // set body
        Map<String, String> params = new HashMap<>();
        String appkey = pluginConfig.getString(WandaSourceConfig2.APP_KEY.key());
        String appsecrt = pluginConfig.getString(WandaSourceConfig2.APP_SECRET.key());
        String nowStr = DateTimeUtils.toString(LocalDateTime.now(), DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
        params.put("appKey", appkey);
        params.put("strSysDatetime", URLEncoder.encode(nowStr, "UTF-8"));
        params.put("pageSize", pluginConfig.getString(WandaSourceConfig2.PAGE_SIZE.key()));
        params.put("dataType", "json");
        if (StringUtils.isNotBlank(queryPara)) {

        }
        params.put("queryPara", URLEncoder.encode(
                pluginConfig.getString(WandaSourceConfig2.queryPara.key()), "UTF-8"));
        String sign = DigestUtils.md5Hex(appkey + nowStr + appsecrt);
        params.put("sign", sign);

        this.setParams(params);
        this.setRetryParameters(pluginConfig);
    }
}
