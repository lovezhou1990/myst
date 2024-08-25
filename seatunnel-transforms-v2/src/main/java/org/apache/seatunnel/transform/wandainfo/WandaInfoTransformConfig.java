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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Getter
@Setter
public class WandaInfoTransformConfig  implements Serializable {
    public Config baseConfig;
    public static final Option<String> url =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("url");

    public static final Option<String> method =
            Options.key("method")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("method");
    public static final Option<String> format =
            Options.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("format");
    public static final Option<String> app_key =
            Options.key("app_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("app_key");
    public static final Option<String> app_secret =
            Options.key("app_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("app_secret");
    public static final Option<String> content_field =
            Options.key("content_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("content_field");

    public static final Option<Map> schema =
            Options.key("schema")
                    .objectType(Map.class)
                    .noDefaultValue()
                    .withDescription(
                            "Specify the field copy relationship between input and output");


    public static WandaInfoTransformConfig of(ReadonlyConfig config) {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
//        Optional<Map<String, String>> optional = config.getOptional(schema);
//        if (optional.isPresent()) {
//            fields.putAll(config.get(schema));
//        } else {
//            fields.put(config.get(DEST_FIELD), config.get(SRC_FIELD));
//        }
        Config baseConfig = config.toConfig();
        WandaInfoTransformConfig copyTransformConfig = new WandaInfoTransformConfig();
        copyTransformConfig.setBaseConfig(baseConfig);
        return copyTransformConfig;
    }
}
