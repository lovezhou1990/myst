package org.apache.seatunnel.connectors.cdc.base.option;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhoulj(周利军) [1217102780@qq.com]
 * @Title: ChangeLogMode
 * @Project:
 * @Module ID:   <(模块)类编号，可以引用系统设计中的类编号>
 * @Comments: <对此类的描述，可以引用系统设计中的描述>
 * @JDK version used:      <JDK1.8> 41
 * @since 2024/6/5-16:41
 */
public class ChangeLogMode {

    public static final Option<List<String>> includeFields =
            Options.key("changelog.include_fields")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("需要包含的字段名称");
    public static final Option<List<String>> excludeFields =
            Options.key("changelog.exclude_fields")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("需要排除的字段名称");

}
