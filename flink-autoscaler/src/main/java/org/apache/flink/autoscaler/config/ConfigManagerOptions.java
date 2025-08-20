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

package org.apache.flink.autoscaler.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

/**
 * @email danyuhao@qq.com
 * @author: Hao Yu
 * @date: 2025/8/18
 * @time: 17:53
 */
public class ConfigManagerOptions {

    public static final String CONFIG_MANAGER = "config.manager.";
    public static final ConfigOption<List<String>> CONFIG_MANAGER_LISTENER_JOB_HOSTS =
            autoScalerConfig("listener.jobs.hosts")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription("List of jobs to listen for configuration changes."
                            + " Format: hostA,hostB,...");

    public static final ConfigOption<List<String>> CONFIG_MANAGER_LISTENER_JOB_PORTS =
            autoScalerConfig("listener.jobs.ports")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription("List of jobs to listen for configuration changes."
                            + " Format: portA,portB,...");

    public static final ConfigOption<List<String>> CONFIG_MANAGER_URL_PREFIXES =
            autoScalerConfig("url.prefixes")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription("List of URL prefixes to listen for configuration changes."
                            + " Format: /prefixA/,/prefixB/,...");

    private static String autoScalerConfigKey(String key) {
        return CONFIG_MANAGER + key;
    }

    private static ConfigOptions.OptionBuilder autoScalerConfig(String key) {
        return ConfigOptions.key(autoScalerConfigKey(key));
    }
}
