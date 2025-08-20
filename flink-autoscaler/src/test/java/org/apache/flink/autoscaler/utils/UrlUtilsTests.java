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

package org.apache.flink.autoscaler.utils;

import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * @email danyuhao@qq.com
 * @author: Hao Yu
 * @date: 2025/8/19
 * @time: 13:44
 */
public class UrlUtilsTests {

    @Test
    public void testUrlUtils() throws MalformedURLException {
        String url = "http://rm5.hadoop.ctripcorp.com:8088/proxy/application_1755093121837_1618796";
        URL URL = new URL(url);
        System.out.println(url.toString());
    }
}
