/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.jmx;

/**
 * Zookeeper MBean信息接口, MBeanRegistry使用该接口来生成JMX对象名称。
 */
public interface ZKMBeanInfo {

    /**
     * 标识MBean的字符串
     *
     * @return a string identifying the MBean 
     */
    public String getName();

    /**
     * 如果isHidden返回true, MBean将不会在MBean服务器上注册，因此将不能用于管理工具。用于对mbean进行分组。
     * @return true if the MBean is hidden.
     */
    public boolean isHidden();
}
