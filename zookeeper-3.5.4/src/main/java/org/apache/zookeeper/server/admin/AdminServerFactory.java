/**
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

package org.apache.zookeeper.server.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * 用于创建AdminServer的工厂
 */
public class AdminServerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AdminServerFactory.class);

    /**
     * 创建一个AdminServer实例，通过该实例，可以通过HTTP请求查看zk的运行时状态。
     * 这个方法封装了我们是否应该使用JettyAdminServer的逻辑 (即AdminServer已启用)或DummyAdminServer(即AdminServer已禁用)。
     * 它在尝试创建JettyAdminServer时使用反射，而不是直接引用类，因此如果用户不希望使用ZooKeeper导入Jetty，可以从类路径中删除Jetty。
     */
    public static AdminServer createAdminServer() {
        // 如果 zookeeper.admin.enableServer 配置不是false，则默认启动JettyAdminServer
        if (!"false".equals(System.getProperty("zookeeper.admin.enableServer"))) {
            try {
                Class<?> jettyAdminServerC = Class.forName("org.apache.zookeeper.server.admin.JettyAdminServer");
                Object adminServer = jettyAdminServerC.getConstructor().newInstance();
                return (AdminServer) adminServer;

            } catch (ClassNotFoundException e) {
                LOG.warn("Unable to start JettyAdminServer", e);
            } catch (InstantiationException e) {
                LOG.warn("Unable to start JettyAdminServer", e);
            } catch (IllegalAccessException e) {
                LOG.warn("Unable to start JettyAdminServer", e);
            } catch (InvocationTargetException e) {
                LOG.warn("Unable to start JettyAdminServer", e);
            } catch (NoSuchMethodException e) {
                LOG.warn("Unable to start JettyAdminServer", e);
            } catch (NoClassDefFoundError e) {
                LOG.warn("Unable to load jetty, not starting JettyAdminServer", e);
            }
        }
        return new DummyAdminServer();
    }
}
