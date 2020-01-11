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
package org.apache.jute;

import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;

/**
 * Jute生成的类会实现该接口：
 *
 * Record提供了2个方法分别是serialize和deserialize，各自都有2个参数，OutputArchive表示序列化器，InputArchive表示反序列器，tag用于标识对象，主要是因为同一个序列化器可以序列化多个对象，所以需要给每个对象一个标识。
 * 
 */
@InterfaceAudience.Public
public interface Record {

    public void serialize(OutputArchive archive, String tag) throws IOException;

    public void deserialize(InputArchive archive, String tag) throws IOException;

}
