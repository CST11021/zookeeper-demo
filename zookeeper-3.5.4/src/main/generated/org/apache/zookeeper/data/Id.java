// File generated by hadoop record compiler. Do not edit.
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

package org.apache.zookeeper.data;

import org.apache.jute.*;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public class Id implements Record {

    private String scheme;

    private String id;

    public Id() {
    }

    public Id(String scheme, String id) {
        this.scheme = scheme;
        this.id = id;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String m_) {
        scheme = m_;
    }

    public String getId() {
        return id;
    }

    public void setId(String m_) {
        id = m_;
    }

    public void serialize(OutputArchive a_, String tag) throws java.io.IOException {
        a_.startRecord(this, tag);
        a_.writeString(scheme, "scheme");
        a_.writeString(id, "id");
        a_.endRecord(this, tag);
    }

    public void deserialize(InputArchive a_, String tag) throws java.io.IOException {
        a_.startRecord(tag);
        scheme = a_.readString("scheme");
        id = a_.readString("id");
        a_.endRecord(tag);
    }

    public String toString() {
        try {
            java.io.ByteArrayOutputStream s =
                    new java.io.ByteArrayOutputStream();
            CsvOutputArchive a_ =
                    new CsvOutputArchive(s);
            a_.startRecord(this, "");
            a_.writeString(scheme, "scheme");
            a_.writeString(id, "id");
            a_.endRecord(this, "");
            return new String(s.toByteArray(), "UTF-8");
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
        return "ERROR";
    }

    public void write(java.io.DataOutput out) throws java.io.IOException {
        BinaryOutputArchive archive = new BinaryOutputArchive(out);
        serialize(archive, "");
    }

    public void readFields(java.io.DataInput in) throws java.io.IOException {
        BinaryInputArchive archive = new BinaryInputArchive(in);
        deserialize(archive, "");
    }

    public int compareTo(Object peer_) throws ClassCastException {
        if (!(peer_ instanceof Id)) {
            throw new ClassCastException("Comparing different types of records.");
        }
        Id peer = (Id) peer_;
        int ret = 0;
        ret = scheme.compareTo(peer.scheme);
        if (ret != 0) return ret;
        ret = id.compareTo(peer.id);
        if (ret != 0) return ret;
        return ret;
    }

    public boolean equals(Object peer_) {
        if (!(peer_ instanceof Id)) {
            return false;
        }
        if (peer_ == this) {
            return true;
        }
        Id peer = (Id) peer_;
        boolean ret = false;
        ret = scheme.equals(peer.scheme);
        if (!ret) return ret;
        ret = id.equals(peer.id);
        if (!ret) return ret;
        return ret;
    }

    public int hashCode() {
        int result = 17;
        int ret;
        ret = scheme.hashCode();
        result = 37 * result + ret;
        ret = id.hashCode();
        result = 37 * result + ret;
        return result;
    }

    public static String signature() {
        return "LId(ss)";
    }
}
