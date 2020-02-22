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

package org.apache.zookeeper.common;


/**
 * 用于检查节点路径是否合法的工具类
 */    
public class PathUtils {
	
	/**
     * 检查节点的路径是否合法，比如：
     * 必须以"/"开始；
     * 必须以"/"结束；
     * 不允许路径中包含特殊字符等
     *
	 * @param path          节点路径
	 * @param isSequential  是否为顺序节点
	 * @throws IllegalArgumentException if the path is invalid
	 */
	public static void validatePath(String path, boolean isSequential) throws IllegalArgumentException {
		validatePath(isSequential? path + "1": path);
	}
	
    /**
     * 检查节点的路径是否合法，比如：
     * 必须以"/"开始；
     * 必须以"/"结束；
     * 不允许路径中包含特殊字符等
     *
     * @param path 节点路径
     * @throws IllegalArgumentException if the path is invalid
     */
    public static void validatePath(String path) throws IllegalArgumentException {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }
        if (path.length() == 0) {
            throw new IllegalArgumentException("Path length must be > 0");
        }
        // 路径必须以"/"开始
        if (path.charAt(0) != '/') {
            throw new IllegalArgumentException("Path must start with / character");
        }

        // done checking - it's the root
        if (path.length() == 1) {
            return;
        }

        // 路径必须以"/"结束
        if (path.charAt(path.length() - 1) == '/') {
            throw new IllegalArgumentException("Path must not end with / character");
        }

        String reason = null;
        char lastc = '/';
        char chars[] = path.toCharArray();
        char c;
        for (int i = 1; i < chars.length; lastc = chars[i], i++) {
            c = chars[i];

            if (c == 0) {
                reason = "null character not allowed @" + i;
                break;
            } else if (c == '/' && lastc == '/') {
                reason = "empty node name specified @" + i;
                break;
            } else if (c == '.' && lastc == '.') {
                if (chars[i-2] == '/' &&
                        ((i + 1 == chars.length)
                                || chars[i+1] == '/')) {
                    reason = "relative paths not allowed @" + i;
                    break;
                }
            } else if (c == '.') {
                if (chars[i-1] == '/' &&
                        ((i + 1 == chars.length)
                                || chars[i+1] == '/')) {
                    reason = "relative paths not allowed @" + i;
                    break;
                }
            } else if (c > '\u0000' && c <= '\u001f'
                    || c >= '\u007f' && c <= '\u009F'
                    || c >= '\ud800' && c <= '\uf8ff'
                    || c >= '\ufff0' && c <= '\uffff') {
                reason = "invalid charater @" + i;
                break;
            }
        }

        if (reason != null) {
            throw new IllegalArgumentException("Invalid path string \"" + path + "\" caused by " + reason);
        }
    }

    /**
     * Convert Windows path to Unix
     *
     * @param path
     *            file path
     * @return converted file path
     */
    public static String normalizeFileSystemPath(String path) {
        if (path != null) {
            String osname = java.lang.System.getProperty("os.name");
            if (osname.toLowerCase().contains("windows")) {
                return path.replace('\\', '/');
            }
        }
        return path;
    }
}
