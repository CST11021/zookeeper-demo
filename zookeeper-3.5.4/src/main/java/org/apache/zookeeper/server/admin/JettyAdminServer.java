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

import org.apache.zookeeper.server.ZooKeeperServer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

/**
 * 启动jetty服务器，用于通过调用url执行zookeeper命令，这个类封装了一个Jetty服务器来运行命令。
 *
 * 给定默认设置，启动ZooKeeper服务器并访问，
 * http://<hostname>:8080/commands，用于链接到所有已注册的命令。参照：http://<hostname>:8080/commands/<commandname>将执行相关的命令并在响应体中返回结果。
 * 命令的任何关键字参数都用URL参数指定(例如，http://localhost:8080/commands/set_trace_mask?traceMask=306)。
 *
 * @see Commands
 * @see CommandOutputter
 */
public class JettyAdminServer implements AdminServer {
    static final Logger LOG = LoggerFactory.getLogger(JettyAdminServer.class);

    /** 默认的服务端口 */
    public static final int DEFAULT_PORT = 8080;
    public static final int DEFAULT_IDLE_TIMEOUT = 30000;
    public static final String DEFAULT_COMMAND_URL = "/commands";
    /***/
    private static final String DEFAULT_ADDRESS = "0.0.0.0";

    /** jetty的Server类 */
    private final Server server;
    private final String address;
    private final int port;
    private final int idleTimeout;
    private final String commandUrl;
    private ZooKeeperServer zkServer;

    public JettyAdminServer() throws AdminServerException {
        this(System.getProperty("zookeeper.admin.serverAddress", DEFAULT_ADDRESS),
             Integer.getInteger("zookeeper.admin.serverPort", DEFAULT_PORT),
             Integer.getInteger("zookeeper.admin.idleTimeout", DEFAULT_IDLE_TIMEOUT),
             System.getProperty("zookeeper.admin.commandURL", DEFAULT_COMMAND_URL));
    }
    public JettyAdminServer(String address, int port, int timeout, String commandUrl) {
        this.port = port;
        this.idleTimeout = timeout;
        this.commandUrl = commandUrl;
        this.address = address;

        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setHost(address);
        connector.setPort(port);
        connector.setIdleTimeout(idleTimeout);
        server.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/*");
        server.setHandler(context);

        context.addServlet(new ServletHolder(new CommandServlet()), commandUrl + "/*");
    }

    /**
     * Start the embedded Jetty server.
     */
    @Override
    public void start() throws AdminServerException {
        try {
            server.start();
        } catch (Exception e) {
            // Server.start() only throws Exception, so let's at least wrap it in an identifiable subclass
            throw new AdminServerException(String.format("Problem starting AdminServer on address %s,"
                            + " port %d and command URL %s", address, port, commandUrl), e);
        }
        LOG.info(String.format("Started AdminServer on address %s, port %d" + " and command URL %s", address, port, commandUrl));
    }

    /**
     * Stop the embedded Jetty server.
     *
     * This is not very important except for tests where multiple
     * JettyAdminServers are started and may try to bind to the same ports if
     * previous servers aren't shut down.
     */
    @Override
    public void shutdown() throws AdminServerException {
        try {
            server.stop();
        } catch (Exception e) {
            throw new AdminServerException(String.format("Problem stopping AdminServer on address %s,"
                    + " port %d and command URL %s", address, port, commandUrl), e);
        }
    }

    /**
     * 设置用于运行命令的ZooKeeperServer。
     *
     * 没有必要在调用AdminServer.start()之前设置ZK服务器，并且可以将ZK服务器设置为null，例如，该服务器正在关闭。
     * 如果ZK服务器没有设置或设置为null, AdminServer仍然可以发出命令，但是在设置ZK服务器之前，它们将返回一个错误。
     */
    @Override
    public void setZooKeeperServer(ZooKeeperServer zkServer) {
        this.zkServer = zkServer;
    }

    /**
     * 用于处理来自Jetty客户端的请求
     */
    private class CommandServlet extends HttpServlet {
        private static final long serialVersionUID = 1L;

        /**
         * 处理get请求
         *
         * @param request
         * @param response
         * @throws ServletException
         * @throws IOException
         */
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            // Capture the command name from the URL
            String cmd = request.getPathInfo();
            if (cmd == null || cmd.equals("/")) {
                // No command specified, print links to all commands instead
                for (String link : commandLinks()) {
                    response.getWriter().println(link);
                    response.getWriter().println("<br/>");
                }
                return;
            }
            // Strip leading "/"
            cmd = cmd.substring(1);

            // Extract keyword arguments to command from request parameters
            @SuppressWarnings("unchecked")
            Map<String, String[]> parameterMap = request.getParameterMap();
            Map<String, String> kwargs = new HashMap<String, String>();
            for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
                kwargs.put(entry.getKey(), entry.getValue()[0]);
            }

            // 运行命令
            CommandResponse cmdResponse = Commands.runCommand(cmd, zkServer, kwargs);

            // 格式化并打印命令的输出
            CommandOutputter outputter = new JsonOutputter();
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(outputter.getContentType());
            outputter.output(cmdResponse, response.getWriter());
        }
    }

    /**
     * Returns a list of URLs to each registered Command.
     */
    private List<String> commandLinks() {
        List<String> links = new ArrayList<String>();
        List<String> commands = new ArrayList<String>(Commands.getPrimaryNames());
        Collections.sort(commands);
        for (String command : commands) {
            String url = commandUrl + "/" + command;
            links.add(String.format("<a href=\"%s\">%s</a>", url, command));
        }
        return links;
    }

    // 测试：
    // 使用如下代码启动jetty，并访问：http://localhost:8080/commands
    // public static void main(String[] args) throws AdminServerException {
    //     new JettyAdminServer().start();
    // }
}
