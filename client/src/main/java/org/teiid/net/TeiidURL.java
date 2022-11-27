/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.net;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.teiid.core.util.StringUtil;
import org.teiid.jdbc.JDBCPlugin;


/**
 * Class defines the URL in the Teiid.
 *
 * @since 4.2
 */
public class TeiidURL {

    public static interface JDBC {
        // constant indicating Virtual database name
        public static final String VDB_NAME = "VirtualDatabaseName"; //$NON-NLS-1$
        // constant indicating Virtual database version
        public static final String VDB_VERSION = "VirtualDatabaseVersion"; //$NON-NLS-1$
        // constant for vdb version part of serverURL
        public static final String VERSION = "version"; //$NON-NLS-1$
    }

    public static interface CONNECTION {
        public static final String CLIENT_IP_ADDRESS = "clientIpAddress"; //$NON-NLS-1$
        public static final String CLIENT_HOSTNAME = "clientHostName"; //$NON-NLS-1$
        public static final String CLIENT_MAC = "clientMAC"; //$NON-NLS-1$
        /**
         * If true, will automatically select a new server instance after a communication exception.
         * @since 5.6
         */
        public static final String AUTO_FAILOVER = "autoFailover";  //$NON-NLS-1$

        public static final String SERVER_URL = "serverURL"; //$NON-NLS-1$
        /**
         * Non-secure Protocol.
         */
        public static final String NON_SECURE_PROTOCOL = "mm"; //$NON-NLS-1$
        /**
         * Secure Protocol.
         */
        public static final String SECURE_PROTOCOL = "mms"; //$NON-NLS-1$
        // name of the application which is obtaining connection
        public static final String APP_NAME = "ApplicationName"; //$NON-NLS-1$
        // constant for username part of url
        public static final String USER_NAME = "user"; //$NON-NLS-1$
        // constant for password part of url
        public static final String PASSWORD = "password"; //$NON-NLS-1$

        public static final String PASSTHROUGH_AUTHENTICATION = "PassthroughAuthentication"; //$NON-NLS-1$

        public static final String JAAS_NAME = "jaasName"; //$NON-NLS-1$

        public static final String KERBEROS_SERVICE_PRINCIPLE_NAME = "kerberosServicePrincipleName"; //$NON-NLS-1$;

        public static final String ENCRYPT_REQUESTS = "encryptRequests"; //$NON-NLS-1$;
        public static final String LOGIN_TIMEOUT = "loginTimeout"; //$NON-NLS-1$

    }

    public static final String DOT_DELIMITER = "."; //$NON-NLS-1$
    public static final String DOUBLE_SLASH_DELIMITER = "//"; //$NON-NLS-1$
    public static final String COMMA_DELIMITER = ","; //$NON-NLS-1$

    public static final String COLON_DELIMITER = ":"; //$NON-NLS-1$
    public static final String BACKSLASH_DELIMITER = "\\"; //$NON-NLS-1$
    public static final String DEFAULT_PROTOCOL= TeiidURL.CONNECTION.NON_SECURE_PROTOCOL + "://"; //$NON-NLS-1$
    public static final String SECURE_PROTOCOL= TeiidURL.CONNECTION.SECURE_PROTOCOL + "://"; //$NON-NLS-1$

    public static final String INVALID_FORMAT_SERVER = JDBCPlugin.Util.getString("MMURL.INVALID_FORMAT"); //$NON-NLS-1$
    /*
     * appserver URL
     */
    private String appServerURL;
    /*
     * List of <code> HostData </code> in a cluster
     */
    private List<HostInfo> hosts = new ArrayList<HostInfo>();

    private boolean usingSSL = false;

    /**
     * Create an MMURL from the server URL.  For use by the server-side.
     * @param serverURL   Expected format: mm[s]://server1:port1[,server2:port2]
     * @throws MalformedURLException
     * @since 4.2
     */
    public TeiidURL(String serverURL) throws MalformedURLException {
        if (serverURL == null) {
            throw new MalformedURLException(INVALID_FORMAT_SERVER);
        }
        if (StringUtil.startsWithIgnoreCase(serverURL, SECURE_PROTOCOL)) {
            usingSSL = true;
        } else if (!StringUtil.startsWithIgnoreCase(serverURL, DEFAULT_PROTOCOL)) {
            throw new MalformedURLException(INVALID_FORMAT_SERVER);
        }

        appServerURL = serverURL;
        parseServerURL(serverURL.substring(usingSSL?SECURE_PROTOCOL.length():DEFAULT_PROTOCOL.length()), INVALID_FORMAT_SERVER);
    }

    public TeiidURL(String host, int port, boolean secure) {
        usingSSL = secure;
        if(host.startsWith("[")) { //$NON-NLS-1$
            host = host.substring(1, host.indexOf(']'));
        }
        hosts.add(new HostInfo(host, port));
    }

    /**
     * Validates that a server URL is in the correct format.
     * @param serverURL  Expected format: mm[s]://server1:port1[,server2:port2]
     * @since 4.2
     */
    public static boolean isValidServerURL(String serverURL) {
        boolean valid = true;
        try {
            new TeiidURL(serverURL);
        } catch (Exception e) {
            valid = false;
        }

        return valid;
    }

    public List<HostInfo> getHostInfo() {
        return hosts;
    }

    /**
     * Get a list of hosts
     *
     * @return string of host separated by commas
     * @since 4.2
     */
    public String getHosts() {
        StringBuffer hostList = new StringBuffer("");  //$NON-NLS-1$
        if( hosts != null) {
            Iterator<HostInfo> iterator = hosts.iterator();
            while (iterator.hasNext()) {
                HostInfo element = iterator.next();
                hostList.append(element.getHostName());
                if( iterator.hasNext()) {
                    hostList.append(COMMA_DELIMITER);
                }
            }
        }
        return hostList.toString();
    }

    /**
     * Get a list of ports
     *
     * @return string of ports seperated by commas
     * @since 4.2
     */
    public String getPorts() {
        StringBuffer portList = new StringBuffer("");  //$NON-NLS-1$
        if( hosts != null) {
            Iterator<HostInfo> iterator = hosts.iterator();
            while (iterator.hasNext()) {
                HostInfo element = iterator.next();
                portList.append(element.getPortNumber());
                if( iterator.hasNext()) {
                    portList.append(COMMA_DELIMITER);
                }
            }
        }
        return portList.toString();
    }

    /**
     * @param serverURL
     * @throws MalformedURLException
     * @since 4.2
     */
    private void parseServerURL(String serverURL, String exceptionMessage) throws MalformedURLException {
        StringTokenizer st = new StringTokenizer(serverURL, COMMA_DELIMITER);
        if (!st.hasMoreTokens()) {
            throw new MalformedURLException(exceptionMessage);
        }
        while (st.hasMoreTokens()) {
            String nextToken = st.nextToken();
            nextToken = nextToken.trim();
            String host = ""; //$NON-NLS-1$
            String port = ""; //$NON-NLS-1$
            if (nextToken.startsWith("[")) { //$NON-NLS-1$
                int hostEnd = nextToken.indexOf("]:"); //$NON-NLS-1$
                if (hostEnd == -1) {
                    throw new MalformedURLException(JDBCPlugin.Util.getString("TeiidURL.invalid_ipv6_hostport", nextToken, exceptionMessage)); //$NON-NLS-1$
                }
                host = nextToken.substring(1, hostEnd);
                port = nextToken.substring(hostEnd+2);
            }
            else {
                int hostEnd = nextToken.indexOf(":"); //$NON-NLS-1$
                if (hostEnd == -1) {
                    throw new MalformedURLException(JDBCPlugin.Util.getString("TeiidURL.invalid_hostport", nextToken, exceptionMessage)); //$NON-NLS-1$
                }
                host = nextToken.substring(0, hostEnd);
                port = nextToken.substring(hostEnd+1);
            }
            host = host.trim();
            port = port.trim();
            if (host.equals("") || port.equals("")) { //$NON-NLS-1$ //$NON-NLS-2$
                throw new MalformedURLException(JDBCPlugin.Util.getString("TeiidURL.invalid_hostport", nextToken, exceptionMessage)); //$NON-NLS-1$
            }
            int portNumber = validatePort(port);
            HostInfo hostInfo = new HostInfo(host, portNumber);
            hosts.add(hostInfo);
        }
    }

    public static int validatePort(String port) throws MalformedURLException {
        int portNumber;
        try {
            portNumber = Integer.parseInt(port);
        } catch (NumberFormatException nfe) {
            throw new MalformedURLException(JDBCPlugin.Util.getString("TeiidURL.non_numeric_port", port)); //$NON-NLS-1$
        }
        String msg = validatePort(portNumber);
        if (msg != null) {
            throw new MalformedURLException(msg);
        }
        return portNumber;
    }

    public static String validatePort(int portNumber) {
        if (portNumber < 0 || portNumber > 0xFFFF) {
            return JDBCPlugin.Util.getString("TeiidURL.port_out_of_range", portNumber); //$NON-NLS-1$
        }
        return null;
    }

    /**
     * Get the Application Server URL
     *
     * @return String for connection to the Server
     * @since 4.2
     */
    public String getAppServerURL() {
        if (appServerURL == null) {
            StringBuffer sb = new StringBuffer();
            if (usingSSL) {
                sb.append(SECURE_PROTOCOL);
            } else {
                sb.append(DEFAULT_PROTOCOL);
            }
            Iterator<HostInfo> iter = hosts.iterator();
            while (iter.hasNext()) {
                HostInfo host = iter.next();

                boolean ipv6HostName = host.getHostName().indexOf(':') != -1;
                if (ipv6HostName) {
                    sb.append('[');
                }
                sb.append(host.getHostName());
                if (ipv6HostName) {
                    sb.append(']');
                }
                sb.append(COLON_DELIMITER);
                sb.append(host.getPortNumber());
                if (iter.hasNext()) {
                    sb.append(COMMA_DELIMITER);
                }
            }
            appServerURL = sb.toString();
        }
        return appServerURL;
    }

    /**
     * @see java.lang.Object#toString()
     * @since 4.2
     */
    public String toString() {
        return getAppServerURL();
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 4.2
     */
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof TeiidURL)) {
            return false;
        }
        TeiidURL url = (TeiidURL)obj;
        return (appServerURL.equals(url.getAppServerURL()));
    }

    /**
     * @see java.lang.Object#hashCode()
     * @since 4.2
     */
    public int hashCode() {
        return appServerURL.hashCode();
    }

    public boolean isUsingSSL() {
        return usingSSL;
    }

}
