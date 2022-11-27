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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.HashCodeUtil;


/**
 * Defines the hostname/port or {@link InetAddress} to connect to a host.
 *
 * Similar to an {@link InetSocketAddress} except that it can be constructed
 * fully resolved, with an {@link InetAddress} and a hostname.
 *
 * @since 4.2
 */
public class HostInfo {
    // Host Name and Port Number
    private String hostName;
    private int portNumber = 0;
    private InetAddress inetAddress;
    private boolean ssl;

    /**
     * Construct a fully resolved {@link HostInfo}.
     * @param hostName
     * @param addr
     */
    public HostInfo(String hostName, InetSocketAddress addr) {
        this.hostName = hostName;
        this.portNumber = addr.getPort();
        this.inetAddress = addr.getAddress();
    }

    /**
     * Construct a {@link HostInfo} that can resolve each
     * time an {@link InetAddress} is asked for.
     * @param host
     * @param port
     */
    public HostInfo (String host, int port) {
        ArgCheck.isNotNull(host);
        this.hostName = host.toLowerCase();
        this.portNumber = port;
    }

    public InetAddress getInetAddress() throws UnknownHostException {
        if (inetAddress != null) {
            return inetAddress;
        }
        //only cache inetaddresses if they represent the ip.
        InetAddress addr = InetAddress.getByName(this.hostName);
        if (addr.getHostAddress().equalsIgnoreCase(this.hostName)) {
            this.inetAddress = addr;
        }
        return addr;
    }

    public String getHostName() {
        return hostName;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(hostName).append(":").append(portNumber); //$NON-NLS-1$
        return sb.toString();
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 4.2
     */
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof HostInfo)) {
            return false;
        }
        HostInfo hostInfo = (HostInfo) obj;
        if (portNumber != hostInfo.getPortNumber()) {
            return false;
        }
        if (ssl != hostInfo.ssl) {
            return false;
        }
        if (inetAddress != null && hostInfo.inetAddress != null) {
            return inetAddress.equals(hostInfo.inetAddress);
        }
        return hostName.equals(hostInfo.getHostName());
    }

    /**
     * @see java.lang.Object#hashCode()
     * @since 4.2
     */
    public int hashCode() {
        int hc = HashCodeUtil.hashCode(0, hostName);
        return HashCodeUtil.hashCode(hc, portNumber);
    }

    public boolean isResolved() {
        return this.inetAddress != null;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

}
