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
package org.teiid.transport;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class SocketConfiguration {

    private int outputBufferSize;
    private int inputBufferSize;
    private int maxSocketThreads;
    private int portNumber;
    private InetAddress hostAddress;
    private SSLConfiguration sslConfiguration;
    private String hostName;
    private String name;
    private WireProtocol protocol = WireProtocol.teiid;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setBindAddress(String addr) {
        this.hostName = addr;
    }

    public void setPortNumber(int port) {
        this.portNumber = port;
    }

    public void setMaxSocketThreads(int value) {
        this.maxSocketThreads = value;
    }

    public void setInputBufferSize(int value) {
        this.inputBufferSize = value;
    }

    public void setOutputBufferSize(int value) {
        this.outputBufferSize = value;
    }

    public void setSSLConfiguration(SSLConfiguration value) {
        this.sslConfiguration = value;
    }

    public int getOutputBufferSize() {
        return outputBufferSize;
    }

    public int getInputBufferSize() {
        return inputBufferSize;
    }

    public int getMaxSocketThreads() {
        return maxSocketThreads;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public InetAddress getHostAddress() {
        return hostAddress;
    }

    public InetAddress getResolvedHostAddress() throws UnknownHostException {
        if (this.hostAddress != null) {
            return hostAddress;
        }
        // if not defined then see if can bind to local address; if supplied resolve it by name
        if (this.hostName == null) {
            this.hostName = InetAddress.getLocalHost().getHostName();
        }
        //only cache inetaddresses if they represent the ip.
        InetAddress addr = InetAddress.getByName(this.hostName);
        if (addr.getHostAddress().equalsIgnoreCase(this.hostName)) {
            this.hostAddress = addr;
        }
        return addr;
    }

    public void setHostAddress(InetAddress hostAddress) {
        this.hostAddress = hostAddress;
        this.hostName = hostAddress.getHostName();
    }

    public String getHostName() {
        return this.hostName;
    }

    public SSLConfiguration getSSLConfiguration() {
        return sslConfiguration;
    }

    public boolean getSslEnabled() {
        return this.sslConfiguration != null && this.sslConfiguration.isSslEnabled();
    }

    public WireProtocol getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = WireProtocol.valueOf(protocol);
    }

    public void setProtocol(WireProtocol protocol) {
        this.protocol = protocol;
    }
}
