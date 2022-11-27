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
package org.teiid.ws.cxf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.cxf.ws.security.trust.STSClient;

public class WSSecurityCredential implements Serializable {
    private static final long serialVersionUID = 7725328159246858176L;
    private boolean useSts;
    private String stsWsdlLocation;
    private QName stsService;
    private QName stsPort;

    public enum SecurityHandler {WSPOLICY, WSS4J};

    private HashMap<String, Object> requestProps = new HashMap<String, Object>();
    private HashMap<String, Object> responseProps = new HashMap<String, Object>();
    private HashMap<String, Object> stsProps = new HashMap<String, Object>();
    private SecurityHandler securityHandler = SecurityHandler.WSS4J;

    public SecurityHandler getSecurityHandler() {
        return this.securityHandler;
    }

    public void setSecurityHandler(SecurityHandler securityHandler) {
        this.securityHandler = securityHandler;
    }

    public HashMap<String, Object> getRequestPropterties() {
        return this.requestProps;
    }

    public HashMap<String, Object> getResponsePropterties() {
        return this.responseProps;
    }

    public HashMap<String, Object> getStsPropterties() {
        return this.stsProps;
    }

    /**
     * This is configuration for WS-Trust STSClient.
     *
     * @param stsWsdlLocation
     * @param stsService
     * @param stsPort
     */
    public void setSTSClient(String stsWsdlLocation, QName stsService,
            QName stsPort) {
        this.stsWsdlLocation = stsWsdlLocation;
        this.stsService = stsService;
        this.stsPort = stsPort;
        this.useSts = true;
    }

    public STSClient buildStsClient(org.apache.cxf.Bus bus) {
        STSClient stsClient = new STSClient(bus);
        stsClient.setWsdlLocation(this.stsWsdlLocation);
        stsClient.setServiceQName(this.stsService);
        stsClient.setEndpointQName(this.stsPort);
        Map<String, Object> props = stsClient.getProperties();
        props.putAll(this.stsProps);
        return stsClient;
    }

    public boolean useSts() {
        return this.useSts;
    }
}
