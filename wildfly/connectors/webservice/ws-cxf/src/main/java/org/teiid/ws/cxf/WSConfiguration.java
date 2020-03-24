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

public interface WSConfiguration {

    static final String DEFAULT_NAMESPACE_URI = "http://teiid.org"; //$NON-NLS-1$

    static final String DEFAULT_LOCAL_NAME = "teiid"; //$NON-NLS-1$

    public enum SecurityType {None,HTTPBasic,Digest,WSSecurity,Kerberos,OAuth}

    String getNamespaceUri();

    String getServiceName();

    String getEndPointName();

    String getAuthPassword();

    String getAuthUserName();

    String getEndPoint();

    Long getRequestTimeout();

    default SecurityType getAsSecurityType() {
        String securityType = getSecurityType();
        if (securityType == null) {
            return SecurityType.None;
        }
        return SecurityType.valueOf(securityType);
    }

    String getSecurityType();

    String getConfigFile();

    String getConfigName();

    String getWsdl();

    Long getConnectTimeout();

}