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
package org.teiid.resource.adapter.salesforce.transport;

import java.util.HashMap;

import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.MessageHandler;

public class SalesforceConnectorConfig extends ConnectorConfig {
    private String cxfConfigFile;
    private HashMap<String, Object> credentialMap = new HashMap<String, Object>();

    public String getCxfConfigFile() {
        return cxfConfigFile;
    }

    public void setCxfConfigFile(String cxfConfigFile) {
        this.cxfConfigFile = cxfConfigFile;
    }

    public Object getCredential(String name) {
        return this.credentialMap.get(name);
    }

    public void setCredential(String name, Object credential) {
        this.credentialMap.put(name, credential);
    }

    @Override
    public void addMessageHandler(MessageHandler handler) {
        throw new UnsupportedOperationException();
    }
}
