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
package org.teiid.olingo.service;

import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.xml.stream.XMLStreamException;

import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.ex.ODataException;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.core.OData4Impl;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.odata.api.Client;
import org.teiid.olingo.ODataPlugin;

public class OlingoBridge {
    
    private ConcurrentHashMap<String, ODataHttpHandler> handlers = new ConcurrentHashMap<String, ODataHttpHandler>();
    
    public ODataHttpHandler getHandler(String baseUri, Client client, String schemaName) throws ServletException {
        if (this.handlers.get(schemaName) == null) {
            org.teiid.metadata.Schema teiidSchema = client.getMetadataStore().getSchema(schemaName);
            if (teiidSchema == null || !isVisible(client.getVDB(), teiidSchema)) {
                throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16022));
            }
    
            try {
                OData odata = OData4Impl.newInstance();
                VDBMetaData vdb = client.getVDB();
                CsdlSchema schema = ODataSchemaBuilder.buildMetadata(vdb.getFullName(), teiidSchema);
                TeiidEdmProvider edmProvider = new TeiidEdmProvider(baseUri, schema, 
                        client.getProperty(Client.INVALID_CHARACTER_REPLACEMENT));
                ServiceMetadata metadata = odata.createServiceMetadata(edmProvider, edmProvider.getReferences());
                ODataHttpHandler handler = odata.createHandler(metadata);
                
                handler.register(new TeiidServiceHandler(schemaName));
                this.handlers.put(schemaName, handler);
            } catch (XMLStreamException e) {
                throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16054));
            } catch (ODataException e) {
                throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16054));
            }
        }
        return this.handlers.get(schemaName);
    }
    
    private static boolean isVisible(VDBMetaData vdb, org.teiid.metadata.Schema schema) {
        String schemaName = schema.getName();
        Model model = vdb.getModel(schemaName);
        if (model == null) {
            return true;
        }
        return model.isVisible();
    }    
}
