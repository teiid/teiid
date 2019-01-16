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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.servlet.ServletException;
import javax.xml.stream.XMLStreamException;

import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.core.OData4Impl;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.metadata.Schema;
import org.teiid.odata.api.Client;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.service.ODataSchemaBuilder.ODataSchemaInfo;
import org.teiid.olingo.service.ODataSchemaBuilder.SchemaResolver;

public class OlingoBridge {
    
    public static List<String> RESERVED = Arrays.asList("teiid", "edm", "core", "olingo-extensions"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    
    //the schema name is handled as case insensitive
    private ConcurrentSkipListMap<String, ODataHttpHandler> handlers = new ConcurrentSkipListMap<String, ODataHttpHandler>(String.CASE_INSENSITIVE_ORDER);
    
    public ODataHttpHandler getHandler(String baseUri, Client client, String schemaName) throws ServletException {
        ODataHttpHandler handler = this.handlers.get(schemaName);
        if (handler != null) {
            return handler;
        }
        VDBMetaData vdb = client.getVDB();
        
        org.teiid.metadata.Schema teiidSchema = client.getMetadataStore().getSchema(schemaName);
        if (teiidSchema == null || !isVisible(vdb, teiidSchema)) {
            throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16022));
        }
        
        synchronized (this) {
            handler = this.handlers.get(schemaName);
            if (handler != null) {
                return handler;
            }
            
            loadAllHandlers(baseUri, client, vdb);
        }
        return handlers.get(schemaName);
    }

    private void loadAllHandlers(String baseUri, Client client, VDBMetaData vdb)
            throws ServletException {
        final Map<String, ODataSchemaInfo> infoMap = new LinkedHashMap<>();
        
        Set<String> aliases = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        aliases.addAll(RESERVED);
        
        //process the base metadata structure
        for (Schema s : client.getMetadataStore().getSchemaList()) {
            if (!isVisible(vdb, s)) {
                 continue;   
            }
            
            //prevent collisions with the reserved, and then with any other schemas
            int i = 1;
            String alias = s.getName();
            while (!aliases.add(alias)) {
                alias = alias + "_" + i++; //$NON-NLS-1$
            }
            
            ODataSchemaInfo info = ODataSchemaBuilder.buildStructuralMetadata(vdb.getFullName(), s, alias);
            infoMap.put(s.getName(), info);
            try {
                info.edmProvider = new TeiidEdmProvider(baseUri, info.schema, 
                        client.getProperty(Client.INVALID_CHARACTER_REPLACEMENT));
            } catch (XMLStreamException e) {
                throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16054));
            }
        }
        
        //process navigation links
        for (Schema s : client.getMetadataStore().getSchemaList()) {
            if (!isVisible(vdb, s)) {
                continue;    
            }
            final ODataSchemaInfo info = infoMap.get(s.getName());
            ODataSchemaBuilder.buildNavigationProperties(s, info.entityTypes, info.entitySets, new SchemaResolver() {
                
                @Override
                public ODataSchemaInfo getSchemaInfo(String name) {
                    ODataSchemaInfo result = infoMap.get(name);
                    if (result != null && info != result) {
                        //add a bi-directional relationship
                        info.edmProvider.addReferenceSchema(
                                vdb.getFullName(), result.schema.getNamespace(), result.schema.getAlias(), result.edmProvider);
                        result.edmProvider.addReferenceSchema(
                                vdb.getFullName(), info.schema.getNamespace(), info.schema.getAlias(), info.edmProvider);
                    }
                    return result;
                }
            });
        }

        OData odata = OData4Impl.newInstance();

        for (Map.Entry<String, ODataSchemaInfo> entry : infoMap.entrySet()) {
            TeiidEdmProvider edmProvider = entry.getValue().edmProvider;
            ServiceMetadata metadata = odata.createServiceMetadata(edmProvider, edmProvider.getReferences());
            ODataHttpHandler handler = odata.createHandler(metadata);
            handler.register(new TeiidServiceHandler(entry.getKey()));
            this.handlers.put(entry.getKey(), handler);
        }
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
