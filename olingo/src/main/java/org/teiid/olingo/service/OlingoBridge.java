/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.olingo.service;

import java.util.LinkedHashMap;
import java.util.Map;
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

    private void loadAllHandlers(String baseUri, Client client, final VDBMetaData vdb)
            throws ServletException {
        final Map<String, ODataSchemaInfo> infoMap = new LinkedHashMap<String, ODataSchemaInfo>();
        
        //process the base metadata structure
        for (Schema s : client.getMetadataStore().getSchemaList()) {
            if (!isVisible(vdb, s)) {
                 continue;   
            }
            ODataSchemaInfo info = ODataSchemaBuilder.buildStructuralMetadata(vdb.getFullName(), s);
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
                    if (result != null) {
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
