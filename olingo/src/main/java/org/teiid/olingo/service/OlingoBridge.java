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
