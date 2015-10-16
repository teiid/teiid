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

import java.util.Collections;
import java.util.HashMap;

import javax.servlet.ServletException;

import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.edmx.EdmxReference;
import org.apache.olingo.server.core.OData4Impl;
import org.apache.olingo.server.core.SchemaBasedEdmProvider;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.odata.api.Client;
import org.teiid.olingo.ODataPlugin;

public class OlingoBridge {
    
    private HashMap<String, ODataHttpHandler> handlers = new HashMap<String, ODataHttpHandler>();
    
    public ODataHttpHandler getHandler(Client client, String schemaName) throws ServletException {
        if (this.handlers.get(schemaName) == null) {
            org.teiid.metadata.Schema teiidSchema = client.getMetadataStore().getSchema(schemaName);
            if (teiidSchema == null || !isVisible(client.getVDB(), teiidSchema)) {
                throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16022));
            }
    
            OData odata = OData4Impl.newInstance();
            VDBMetaData vdb = client.getVDB();
            CsdlSchema schema = ODataSchemaBuilder.buildMetadata(vdb.getFullName(), teiidSchema);
            SchemaBasedEdmProvider edmProvider = new SchemaBasedEdmProvider();
            edmProvider.addSchema(schema);
            ServiceMetadata metadata = odata.createServiceMetadata(edmProvider, Collections.<EdmxReference> emptyList());
            ODataHttpHandler handler = odata.createHandler(metadata);
            
            handler.register(new TeiidServiceHandler(schemaName));
            this.handlers.put(schemaName, handler);
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
