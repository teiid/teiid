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
package org.teiid.olingo;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.edm.provider.Schema;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidRuntimeException;

@SuppressWarnings("serial")
public class ODataServlet extends HttpServlet {

    @Override
    public void service(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        Client client = (Client) request.getAttribute(Client.class.getName());
        String schemaName = (String) request
                .getAttribute(ODataFilter.SCHEMA_NAME);

        org.teiid.metadata.Schema teiidSchema = client.getMetadataStore()
                .getSchema(schemaName);
        if (teiidSchema == null || !isVisible(client.getVDB(), teiidSchema)) {
            // TODO: send error response, this is exception
            throw new TeiidRuntimeException(
                    ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16022));
        }

        OData odata = OData.newInstance();
        Schema schema = OData4EntitySchemaBuilder.buildMetadata(teiidSchema);
        Edm edm = odata.createEdm(new TeiidEdmProvider(client
                .getMetadataStore(), schema));

        ODataHttpHandler handler = odata.createHandler(edm);
        boolean prepared = true;
        if (request.getParameter("prepared") != null) { //$NON-NLS-1$
            prepared = Boolean.getBoolean(request.getParameter("prepared")); //$NON-NLS-1$
        }
        handler.register(new TeiidProcessor(client, prepared));
        handler.process(request, response);
    }

    private static boolean isVisible(VDBMetaData vdb,
            org.teiid.metadata.Schema schema) {
        String schemaName = schema.getName();
        Model model = vdb.getModel(schemaName);
        if (model == null) {
            return true;
        }
        return model.isVisible();
    }
}
