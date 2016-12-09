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
package org.teiid.olingo.web;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.olingo.ODataPlugin;

@SuppressWarnings("serial")
/**
 * static servlet is used for serving static documents, especially annotation documents.
 */
public class StaticContentServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        String pathInfo = request.getPathInfo();
        
        try {
            if (pathInfo.endsWith(".xml") //$NON-NLS-1$
                    && !pathInfo.endsWith("pom.xml") //$NON-NLS-1$
                    && !pathInfo.contains("META-INF") //$NON-NLS-1$
                    && !pathInfo.contains("WEB-INF") //$NON-NLS-1$
                    && !pathInfo.substring(1).contains("/")) { //$NON-NLS-1$
                InputStream contents = getClass().getResourceAsStream(pathInfo);
                if (contents != null) {
	                writeContent(response, contents);
	                response.flushBuffer();
	                return;
                }
            }
            throw new TeiidProcessingException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16055, pathInfo));
        } catch (TeiidProcessingException e) {
            ODataFilter.writeError(request, e, response, 404);
        }
    }
    
    private void writeContent(HttpServletResponse response, InputStream contents) throws IOException {
        ObjectConverterUtil.write(response.getOutputStream(), contents, -1);
    }    
}
