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
            if (pathInfo.endsWith("org.apache.olingo.v1.xml") || pathInfo.endsWith("org.teiid.v1.xml") //$NON-NLS-1$ //$NON-NLS-2$
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
