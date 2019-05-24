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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.olingo.server.api.ODataHttpHandler;
import org.teiid.odata.api.Client;
import org.teiid.olingo.service.TeiidServiceHandler;

@SuppressWarnings("serial")
public class ODataServlet extends HttpServlet {
    @Override
    public void service(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ODataHttpHandler handler = (ODataHttpHandler) request.getAttribute(ODataHttpHandler.class.getName());
        Client client = (Client) request.getAttribute(Client.class.getName());
        try {
            TeiidServiceHandler.setClient(client);
            handler.process(request, response);
        } finally {
            TeiidServiceHandler.setClient(null);
        }
    }
}
