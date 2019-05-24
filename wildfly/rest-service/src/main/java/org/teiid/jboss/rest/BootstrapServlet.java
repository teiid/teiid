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
package org.teiid.jboss.rest;

import io.swagger.jaxrs.config.BeanConfig;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class BootstrapServlet extends HttpServlet {

    private static final long serialVersionUID = 5704762873796188429L;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        BeanConfig beanConfig = new BeanConfig();
        init(beanConfig);
    }

    protected void init(BeanConfig beanConfig) {
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
        String str1 = req.getContextPath();
        String str2 = req.getScheme() + "://" + req.getServerName() + ":" + req.getServerPort() + str1 + "/"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        String str3 = str1 + "/api.html"; //$NON-NLS-1$
        String str4 = "swagger.json"; //$NON-NLS-1$
        String str5 = "/url=" + str2 + str4; //$NON-NLS-1$
        String str6 = str3 + "?" + str5; //$NON-NLS-1$
        resp.sendRedirect(str6);
    }

}
