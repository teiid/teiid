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
