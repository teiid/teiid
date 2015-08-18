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
package org.teiid.swagger.rest;

import java.io.IOException;

import io.swagger.jaxrs.config.BeanConfig;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class BootstrapServlet extends HttpServlet{

    private static final long serialVersionUID = 8320267972392260667L;
    
    static final String BASEURL = "";

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init();
        
      //TODO-- read from servlet parameter
        BeanConfig beanConfig = new BeanConfig();
        beanConfig.setTitle("Jaxrs Examples Petstore");
        beanConfig.setVersion("1.0.0");
        beanConfig.setSchemes(new String[]{"http"});
        beanConfig.setBasePath(BASEURL);
        beanConfig.setResourcePackage("org.teiid");
        beanConfig.setScan(true);
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String location = buildLocation(req, resp);
        resp.sendRedirect(location);
    }

    private String buildLocation(HttpServletRequest request, HttpServletResponse resp) {
        String path = request.getContextPath();
        String basePath = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort() + path + "/";
//        System.out.println(path);
//        System.out.println(basePath);
        String base = path + "/teiid.html";
        String restPrefix = "rest";
        String swagger = "/swagger.json";
        String param = "/url=" + basePath + restPrefix + swagger;
        return base + "?" + param;
    }

}
