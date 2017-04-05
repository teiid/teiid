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
package org.teiid.olingo.web.gzip;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Filter for reading/writing messages in GZIP format.
 * <p>
 * If request body is in GZIP format, then it will replace input stream
 * with one which inflates input.
 * </p>
 * <p>
 * If client accepts GZIP encoding, then it will replace output stream
 * with one which deflates output.
 * </p>
 */
public class GzipFilter implements Filter{

    @Override
    public void init(FilterConfig filterConfig) throws ServletException{}

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException{
        HttpServletRequest req = (HttpServletRequest)request;
        HttpServletResponse res = (HttpServletResponse)response;
        if("gzip".equalsIgnoreCase(req.getHeader("Content-Encoding"))){
            req = new GzipMessageRequest(req);
        }
        if(String.valueOf(req.getHeader("Accept-Encoding")).toLowerCase().contains("gzip")){
            res = new GzipMessageResponse(res);
            res.setHeader("Content-Encoding", "gzip");
        }
        chain.doFilter(req, res);
    }

    @Override
    public void destroy(){}
}
