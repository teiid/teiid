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
 *
 * <p>
 * If client accepts GZIP encoding, then it will replace output stream
 * with one which deflates output.
 *
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
