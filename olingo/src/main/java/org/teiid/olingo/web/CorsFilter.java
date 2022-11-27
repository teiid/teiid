/*
 * Copyright 2015 JBoss Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.olingo.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.teiid.core.TeiidProcessingException;
import org.teiid.olingo.ODataPlugin;

/**
 * A simple CORS filter for apiman.
 *
 * @author eric.wittmann@redhat.com
 */
public class CorsFilter implements Filter {

    /**
     * Constructor.
     */
    public CorsFilter() {
    }

    /**
     * @see javax.servlet.Filter#init(javax.servlet.FilterConfig)
     */
    @Override
    public void init(FilterConfig config) throws ServletException {
    }

    /**
     * @see javax.servlet.Filter#doFilter(javax.servlet.ServletRequest, javax.servlet.ServletResponse, javax.servlet.FilterChain)
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException {
        HttpServletRequest httpReq = (HttpServletRequest) request;
        HttpServletResponse httpResp = (HttpServletResponse) response;

        if (isOptionsMethod(httpReq) && !hasOriginHeader(httpReq)) {
            ODataFilter.writeError(request,
                    new TeiidProcessingException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16056)),
                    httpResp, 400);
            return;
        }

        if (isPreflightRequest(httpReq)) {
            httpResp.setHeader("Access-Control-Allow-Origin", httpReq.getHeader("Origin")); //$NON-NLS-1$ //$NON-NLS-2$
            httpResp.setHeader("Access-Control-Allow-Credentials", "true"); //$NON-NLS-1$ //$NON-NLS-2$
            httpResp.setHeader("Access-Control-Max-Age", "1800"); //$NON-NLS-1$ //$NON-NLS-2$
            httpResp.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE"); //$NON-NLS-1$ //$NON-NLS-2$
            httpResp.setHeader("Access-Control-Allow-Headers", "Content-Type,Accept,Origin,Authorization"); //$NON-NLS-1$ //$NON-NLS-2$
            httpResp.setStatus(204);
        } else {
            if (hasOriginHeader(httpReq)) {
                httpResp.setHeader("Access-Control-Allow-Origin", httpReq.getHeader("Origin")); //$NON-NLS-1$ //$NON-NLS-2$
                httpResp.setHeader("Access-Control-Allow-Credentials", "true"); //$NON-NLS-1$ //$NON-NLS-2$
            }
            chain.doFilter(httpReq, httpResp);
        }
    }

    /**
     * Determines whether the request is a CORS preflight request.
     * @param httpReq the http servlet request
     * @return true if preflight, else false
     */
    private boolean isPreflightRequest(HttpServletRequest httpReq) {
        return isOptionsMethod(httpReq) && hasOriginHeader(httpReq);
    }

    /**
     * Returns true if it's an OPTIONS http request.
     * @param httpReq the http servlet request
     * @return true if options method, else false
     */
    static boolean isOptionsMethod(HttpServletRequest httpReq) {
        return "OPTIONS".equals(httpReq.getMethod()); //$NON-NLS-1$
    }

    /**
     * Returns true if the Origin request header is present.
     * @param httpReq the http servlet request
     * @return true if has origin header, else false
     */
    static boolean hasOriginHeader(HttpServletRequest httpReq) {
        String origin = httpReq.getHeader("Origin"); //$NON-NLS-1$
        return origin != null && origin.trim().length() > 0;
    }

    /**
     * @see javax.servlet.Filter#destroy()
     */
    @Override
    public void destroy() {
    }

}
