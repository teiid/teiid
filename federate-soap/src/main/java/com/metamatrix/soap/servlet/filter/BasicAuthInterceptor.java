/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.soap.servlet.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



/**
 * This servlet filter checks to make sure we have a authentication header. This is needed when using HTTP Basic to circumvent
 * preemptive authentication and avoids the requirement of a realm configuration. It essentially serves the role of a passthrough realm.
 */
public class BasicAuthInterceptor implements
                              Filter {
	public final static String WWW_AUTHENTICATE_HEADER = "WWW-Authenticate"; //$NON-NLS-1$
	public final static String HTTP_AUTHORIZATION_HEADER = "Authorization"; //$NON-NLS-1$
	public final static String BOGUS_REALM_VALUE = "Basic Realm=\"MetaMatrix REALM\""; //$NON-NLS-1$
   
    /**
     * @see javax.servlet.Filter#doFilter(javax.servlet.ServletRequest, javax.servlet.ServletResponse, javax.servlet.FilterChain)
     */
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain) throws IOException, ServletException {
    	
//    	 Acquiring Authorization Header from servlet request
    	String basicAuthenticationString = ((HttpServletRequest)request).getHeader(HTTP_AUTHORIZATION_HEADER); //$NON-NLS-1$
		if (basicAuthenticationString==null){
			((HttpServletResponse)response).addHeader(WWW_AUTHENTICATE_HEADER, BOGUS_REALM_VALUE);
			((HttpServletResponse)response).sendError(HttpServletResponse.SC_UNAUTHORIZED);
		}else{
	        // Deliver request to next filter, if applicable
	        chain.doFilter(request, response);
		}		
    }

	public void destroy() {
		
	}

	public void init(FilterConfig arg0) throws ServletException {
		
	}

 
}
