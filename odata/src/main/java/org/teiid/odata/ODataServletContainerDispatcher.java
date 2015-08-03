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
package org.teiid.odata;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.SecurityContext;

import org.jboss.resteasy.core.SynchronousDispatcher;
import org.jboss.resteasy.core.ThreadLocalResteasyProviderFactory;
import org.jboss.resteasy.logging.Logger;
import org.jboss.resteasy.plugins.server.servlet.ConfigurationBootstrap;
import org.jboss.resteasy.plugins.server.servlet.HttpRequestFactory;
import org.jboss.resteasy.plugins.server.servlet.HttpResponseFactory;
import org.jboss.resteasy.plugins.server.servlet.ServletContainerDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ServletSecurityContext;
import org.jboss.resteasy.plugins.server.servlet.ServletUtil;
import org.jboss.resteasy.specimpl.UriInfoImpl;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponse;
import org.jboss.resteasy.spi.NotFoundException;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

public class ODataServletContainerDispatcher extends ServletContainerDispatcher {
	private final static Logger logger = Logger.getLogger(ServletContainerDispatcher.class);
	ThreadLocal<String> servletMapping;
	ThreadLocal<String> proxyBaseURI;
	
	public ODataServletContainerDispatcher() {
		super();
		this.servletMapping = new ThreadLocal<String>();
		this.proxyBaseURI = new ThreadLocal<String>();
	}
	
	@Override
	public void init(ServletContext servletContext,
			ConfigurationBootstrap bootstrap,
			HttpRequestFactory requestFactory,
			HttpResponseFactory responseFactory) throws ServletException {
		super.init(servletContext, bootstrap, requestFactory, responseFactory);
		this.servletMapping.set(bootstrap.getParameter("resteasy.servlet.mapping.prefix"));
		
		String proxyURI = bootstrap.getParameter("proxy-base-uri");
		if (proxyURI != null && proxyURI.startsWith("${") && proxyURI.endsWith("}")) {
			proxyURI = proxyURI.substring(2, proxyURI.length()-1);
			proxyURI = System.getProperty(proxyURI);
		}
		this.proxyBaseURI.set(proxyURI);
	}
	
	
	@Override
	/**
	 * This code is copy taken from resteasy project as is, with a minor modification.
	 * @see org.jboss.resteasy.plugins.server.servlet.ServletContainerDispatcher#service(java.lang.String, javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse, boolean)
	 */
	public void service(String httpMethod, HttpServletRequest request, HttpServletResponse response, boolean handleNotFound) throws IOException, NotFoundException {
		try {
			// logger.info("***PATH: " + request.getRequestURL());
			// classloader/deployment aware RestasyProviderFactory. Used to have
			// request specific
			// ResteasyProviderFactory.getInstance()
			ResteasyProviderFactory defaultInstance = ResteasyProviderFactory.getInstance();
			if (defaultInstance instanceof ThreadLocalResteasyProviderFactory) {
				ThreadLocalResteasyProviderFactory.push(providerFactory);
			}
			HttpHeaders headers = null;
			UriInfoImpl uriInfo = null;
			try {
				String proxyURI = this.proxyBaseURI.get(); 
				if (proxyURI != null) {
					request = new ProxyHttpServletRequest(request, proxyURI);
				}
				
				headers = ServletUtil.extractHttpHeaders(request);
				uriInfo = ServletUtil.extractUriInfo(request, servletMapping.get());
				
			} catch (Exception e) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST);
				// made it warn so that people can filter this.
				logger.warn("Failed to parse request.", e);
				return;
			}

			HttpResponse theResponse = responseFactory.createResteasyHttpResponse(response);
			HttpRequest in = requestFactory.createResteasyHttpRequest(httpMethod, request, headers, uriInfo, theResponse,response);

			try {
				ResteasyProviderFactory.pushContext(HttpServletRequest.class,request);
				ResteasyProviderFactory.pushContext(HttpServletResponse.class,response);

				ResteasyProviderFactory.pushContext(SecurityContext.class,new ServletSecurityContext(request));
				if (handleNotFound) {
					dispatcher.invoke(in, theResponse);
				} else {
					((SynchronousDispatcher) dispatcher).invokePropagateNotFound(in, theResponse);
				}
			} finally {
				ResteasyProviderFactory.clearContextData();
			}
		} finally {
			ResteasyProviderFactory defaultInstance = ResteasyProviderFactory.getInstance();
			if (defaultInstance instanceof ThreadLocalResteasyProviderFactory) {
				ThreadLocalResteasyProviderFactory.pop();
			}
		}
	}

	public void setServletMapping(String mapping) {
		this.servletMapping.set(mapping);
	}
}
