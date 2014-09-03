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

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ServletBootstrap;

@SuppressWarnings("serial")
public class ODataServlet extends HttpServletDispatcher {

	private ODataServletContainerDispatcher dispatcher;
	private ServletBootstrap bootstrap;
	private boolean changeRoot = true;
	
	@Override
	public void init(ServletConfig servletConfig) throws ServletException {
		super.init(servletConfig);
		this.dispatcher = new ODataServletContainerDispatcher();
		this.servletContainerDispatcher = dispatcher;
		this.bootstrap = new ServletBootstrap(servletConfig);
		this.servletContainerDispatcher.init(servletConfig.getServletContext(),bootstrap, this, this);
		this.servletContainerDispatcher.getDispatcher().getDefaultContextObjects().put(ServletConfig.class, servletConfig);
		this.changeRoot = (servletConfig.getServletContext().getInitParameter("allow-vdb") == null); //$NON-NLS-1$
	}

	public void service(String httpMethod, HttpServletRequest request, HttpServletResponse response) throws IOException {
		if (this.changeRoot) {
			String path = request.getPathInfo();
			int idx = path.indexOf('/', 1);
			if (idx != -1) {
				String vdb = path.substring(1, idx);
				this.dispatcher.setServletMapping(vdb);
			}
			else {
				String vdb = path.substring(1);
				this.dispatcher.setServletMapping(vdb);			
			}
		}
		this.dispatcher.service(httpMethod, request, response, true);
	}
}
