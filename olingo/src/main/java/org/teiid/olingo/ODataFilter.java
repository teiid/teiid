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
package org.teiid.olingo;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.LRUCache;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.transport.LocalServerConnection;
import org.teiid.vdb.runtime.VDBKey;

public class ODataFilter implements Filter, VDBLifeCycleListener {

	protected String proxyBaseURI;
	protected Properties initProperties;
	protected Map<VDBKey, SoftReference<Client>> clientMap = Collections.synchronizedMap(new LRUCache<VDBKey, SoftReference<Client>>());
	private volatile boolean listenerRegistered = false;
	
	@Override
	public void init(FilterConfig config) throws ServletException {
		// handle proxy-uri in the case of cloud environments
		String proxyURI = config.getInitParameter("proxy-base-uri"); //$NON-NLS-1$
		if (proxyURI != null && proxyURI.startsWith("${") && proxyURI.endsWith("}")) { //$NON-NLS-1$ //$NON-NLS-2$
			proxyURI = proxyURI.substring(2, proxyURI.length()-1);
			proxyURI = System.getProperty(proxyURI);
		}
		
		if (proxyURI != null) {
			this.proxyBaseURI = proxyURI;
		}
		
		Properties props = new Properties();
		Enumeration<String> names = config.getInitParameterNames();
		while(names.hasMoreElements()) {
			String name = names.nextElement();
			props.setProperty(name, config.getInitParameter(name));
		}
		this.initProperties = props;
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response,
			FilterChain chain) throws IOException, ServletException {
		
		HttpServletRequest httpRequest = (HttpServletRequest)request;
		
		String proxyURI = this.proxyBaseURI; 
		if (proxyURI != null) {
			httpRequest = new ProxyHttpServletRequest(httpRequest, proxyURI);
		}

		VDBKey key = null;
		String vdbName = null;
		int version = 1;
		
		String uri = ((HttpServletRequest)request).getRequestURL().toString();
		int idx = uri.indexOf("/odata4/"); //$NON-NLS-1$
		if (idx != -1) {
			String contextPath = httpRequest.getContextPath();
			if (contextPath == null) {
				contextPath = "/odata4"; //$NON-NLS-1$
			}
			
			int endIdx = uri.indexOf('/', idx+8);
			if (endIdx == -1) {
				vdbName = uri.substring(idx+8);
			}
			else {
				vdbName = uri.substring(idx+8, endIdx);
			}
			
			contextPath = contextPath+"/"+vdbName; //$NON-NLS-1$
			
			int versionIdx = vdbName.indexOf('.');
			if (versionIdx != -1) {
				version = Integer.parseInt(vdbName.substring(versionIdx+1));
				vdbName = vdbName.substring(0, versionIdx);
			}
			
			vdbName = vdbName.trim();
			if (vdbName.isEmpty()) {
				throw new TeiidRuntimeException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16008));
			}
			ContextAwareHttpSerlvetRequest contextAwareRequest = new ContextAwareHttpSerlvetRequest(httpRequest);
			contextAwareRequest.setContextPath(contextPath);
			httpRequest = contextAwareRequest;
			key = new VDBKey(vdbName, version);
		}
		else {
			if (this.initProperties.getProperty("vdb-name") == null || this.initProperties.getProperty("vdb-version") == null) { //$NON-NLS-1$ //$NON-NLS-2$
				throw new ServletException("Must configure VDB name and version to proceed");
			}
			vdbName = this.initProperties.getProperty("vdb-name"); //$NON-NLS-1$
			version = Integer.parseInt(this.initProperties.getProperty("vdb-version")); //$NON-NLS-1$
		}
		
		SoftReference<Client> ref = this.clientMap.get(key);
		Client client = null;
		if (ref != null) {
			client = ref.get();
		}
		if (client == null) {
			client = buildClient(vdbName, version, this.initProperties);
			if (!this.listenerRegistered) {
				synchronized(this) {
					if (!this.listenerRegistered) {
						ConnectionImpl connection = null;
						if (client instanceof LocalClient) {
							try {
								connection = ((LocalClient)client).getConnection();
								LocalServerConnection lsc = (LocalServerConnection)connection.getServerConnection();
								lsc.addListener(this);
								this.listenerRegistered = true;
							} catch (SQLException e) {
								LogManager.logWarning(LogConstants.CTX_ODATA, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16014)); 
							} finally {
								if (connection != null) {
									try {
										connection.close();
									} catch (SQLException e) {
									}
								}
							}
						}
					}
				}
			}
			ref = new SoftReference<Client>(client);
			this.clientMap.put(key, ref);
		}	
		httpRequest.setAttribute(Client.class.getName(), client);
		chain.doFilter(httpRequest, response);
	}

	public Client buildClient(String vdbName, int version, Properties props) {
		return new LocalClient(vdbName, version, props);
	}

	@Override
	public void destroy() {
		this.clientMap.clear();
	}
	
	@Override
	public void removed(String name, int version, CompositeVDB vdb) {
		this.clientMap.remove(new VDBKey(name, version));
	}
	
	@Override
	public void finishedDeployment(String name, int version, CompositeVDB vdb,boolean reloading) {
		this.clientMap.remove(new VDBKey(name, version));		
	}
	
	@Override
	public void beforeRemove(String name, int version, CompositeVDB vdb) {
	}
	
	@Override
	public void added(String name, int version, CompositeVDB vdb,boolean reloading) {
	}
}
