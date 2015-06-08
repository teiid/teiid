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
package org.teiid.olingo.web;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.sql.Connection;
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

import org.apache.olingo.server.api.ODataHttpHandler;
import org.teiid.core.util.LRUCache;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.odata.api.Client;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.service.LocalClient;
import org.teiid.olingo.service.OlingoBridge;
import org.teiid.transport.LocalServerConnection;
import org.teiid.vdb.runtime.VDBKey;

public class ODataFilter implements Filter, VDBLifeCycleListener {

    protected String proxyBaseURI;
    protected Properties initProperties;
    protected Map<VDBKey, SoftReference<OlingoBridge>> contextMap = Collections
            .synchronizedMap(new LRUCache<VDBKey, SoftReference<OlingoBridge>>());
    private volatile boolean listenerRegistered = false;
    
    @Override
    public void init(FilterConfig config) throws ServletException {
        // handle proxy-uri in the case of cloud environments
        String proxyURI = config.getInitParameter("proxy-base-uri"); //$NON-NLS-1$
        if (proxyURI != null && proxyURI.startsWith("${") && proxyURI.endsWith("}")) { //$NON-NLS-1$ //$NON-NLS-2$
            proxyURI = proxyURI.substring(2, proxyURI.length() - 1);
            proxyURI = System.getProperty(proxyURI);
        }

        if (proxyURI != null) {
            this.proxyBaseURI = proxyURI;
        }

        Properties props = new Properties();
        Enumeration<String> names = config.getInitParameterNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            props.setProperty(name, config.getInitParameter(name));
        }
        this.initProperties = props;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;

        String proxyURI = this.proxyBaseURI;
        if (proxyURI != null) {
            httpRequest = new ProxyHttpServletRequest(httpRequest, proxyURI);
        }

        VDBKey key = null;
        String vdbName = null;
        int version = 1;
        String modelName = null;

        String uri = ((HttpServletRequest) request).getRequestURL().toString();
        int idx = uri.indexOf("/odata4/"); //$NON-NLS-1$
        if (idx != -1) {
            String contextPath = httpRequest.getContextPath();
            if (contextPath == null) {
                contextPath = "/odata4"; //$NON-NLS-1$
            }

            int endIdx = uri.indexOf('/', idx + 8);
            if (endIdx == -1) {
                throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16020));
            }

            vdbName = uri.substring(idx + 8, endIdx);
            int modelIdx = uri.indexOf('/', endIdx + 1);
            if (modelIdx == -1) {
                modelName = uri.substring(endIdx + 1).trim();
                if (modelName.isEmpty()) {
                    throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16019));
                }
            } else {
                modelName = uri.substring(endIdx + 1, modelIdx);
            }

            contextPath = contextPath + "/" + vdbName + "/" + modelName; //$NON-NLS-1$ //$NON-NLS-2$

            int versionIdx = vdbName.indexOf('.');
            if (versionIdx != -1) {
                version = Integer.parseInt(vdbName.substring(versionIdx + 1));
                vdbName = vdbName.substring(0, versionIdx);
            }

            vdbName = vdbName.trim();
            if (vdbName.isEmpty()) {
                throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16008));
            }

            ContextAwareHttpSerlvetRequest contextAwareRequest = new ContextAwareHttpSerlvetRequest(httpRequest);
            contextAwareRequest.setContextPath(contextPath);
            httpRequest = contextAwareRequest;
        } else {
            if (this.initProperties.getProperty("vdb-name") == null || 
                    this.initProperties.getProperty("vdb-version") == null) { //$NON-NLS-1$ //$NON-NLS-2$
                throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16018));
            }
            vdbName = this.initProperties.getProperty("vdb-name"); //$NON-NLS-1$
            version = Integer.parseInt(this.initProperties.getProperty("vdb-version")); //$NON-NLS-1$
            int modelIdx = uri.indexOf('/', uri.indexOf('/'));
            if (modelIdx == -1) {
                modelName = uri.substring(uri.indexOf('/') + 1).trim();
                if (modelName.isEmpty()) {
                    throw new ServletException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16021));
                }
            }
            modelName = uri.substring(uri.indexOf('/'), uri.indexOf('/', uri.indexOf('/')));
        }
        
        key = new VDBKey(vdbName, version);
        
        SoftReference<OlingoBridge> ref = this.contextMap.get(key);
        OlingoBridge context = null;
        if (ref != null) {
            context = ref.get();
        }
        
        if (context == null) {
            context = new OlingoBridge();
            ref = new SoftReference<OlingoBridge>(context);
            this.contextMap.put(key, ref);
        }
        
        Client client = buildClient(vdbName, version, this.initProperties);
        try {
            Connection connection = client.open();
            registerVDBListener(client, connection);
            ODataHttpHandler handler = context.getHandler(client, modelName);
            httpRequest.setAttribute(ODataHttpHandler.class.getName(), handler);
            httpRequest.setAttribute(Client.class.getName(), client);
            chain.doFilter(httpRequest, response);
        } catch(SQLException e) {
            throw new ServletException(e);
        } finally {
            try {
                client.close();
            } catch (SQLException e) {
                //ignore
            }
        }
    }
    
    private void registerVDBListener(Client client, Connection conn) {
        if (!this.listenerRegistered) {
            synchronized (this) {
                if (!this.listenerRegistered) {
                    if (client instanceof LocalClient) {
                        try {
                            ConnectionImpl connection = (ConnectionImpl)conn;
                            LocalServerConnection lsc = (LocalServerConnection) connection.getServerConnection();
                            lsc.addListener(this);
                            this.listenerRegistered = true;
                        } catch (SQLException e) {
                            LogManager.logWarning(LogConstants.CTX_ODATA, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16014));
                        } 
                    }
                }
            }
        }
    }
    
    public Client buildClient(String vdbName, int version, Properties props) {
        return new LocalClient(vdbName, version, props);        
    }
        
    @Override
    public void destroy() {
        this.contextMap.clear();
    }

    @Override
    public void removed(String name, int version, CompositeVDB vdb) {
        this.contextMap.remove(new VDBKey(name, version));
    }

    @Override
    public void finishedDeployment(String name, int version, CompositeVDB vdb,
            boolean reloading) {
        this.contextMap.remove(new VDBKey(name, version));
    }

    @Override
    public void beforeRemove(String name, int version, CompositeVDB vdb) {
    }

    @Override
    public void added(String name, int version, CompositeVDB vdb, boolean reloading) {
    }
}
