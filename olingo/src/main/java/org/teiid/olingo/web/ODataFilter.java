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
package org.teiid.olingo.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.ref.SoftReference;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.server.api.ODataHandler;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.LRUCache;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.json.simple.JSONParser;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.odata.api.Client;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.service.LocalClient;
import org.teiid.olingo.service.OlingoBridge;
import org.teiid.olingo.service.OlingoBridge.HandlerInfo;
import org.teiid.transport.LocalServerConnection;
import org.teiid.vdb.runtime.VDBKey;

public class ODataFilter implements Filter, VDBLifeCycleListener {

    protected String proxyBaseURI;
    protected Properties initProperties;
    protected Map<VDBKey, SoftReference<OlingoBridge>> contextMap = Collections
            .synchronizedMap(new LRUCache<VDBKey, SoftReference<OlingoBridge>>());
    private volatile boolean listenerRegistered = false;
    //default odata behavior requires explicit versioning
    private String defaultVdbVersion = "1"; //$NON-NLS-1$
    private Map<Object, Future<Boolean>> loadingQueries = new ConcurrentHashMap<>();

    protected OpenApiHandler openApiHandler;

    @Override
    public void init(FilterConfig config) throws ServletException {
        // handle proxy-uri in the case of cloud environments
        String proxyURI = config.getInitParameter("proxy-base-uri"); //$NON-NLS-1$
        if (proxyURI != null && proxyURI.startsWith("${") && proxyURI.endsWith("}")) { //$NON-NLS-1$ //$NON-NLS-2$
            proxyURI = proxyURI.substring(2, proxyURI.length() - 1);
            proxyURI = PropertiesUtils.getHierarchicalProperty(proxyURI, null);
        }

        if (proxyURI != null) {
            this.proxyBaseURI = proxyURI;
        }

        String value = config.getInitParameter("explicit-vdb-version");  //$NON-NLS-1$
        if (value != null && !Boolean.valueOf(value)) {
            defaultVdbVersion = null;
        }

        Properties props = new Properties();
        Enumeration<String> names = config.getServletContext().getInitParameterNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            props.setProperty(name, config.getServletContext().getInitParameter(name));
        }
        names = config.getInitParameterNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            props.setProperty(name, config.getInitParameter(name));
        }
        this.initProperties = props;
        this.openApiHandler = new OpenApiHandler(config.getServletContext());
    }

    public String getDefaultVdbVersion() {
        return defaultVdbVersion;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {
        try {
            internalDoFilter(request, response, chain);
        } catch (TeiidProcessingException e) {
            //TODO: use engine style logic to determine if the stack should be logged
            LogManager.logWarning(LogConstants.CTX_ODATA, e, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16047, e.getMessage()));
            HttpServletResponse httpResponse = (HttpServletResponse)response;
            writeError(request, e, httpResponse, 404);
        }
    }

    static void writeError(ServletRequest request, TeiidProcessingException e,
            HttpServletResponse httpResponse, int statusCode) throws IOException {
        httpResponse.setStatus(statusCode);
        String format = request.getParameter("$format"); //$NON-NLS-1$
        if (format == null) {
            //TODO: could also look at the accepts header
            ContentType contentType = ContentType.parse(request.getContentType());
            if (contentType == null || contentType.isCompatible(ContentType.APPLICATION_JSON)) {
                format = "json"; //$NON-NLS-1$
            } else {
                format = "xml"; //$NON-NLS-1$
            }
        }
        PrintWriter writer = httpResponse.getWriter();
        String code = e.getCode()==null?"":e.getCode(); //$NON-NLS-1$
        String message = e.getMessage()==null?"":e.getMessage(); //$NON-NLS-1$
        if (format.equalsIgnoreCase("json")) { //$NON-NLS-1$
            httpResponse.setHeader(HttpHeader.CONTENT_TYPE, ContentType.APPLICATION_JSON.toContentTypeString());
            writer.write("{ \"error\": { \"code\": \""); //$NON-NLS-1$
            JSONParser.escape(code, writer);
            writer.write("\", \"message\": \""); //$NON-NLS-1$
            JSONParser.escape(message, writer);
            writer.write("\" } }"); //$NON-NLS-1$
        } else {
            try {
                httpResponse.setHeader(HttpHeader.CONTENT_TYPE, ContentType.APPLICATION_XML.toContentTypeString());
                XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(writer);
                xmlStreamWriter.writeStartElement("m", "error", "http://docs.oasis-open.org/odata/ns/metadata"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                xmlStreamWriter.writeNamespace("m", "http://docs.oasis-open.org/odata/ns/metadata"); //$NON-NLS-1$ //$NON-NLS-2$
                xmlStreamWriter.writeStartElement("m", "code", "http://docs.oasis-open.org/odata/ns/metadata"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                xmlStreamWriter.writeCharacters(code);
                xmlStreamWriter.writeEndElement();
                xmlStreamWriter.writeStartElement("m", "message", "http://docs.oasis-open.org/odata/ns/metadata"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                xmlStreamWriter.writeCharacters(message);
                xmlStreamWriter.writeEndElement();
                xmlStreamWriter.writeEndElement();
                xmlStreamWriter.flush();
            } catch (XMLStreamException x) {
                throw new IOException(x);
            }
        }
        writer.close();
    }

    public void internalDoFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException, TeiidProcessingException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;

        String proxyURI = this.proxyBaseURI;
        if (proxyURI != null) {
            httpRequest = new ProxyHttpServletRequest(httpRequest, proxyURI);
        } else {
            httpRequest = ProxyHttpServletRequest.handleProxiedRequest(httpRequest);
        }

        VDBKey key = null;
        String vdbName = null;
        String version = null;
        String modelName = null;

        String uri = httpRequest.getRequestURI().toString();
        String fullURL = httpRequest.getRequestURL().toString();
        if (uri.startsWith("/odata4/static/") || uri.startsWith("/odata4/keycloak/")){ //$NON-NLS-1$ //$NON-NLS-2$
            chain.doFilter(httpRequest, response);
            return;
        }

        String contextPathOriginal = httpRequest.getContextPath();
        String contextPath = contextPathOriginal;
        String baseURI = fullURL.substring(0, fullURL.indexOf(contextPath));

        int endIdx = uri.indexOf('/', contextPath.length() + 1);
        int beginIdx = contextPath.length() + 1;

        if (contextPath.equals("/odata4")) { //$NON-NLS-1$
            if (endIdx == -1) {
                throw new TeiidProcessingException(ODataPlugin.Event.TEIID16020, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16020));
            }
            baseURI = baseURI+"/odata4"; //$NON-NLS-1$
            vdbName = uri.substring(beginIdx, endIdx);
            int modelIdx = uri.indexOf('/', endIdx + 1);
            if (modelIdx == -1) {
                modelName = uri.substring(endIdx + 1).trim();
                if (modelName.isEmpty()) {
                    throw new TeiidProcessingException(ODataPlugin.Event.TEIID16019, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16019));
                }
            } else {
                modelName = uri.substring(endIdx + 1, modelIdx);
            }

            contextPath = contextPath + "/" + vdbName + "/" + modelName; //$NON-NLS-1$ //$NON-NLS-2$

            vdbName = vdbName.trim();
            if (vdbName.isEmpty()) {
                throw new TeiidProcessingException(ODataPlugin.Event.TEIID16008, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16008));
            }

            modelName = URLDecoder.decode(modelName, "UTF-8"); //$NON-NLS-1$
            vdbName = URLDecoder.decode(vdbName, "UTF-8"); //$NON-NLS-1$
        } else {
            if (this.initProperties.getProperty("vdb-name") == null) { //$NON-NLS-1$
                throw new TeiidProcessingException(ODataPlugin.Event.TEIID16018, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16018));
            }

            vdbName = this.initProperties.getProperty("vdb-name"); //$NON-NLS-1$
            version = this.initProperties.getProperty("vdb-version"); //$NON-NLS-1$

            if (endIdx == -1) {
                modelName = uri.substring(beginIdx).trim();
                if (modelName.isEmpty()) {
                    throw new TeiidProcessingException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16021));
                }
            } else {
                modelName = uri.substring(beginIdx, endIdx);
            }

            contextPath = contextPath + "/" + modelName; //$NON-NLS-1$
        }

        ContextAwareHttpSerlvetRequest contextAwareRequest = new ContextAwareHttpSerlvetRequest(httpRequest);
        contextAwareRequest.setContextPath(contextPath);
        httpRequest = contextAwareRequest;

        key = new VDBKey(vdbName, version);
        if (key.isAtMost()) {
            if (key.getVersion() != null) {
                throw new TeiidProcessingException(ODataPlugin.Event.TEIID16044, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16044, key));
            }
            key = new VDBKey(vdbName, defaultVdbVersion);
        }

        SoftReference<OlingoBridge> ref = this.contextMap.get(key);
        OlingoBridge context = null;
        if (ref != null) {
            context = ref.get();
        }

        if (context == null) {
            context = new OlingoBridge(null);
            ref = new SoftReference<OlingoBridge>(context);
            this.contextMap.put(key, ref);
        }

        Client client = buildClient(key.getName(), key.getVersion(), this.initProperties);
        try {
            Connection connection = client.open();
            registerVDBListener(client, connection);
            HandlerInfo handlerInfo = context.getHandlers(contextPathOriginal, client, modelName);
            ODataHandler handler = handlerInfo.oDataHttpHandler;

            if (openApiHandler.processOpenApiMetadata(httpRequest, key, uri, modelName,
                    response, handlerInfo.serviceMetadata, null)) {
                return;
            }

            httpRequest.setAttribute(ODataHttpHandler.class.getName(), handler);
            httpRequest.setAttribute(Client.class.getName(), client);
            chain.doFilter(httpRequest, response);
            response.flushBuffer();
        } catch(SQLException e) {
            throw new TeiidProcessingException(e);
        } finally {
            try {
                client.close();
            } catch (SQLException e) {
                //ignore
            }
        }
    }

    protected void registerVDBListener(Client client, Connection conn) {
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

    public Client buildClient(String vdbName, String version, Properties props) {
        return new LocalClient(vdbName, version, props, loadingQueries);
    }

    @Override
    public void destroy() {
        this.contextMap.clear();
    }

    @Override
    public void removed(String name, CompositeVDB vdb) {
        this.contextMap.remove(vdb.getVDBKey());
    }

    @Override
    public void finishedDeployment(String name, CompositeVDB vdb) {
        this.contextMap.remove(vdb.getVDBKey());
    }

    @Override
    public void beforeRemove(String name, CompositeVDB vdb) {
    }

    @Override
    public void added(String name, CompositeVDB vdb) {
    }
}
