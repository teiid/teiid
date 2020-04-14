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

package org.teiid.ws.cxf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.activation.DataSource;
import javax.security.auth.Subject;
import javax.ws.rs.core.Response.Status;
import javax.xml.namespace.QName;
import javax.xml.ws.AsyncHandler;
import javax.xml.ws.Binding;
import javax.xml.ws.Dispatch;
import javax.xml.ws.EndpointReference;
import javax.xml.ws.Response;
import javax.xml.ws.Service;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.bus.spring.SpringBusFactory;
import org.apache.cxf.configuration.security.AuthorizationPolicy;
import org.apache.cxf.endpoint.Client;
import org.apache.cxf.endpoint.Endpoint;
import org.apache.cxf.interceptor.Interceptor;
import org.apache.cxf.jaxrs.client.JAXRSClientFactoryBean;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxws.DispatchImpl;
import org.apache.cxf.rt.security.SecurityConstants;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transport.http.HTTPConduitFactory;
import org.apache.cxf.transport.http.asyncclient.AsyncHTTPConduitFactory;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.cxf.ws.security.wss4j.WSS4JInInterceptor;
import org.apache.cxf.ws.security.wss4j.WSS4JOutInterceptor;
import org.ietf.jgss.GSSCredential;
import org.teiid.OAuthCredential;
import org.teiid.core.util.ArgCheck;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.translator.ws.WSConnection;
import org.teiid.util.WSUtil;

/**
 * WebService connection implementation.
 *
 * TODO: set a handler chain
 */
public abstract class BaseWSConnection implements WSConnection {

    private static final String CONNECTION_TIMEOUT = "javax.xml.ws.client.connectionTimeout"; //$NON-NLS-1$
    private static final String RECEIVE_TIMEOUT = "javax.xml.ws.client.receiveTimeout"; //$NON-NLS-1$
    private static final String DUMMY_BINDING = ""; //NON-NLS-1$

    private static final class HttpDataSource implements DataSource {
        private final URL url;
        private InputStream content;
        private String contentType;

        private HttpDataSource(URL url, InputStream entity, String contentType) {
            this.url = url;
            this.content = entity;
            this.contentType = contentType;
        }

        @Override
        public OutputStream getOutputStream() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            return this.url.getPath();
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return this.content;
        }

        @Override
        public String getContentType() {
            return this.contentType;
        }
    }

    private static final class HttpDispatch implements Dispatch<DataSource> {

        private static final String AUTHORIZATION = "Authorization"; //$NON-NLS-1$
        private HashMap<String, Object> requestContext = new HashMap<String, Object>();
        private HashMap<String, Object> responseContext = new HashMap<String, Object>();
        private WebClient client;
        private String endpoint;
        private String configFile;

        public HttpDispatch(String endpoint, String configFile, @SuppressWarnings("unused") String configName) {
            this.endpoint = endpoint;
            this.configFile = configFile;
        }

        WebClient createWebClient(String baseAddress, Bus bus) {
            JAXRSClientFactoryBean bean = new JAXRSClientFactoryBean();
            bean.setBus(bus);
            bean.setAddress(baseAddress);
            return bean.createWebClient();
        }

        Bus getBus(String configLocation) {
            if (configLocation != null) {
                SpringBusFactory bf = new SpringBusFactory();
                return bf.createBus(configLocation);
            } else {
                return BusFactory.getThreadDefaultBus();
            }
        }

        @Override
        public DataSource invoke(DataSource msg) {
            try {
                final URL url = new URL(this.endpoint);
                url.toURI(); //ensure this is a valid uri

                final String httpMethod = (String)this.requestContext.get(MessageContext.HTTP_REQUEST_METHOD);

                // see to use patch
                // http://stackoverflow.com/questions/32067687/how-to-use-patch-method-in-cxf
                Bus bus = getBus(this.configFile);
                if (httpMethod.equals("PATCH")) {
                    bus.setProperty("use.async.http.conduit", Boolean.TRUE);
                    bus.setExtension(new AsyncHTTPConduitFactory(bus), HTTPConduitFactory.class);
                }
                this.client = createWebClient(this.endpoint, bus);

                Map<String, List<String>> header = (Map<String, List<String>>)this.requestContext.get(MessageContext.HTTP_REQUEST_HEADERS);
                for (Map.Entry<String, List<String>> entry : header.entrySet()) {
                    this.client.header(entry.getKey(), entry.getValue().toArray());
                }

                if (this.requestContext.get(AuthorizationPolicy.class.getName()) != null) {
                    HTTPConduit conduit = (HTTPConduit)WebClient.getConfig(this.client).getConduit();
                    AuthorizationPolicy policy = (AuthorizationPolicy)this.requestContext.get(AuthorizationPolicy.class.getName());
                    conduit.setAuthorization(policy);
                }
                else if (this.requestContext.get(GSSCredential.class.getName()) != null) {
                    WebClient.getConfig(this.client).getRequestContext().put(GSSCredential.class.getName(), this.requestContext.get(GSSCredential.class.getName()));
                    WebClient.getConfig(this.client).getRequestContext().put("auth.spnego.requireCredDelegation", true); //$NON-NLS-1$
                }
                else if (this.requestContext.get(OAuthCredential.class.getName()) != null) {
                    OAuthCredential credential = (OAuthCredential)this.requestContext.get(OAuthCredential.class.getName());
                    this.client.header(AUTHORIZATION, credential.getAuthorizationHeader(this.endpoint, httpMethod));
                }

                InputStream payload = null;
                if (msg != null) {
                    payload = msg.getInputStream();
                }

                HTTPClientPolicy clientPolicy = WebClient.getConfig(this.client).getHttpConduit().getClient();
                Long timeout = (Long) this.requestContext.get(RECEIVE_TIMEOUT);
                if (timeout != null) {
                    clientPolicy.setReceiveTimeout(timeout);
                }
                timeout = (Long) this.requestContext.get(CONNECTION_TIMEOUT);
                if (timeout != null) {
                    clientPolicy.setConnectionTimeout(timeout);
                }

                javax.ws.rs.core.Response response = this.client.invoke(httpMethod, payload);
                this.responseContext.put(WSConnection.STATUS_CODE, response.getStatus());
                this.responseContext.putAll(response.getMetadata());

                ArrayList contentTypes = (ArrayList)this.responseContext.get("content-type"); //$NON-NLS-1$
                String contentType = contentTypes != null ? (String)contentTypes.get(0):"application/octet-stream"; //$NON-NLS-1$
                return new HttpDataSource(url, (InputStream)response.getEntity(), contentType);
            } catch (IOException e) {
                throw new WebServiceException(e);
            } catch (URISyntaxException e) {
                throw new WebServiceException(e);
            }
        }

        @Override
        public Map<String, Object> getRequestContext() {
            return this.requestContext;
        }

        @Override
        public Map<String, Object> getResponseContext() {
            return this.responseContext;
        }

        @Override
        public Binding getBinding() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EndpointReference getEndpointReference() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends EndpointReference> T getEndpointReference(Class<T> clazz) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Response<DataSource> invokeAsync(DataSource msg) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<?> invokeAsync(DataSource msg,AsyncHandler<DataSource> handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void invokeOneWay(DataSource msg) {
            throw new UnsupportedOperationException();
        }
    }



    private WSConnectionFactory mcf;
    private Service wsdlService;

    public BaseWSConnection(WSConnectionFactory mcf) {
        this.mcf = mcf;
    }

    public <T> Dispatch<T> createDispatch(Class<T> type, Mode mode) throws IOException {
        if (this.wsdlService == null) {
            Bus bus = BusFactory.getThreadDefaultBus();
            BusFactory.setThreadDefaultBus(this.mcf.getBus());
            try {
                this.wsdlService = Service.create(this.mcf.getWsdlUrl(), this.mcf.getServiceQName());
            } finally {
                BusFactory.setThreadDefaultBus(bus);
            }
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_WS, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_WS, "Created the WSDL service for", this.mcf.getConfig().getWsdl()); //$NON-NLS-1$
            }
        }
        Dispatch<T> dispatch = this.wsdlService.createDispatch(this.mcf.getPortQName(), type, mode);
        configureWSSecurity(dispatch);
        setDispatchProperties(dispatch, DUMMY_BINDING);
        return dispatch;
    }

    public <T> Dispatch<T> createDispatch(String binding, String endpoint, Class<T> type, Mode mode) {
        ArgCheck.isNotNull(binding);
        if (endpoint != null) {
            try {
                new URL(endpoint);
                //valid url, just use the endpoint
            } catch (MalformedURLException e) {
                //otherwise it should be a relative value
                //but we should still preserve the base path and query string
                String defaultEndpoint = this.mcf.getConfig().getEndPoint();
                String defaultQueryString = null;
                String defaultFragment = null;
                if (defaultEndpoint == null) {
                    throw new WebServiceException(WSConnectionFactory.UTIL.getString("null_default_endpoint")); //$NON-NLS-1$
                }
                String[] parts = defaultEndpoint.split("\\?", 2); //$NON-NLS-1$
                defaultEndpoint = parts[0];
                if (parts.length > 1) {
                    defaultQueryString = parts[1];
                    parts = defaultQueryString.split("#"); //$NON-NLS-1$
                    defaultQueryString = parts[0];
                    if (parts.length > 1) {
                        defaultFragment = parts[1];
                    }
                }
                if (endpoint.startsWith("?") || endpoint.startsWith("/") || defaultEndpoint.endsWith("/")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    endpoint = defaultEndpoint + endpoint;
                } else {
                    endpoint = defaultEndpoint + "/" + endpoint; //$NON-NLS-1$
                }
                if ((defaultQueryString != null) && (defaultQueryString.trim().length() > 0)) {
                    endpoint = WSUtil.appendQueryString(endpoint, defaultQueryString);
                }
                if ((defaultFragment != null) && (endpoint.indexOf('#') < 0)) {
                    endpoint = endpoint + '#' + defaultFragment;
                }
            }
        } else {
            endpoint = this.mcf.getConfig().getEndPoint();
            if (endpoint == null) {
                throw new WebServiceException(WSConnectionFactory.UTIL.getString("null_endpoint")); //$NON-NLS-1$
            }
        }
        Dispatch<T> dispatch = null;
        Bus bus = BusFactory.getThreadDefaultBus();
        BusFactory.setThreadDefaultBus(this.mcf.getBus());
        if (HTTPBinding.HTTP_BINDING.equals(binding) && (type == DataSource.class)) {
            try {
                dispatch = (Dispatch<T>) new HttpDispatch(endpoint, this.mcf.getConfig().getConfigFile(), this.mcf.getConfig().getConfigName());
            } finally {
                BusFactory.setThreadDefaultBus(bus);
            }
        } else {
            //TODO: cache service/port/dispatch instances?
            Service svc;
            try {
                svc = Service.create(this.mcf.getServiceQName());
            } finally {
                BusFactory.setThreadDefaultBus(bus);
            }
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_WS, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_WS, "Creating a dispatch with endpoint", endpoint); //$NON-NLS-1$
            }
            svc.addPort(this.mcf.getPortQName(), binding, endpoint);

            dispatch = svc.createDispatch(this.mcf.getPortQName(), type, mode);
            configureWSSecurity(dispatch);
        }
        setDispatchProperties(dispatch, binding);
        return dispatch;
    }

    private <T> void configureWSSecurity(Dispatch<T> dispatch) {
        if (this.mcf.getConfig().getAsSecurityType() == WSConfiguration.SecurityType.WSSecurity) {
            Bus bus = BusFactory.getThreadDefaultBus();
            BusFactory.setThreadDefaultBus(this.mcf.getBus());
            try {
                Client client = ((DispatchImpl)dispatch).getClient();
                Endpoint ep = client.getEndpoint();

                // spring configuration file
                if (this.mcf.getOutInterceptors() != null) {
                    for (Interceptor i : this.mcf.getOutInterceptors()) {
                        ep.getOutInterceptors().add(i);
                    }
                }

                // ws-security pass-thru from custom jaas domain
                Subject subject = getSubject();
                if (subject != null) {
                    WSSecurityCredential credential = getSecurityCredential(subject, WSSecurityCredential.class);
                    if (credential != null) {
                        if (credential.useSts()) {
                            dispatch.getRequestContext().put(SecurityConstants.STS_CLIENT, credential.buildStsClient(bus));
                        }
                        if(credential.getSecurityHandler() == WSSecurityCredential.SecurityHandler.WSS4J) {
                            ep.getOutInterceptors().add(new WSS4JOutInterceptor(credential.getRequestPropterties()));
                            ep.getInInterceptors().add(new WSS4JInInterceptor(credential.getResponsePropterties()));
                        }
                        else if (credential.getSecurityHandler() == WSSecurityCredential.SecurityHandler.WSPOLICY) {
                            dispatch.getRequestContext().putAll(credential.getRequestPropterties());
                            dispatch.getResponseContext().putAll(credential.getResponsePropterties());
                        }
                    }

                    // When properties are set on subject treat them as they can configure WS-Security
                    HashMap<String, String> properties = getSecurityCredential(subject, HashMap.class);
                    for (String key:properties.keySet()) {
                        if (key.startsWith("ws-security.")) { //$NON-NLS-1$
                            ep.put(key, properties.get(key));
                        }
                    }
                }
            } finally {
                BusFactory.setThreadDefaultBus(bus);
            }
        }
    }

    private <T> void setDispatchProperties(Dispatch<T> dispatch, String binding) {
        if (this.mcf.getConfig().getAsSecurityType() == WSConfiguration.SecurityType.HTTPBasic
                || this.mcf.getConfig().getAsSecurityType() == WSConfiguration.SecurityType.Digest){

            String userName = this.mcf.getConfig().getAuthUserName();
            String password = this.mcf.getConfig().getAuthPassword();

            // if security-domain is specified and caller identity is used; then use
            // credentials from subject
            Subject subject = getSubject();
            if (subject != null) {
                userName = getUserName(subject, userName);
                password = getPassword(subject, userName, password);
            }
            AuthorizationPolicy policy = new AuthorizationPolicy();
            policy.setUserName(userName);
            policy.setPassword(password);
            if (this.mcf.getConfig().getAsSecurityType() == WSConfiguration.SecurityType.Digest) {
                policy.setAuthorizationType("Digest");
            } else {
                policy.setAuthorizationType("Basic");
            }
            dispatch.getRequestContext().put(AuthorizationPolicy.class.getName(), policy);
        }
        else if (this.mcf.getConfig().getAsSecurityType() == WSConfiguration.SecurityType.Kerberos) {
            boolean credentialFound = false;
            Subject subject = getSubject();
            if (subject != null) {
                GSSCredential credential = getSecurityCredential(subject, GSSCredential.class);
                if (credential != null) {
                    dispatch.getRequestContext().put(GSSCredential.class.getName(), credential);
                    credentialFound = true;
                }
            }
            if (!credentialFound) {
                throw new WebServiceException(WSConnectionFactory.UTIL.getString("no_gss_credential")); //$NON-NLS-1$
            }
        }
        else if (this.mcf.getConfig().getAsSecurityType() == WSConfiguration.SecurityType.OAuth) {
            boolean credentialFound = false;
            Subject subject = getSubject();
            if (subject != null) {
                OAuthCredential credential = getSecurityCredential(subject, OAuthCredential.class);
                if (credential != null) {
                    dispatch.getRequestContext().put(OAuthCredential.class.getName(), credential);
                    credentialFound = true;
                }
            }
            if (!credentialFound) {
                throw new WebServiceException(WSConnectionFactory.UTIL.getString("no_oauth_credential")); //$NON-NLS-1$
            }
        }

        if (this.mcf.getConfig().getRequestTimeout() != null){
            dispatch.getRequestContext().put(RECEIVE_TIMEOUT, this.mcf.getConfig().getRequestTimeout());
        }
        if (this.mcf.getConfig().getConnectTimeout() != null){
            dispatch.getRequestContext().put(CONNECTION_TIMEOUT, this.mcf.getConfig().getConnectTimeout());
        }

        if (HTTPBinding.HTTP_BINDING.equals(binding)) {
            Map<String, List<String>> httpHeaders = (Map<String, List<String>>)dispatch.getRequestContext().get(MessageContext.HTTP_REQUEST_HEADERS);
            if(httpHeaders == null) {
                httpHeaders = new HashMap<String, List<String>>();
            }
            httpHeaders.put("Content-Type", Collections.singletonList("text/xml; charset=utf-8"));//$NON-NLS-1$ //$NON-NLS-2$
            httpHeaders.put("User-Agent", Collections.singletonList("Teiid Server"));//$NON-NLS-1$ //$NON-NLS-2$
            dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_HEADERS, httpHeaders);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public String getEndPoint() {
        return this.mcf.getConfig().getEndPoint();
    }

    @Override
    public URL getWsdl() {
        return this.mcf.getWsdlUrl();
    }

    @Override
    public QName getServiceQName() {
        return this.mcf.getServiceQName();
    }

    @Override
    public QName getPortQName() {
        return this.mcf.getPortQName();
    }

    @Override
    public String getStatusMessage(int status) {
        Status s = javax.ws.rs.core.Response.Status.fromStatusCode(status);
        if (s != null) {
            return s.getReasonPhrase();
        }
        return null;
    }

    protected abstract Subject getSubject();
    protected abstract <T> T getSecurityCredential(Subject s, Class<T> clazz);
    protected abstract String getUserName(Subject s, String defaultUserName);
    protected abstract String getPassword(Subject s, String userName, String defaultPassword);
}
