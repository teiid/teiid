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

package org.teiid.resource.adapter.ws;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Future;

import javax.activation.DataSource;
import javax.resource.ResourceException;
import javax.security.auth.Subject;
import javax.ws.rs.core.Response.Status;
import javax.xml.namespace.QName;
import javax.xml.ws.*;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.endpoint.Client;
import org.apache.cxf.endpoint.Endpoint;
import org.apache.cxf.interceptor.Interceptor;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxws.DispatchImpl;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.cxf.ws.security.SecurityConstants;
import org.apache.cxf.ws.security.wss4j.WSS4JInInterceptor;
import org.apache.cxf.ws.security.wss4j.WSS4JOutInterceptor;
import org.ietf.jgss.GSSCredential;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.Base64;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.ConnectionContext;
import org.teiid.translator.WSConnection;

/**
 * WebService connection implementation.
 *
 * TODO: set a handler chain
 */
public class WSConnectionImpl extends BasicConnection implements WSConnection {

	private static final String CONNECTION_TIMEOUT = "javax.xml.ws.client.connectionTimeout"; //$NON-NLS-1$
	private static final String RECEIVE_TIMEOUT = "javax.xml.ws.client.receiveTimeout"; //$NON-NLS-1$

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

		private HashMap<String, Object> requestContext = new HashMap<String, Object>();
		private HashMap<String, Object> responseContext = new HashMap<String, Object>();
		private WebClient client;
		private String endpoint;

		public HttpDispatch(String endpoint, String configFile, @SuppressWarnings("unused") String configName) {
			this.endpoint = endpoint;
			if (configFile == null) {
			    this.client = WebClient.create(this.endpoint);
			}
			else {
			    this.client = WebClient.create(this.endpoint, configFile);
			}
		}

		@Override
		public DataSource invoke(DataSource msg) {
			try {
				final URL url = new URL(this.endpoint);

				Map<String, List<String>> header = (Map<String, List<String>>)this.requestContext.get(MessageContext.HTTP_REQUEST_HEADERS);
				for (Map.Entry<String, List<String>> entry : header.entrySet()) {
					String value = StringUtil.join(entry.getValue(), ","); //$NON-NLS-1$
					this.client.header(entry.getKey(), value);
				}
				String username = (String) this.requestContext.get(Dispatch.USERNAME_PROPERTY);
				String password = (String) this.requestContext.get(Dispatch.PASSWORD_PROPERTY);
				
				if (username != null) {
					this.client.header("Authorization", "Basic " + Base64.encodeBytes((username + ':' + password).getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
				}
				else if (this.requestContext.get(GSSCredential.class.getName()) != null) {
				    WebClient.getConfig(this.client).getRequestContext().put(GSSCredential.class.getName(), this.requestContext.get(GSSCredential.class.getName()));
				    WebClient.getConfig(this.client).getRequestContext().put("auth.spnego.requireCredDelegation", true); //$NON-NLS-1$ 
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
				
				javax.ws.rs.core.Response response = this.client.invoke((String)this.requestContext.get(MessageContext.HTTP_REQUEST_METHOD), payload);
				this.responseContext.put(WSConnection.STATUS_CODE, response.getStatus());
				this.responseContext.putAll(response.getMetadata());

				ArrayList contentTypes = (ArrayList)this.responseContext.get("content-type"); //$NON-NLS-1$
				String contentType = contentTypes != null ? (String)contentTypes.get(0):"application/octet-stream"; //$NON-NLS-1$
				return new HttpDataSource(url, (InputStream)response.getEntity(), contentType);
			} catch (IOException e) {
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



	private WSManagedConnectionFactory mcf;
	private Service wsdlService;

	public WSConnectionImpl(WSManagedConnectionFactory mcf) {
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
				LogManager.logDetail(LogConstants.CTX_WS, "Created the WSDL service for", this.mcf.getWsdl()); //$NON-NLS-1$
			}
		}
		Dispatch<T> dispatch = this.wsdlService.createDispatch(this.mcf.getPortQName(), type, mode);
		configureWSSecurity(dispatch);
		setDispatchProperties(dispatch, "SOAP12"); //$NON-NLS-1$
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
				String defaultEndpoint = this.mcf.getEndPoint();
				String defaultQueryString = null;
				String defaultFragment = null;
				if (defaultEndpoint == null) {
					throw new WebServiceException(WSManagedConnectionFactory.UTIL.getString("null_default_endpoint")); //$NON-NLS-1$
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
				if (endpoint.startsWith("?") || endpoint.startsWith("/")) { //$NON-NLS-1$ //$NON-NLS-2$
					endpoint = defaultEndpoint + endpoint;
				} else {
					endpoint = defaultEndpoint + "/" + endpoint; //$NON-NLS-1$
				}
				if ((defaultQueryString != null) && (defaultQueryString.trim().length() > 0)) {
					endpoint = WSConnection.Util.appendQueryString(endpoint, defaultQueryString);
				}
				if ((defaultFragment != null) && (endpoint.indexOf('#') < 0)) {
					endpoint = endpoint + '#' + defaultFragment;
				}
			}
		} else {
			endpoint = this.mcf.getEndPoint();
			if (endpoint == null) {
				throw new WebServiceException(WSManagedConnectionFactory.UTIL.getString("null_endpoint")); //$NON-NLS-1$
			}
		}
		Dispatch<T> dispatch = null;
		if (HTTPBinding.HTTP_BINDING.equals(binding) && (type == DataSource.class)) {
			Bus bus = BusFactory.getThreadDefaultBus();
			BusFactory.setThreadDefaultBus(this.mcf.getBus());
			try {
				dispatch = (Dispatch<T>) new HttpDispatch(endpoint, this.mcf.getConfigFile(), this.mcf.getConfigName());
			} finally {
				BusFactory.setThreadDefaultBus(bus);
			}
		} else {
			//TODO: cache service/port/dispatch instances?
			Bus bus = BusFactory.getThreadDefaultBus();
			BusFactory.setThreadDefaultBus(this.mcf.getBus());
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
        if (this.mcf.getAsSecurityType() == WSManagedConnectionFactory.SecurityType.WSSecurity) {
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
            	Subject subject = ConnectionContext.getSubject();
            	if (subject != null) {
            		WSSecurityCredential credential = ConnectionContext.getSecurityCredential(subject, WSSecurityCredential.class);
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
            		HashMap<String, String> properties = ConnectionContext.getSecurityCredential(subject, HashMap.class);
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
		if (this.mcf.getAsSecurityType() == WSManagedConnectionFactory.SecurityType.HTTPBasic){

			String userName = this.mcf.getAuthUserName();
			String password = this.mcf.getAuthPassword();

			// if security-domain is specified and caller identity is used; then use
			// credentials from subject
			Subject subject = ConnectionContext.getSubject();
			if (subject != null) {
				userName = ConnectionContext.getUserName(subject, this.mcf, userName);
				password = ConnectionContext.getPassword(subject, this.mcf, userName, password);
			}

			dispatch.getRequestContext().put(Dispatch.USERNAME_PROPERTY, userName);
			dispatch.getRequestContext().put(Dispatch.PASSWORD_PROPERTY, password);
		}
		else if (this.mcf.getAsSecurityType() == WSManagedConnectionFactory.SecurityType.Kerberos) {
		    boolean credentialFound = false;
            Subject subject = ConnectionContext.getSubject();
            if (subject != null) {
                GSSCredential credential = ConnectionContext.getSecurityCredential(subject, GSSCredential.class);
                if (credential != null) {
                    dispatch.getRequestContext().put(GSSCredential.class.getName(), credential);  
                    credentialFound = true;
                }
            }
            if (!credentialFound) {
                throw new WebServiceException(WSManagedConnectionFactory.UTIL.getString("no_gss_credential")); //$NON-NLS-1$
            }
		}

		if (this.mcf.getRequestTimeout() != null){
			dispatch.getRequestContext().put(RECEIVE_TIMEOUT, this.mcf.getRequestTimeout()); 
		}
		if (this.mcf.getConnectTimeout() != null){
			dispatch.getRequestContext().put(CONNECTION_TIMEOUT, this.mcf.getConnectTimeout()); 
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
	public void close() throws ResourceException {
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
}
