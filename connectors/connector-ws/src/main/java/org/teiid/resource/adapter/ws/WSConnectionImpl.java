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
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.activation.DataSource;
import javax.resource.ResourceException;
import javax.xml.namespace.QName;
import javax.xml.ws.AsyncHandler;
import javax.xml.ws.Binding;
import javax.xml.ws.Dispatch;
import javax.xml.ws.EndpointReference;
import javax.xml.ws.Response;
import javax.xml.ws.Service;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.endpoint.Client;
import org.apache.cxf.endpoint.Endpoint;
import org.apache.cxf.interceptor.Interceptor;
import org.apache.cxf.jaxws.DispatchImpl;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.Base64;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.WSConnection;

/**
 * WebService connection implementation.
 * 
 * TODO: set a handler chain
 */
public class WSConnectionImpl extends BasicConnection implements WSConnection {
	
	private static final class HttpDataSource implements DataSource {
		private final URL url;
		private final HttpURLConnection httpConn;

		private HttpDataSource(URL url, HttpURLConnection httpConn) {
			this.url = url;
			this.httpConn = httpConn;
		}

		@Override
		public OutputStream getOutputStream() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getName() {
			return url.getPath();
		}

		@Override
		public InputStream getInputStream() throws IOException {
			return httpConn.getInputStream();
		}

		@Override
		public String getContentType() {
			return httpConn.getContentType();
		}
	}
	
	/**
	 * Workaround dispatch, since neither JBossNative nor CXF 2.2.2 implement
	 * this functionality.
	 */
	private static final class BinaryDispatch implements Dispatch<DataSource> {

		HashMap<String, Object> requestContext = new HashMap<String, Object>();
		
		private String endpoint;
		
		public BinaryDispatch(String endpoint) {
			this.endpoint = endpoint;
		}

		@Override
		public DataSource invoke(DataSource msg) {
			try {
				final URL url = new URL(endpoint);
				final HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
				httpConn.setRequestMethod((String) requestContext.get(MessageContext.HTTP_REQUEST_METHOD));
				Map<String, List<String>> header = (Map<String, List<String>>)requestContext.get(MessageContext.HTTP_REQUEST_HEADERS);
				for (Map.Entry<String, List<String>> entry : header.entrySet()) {
					String value = StringUtil.join(entry.getValue(), ","); //$NON-NLS-1$
					httpConn.setRequestProperty(entry.getKey(), value);
				}
				String username = (String) requestContext.get(Dispatch.USERNAME_PROPERTY);
				String password = (String) requestContext.get(Dispatch.PASSWORD_PROPERTY);
	
				if (username != null) {
					httpConn.setRequestProperty("Authorization", "Basic " + Base64.encodeBytes((username + ':' + password).getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
				}
				
				if (msg != null) {
					httpConn.setDoOutput(true);
					OutputStream os = httpConn.getOutputStream();
					InputStream is = msg.getInputStream();
					ObjectConverterUtil.write(os, is, -1);
				}
				
				return new HttpDataSource(url, httpConn);
			} catch (IOException e) {
				throw new WebServiceException(e);
			}
		}

		@Override
		public Response<DataSource> invokeAsync(DataSource msg) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Future<?> invokeAsync(DataSource msg, AsyncHandler<DataSource> handler) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void invokeOneWay(DataSource msg) {
			throw new UnsupportedOperationException();
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
		public <T extends EndpointReference> T getEndpointReference(
				Class<T> clazz) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Map<String, Object> getRequestContext() {
			return requestContext;
		}

		@Override
		public Map<String, Object> getResponseContext() {
			throw new UnsupportedOperationException();
		}
	}
	
	static final String DEFAULT_NAMESPACE_URI = "http://teiid.org"; //$NON-NLS-1$ 
	static final String DEFAULT_LOCAL_NAME = "teiid"; //$NON-NLS-1$

	private static QName svcQname = new QName(DEFAULT_NAMESPACE_URI, DEFAULT_LOCAL_NAME); 
	private WSManagedConnectionFactory mcf;
	
	public WSConnectionImpl(WSManagedConnectionFactory mcf) {
		this.mcf = mcf;
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
				String defaultEndpoint = mcf.getEndPoint();
				String defaultQueryString = null;
				String defaultFragment = null;
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
				if (defaultQueryString != null && defaultQueryString.trim().length() > 0) {
					endpoint = WSConnection.Util.appendQueryString(endpoint, defaultQueryString);
				}
				if (defaultFragment != null && endpoint.indexOf('#') < 0) {
					endpoint = endpoint + '#' + defaultFragment;
				}
			}
		} else {
			endpoint = mcf.getEndPoint();
			if (endpoint == null) {
				throw new WebServiceException(WSManagedConnectionFactory.UTIL.getString("null_endpoint")); //$NON-NLS-1$
			}
		}
		Dispatch<T> dispatch = null;
		if (HTTPBinding.HTTP_BINDING.equals(binding) && type == DataSource.class) {
			dispatch = (Dispatch<T>) new BinaryDispatch(endpoint);
		} else {
			//TODO: cache service/port/dispatch instances?
			Bus bus = BusFactory.getThreadDefaultBus();
			BusFactory.setThreadDefaultBus(mcf.getBus());
			Service svc;
			try {
				svc = Service.create(svcQname);
			} finally {
				BusFactory.setThreadDefaultBus(bus);
			}
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_WS, MessageLevel.DETAIL)) {
				LogManager.logDetail(LogConstants.CTX_WS, "Creating a dispatch with endpoint", endpoint); //$NON-NLS-1$
			}
			svc.addPort(mcf.getPortQName(), binding, endpoint);
			
			dispatch = svc.createDispatch(mcf.getPortQName(), type, mode);
			
			if (mcf.getSecurityType() == WSManagedConnectionFactory.SecurityType.WSSecurity 
					&& mcf.getOutInterceptors() != null) {
				Client client = ((DispatchImpl)dispatch).getClient();
				Endpoint ep = client.getEndpoint();
				for (Interceptor i : mcf.getOutInterceptors()) {
					ep.getOutInterceptors().add(i);
				}
			}
		}
		
		if (mcf.getSecurityType() == WSManagedConnectionFactory.SecurityType.HTTPBasic){
			dispatch.getRequestContext().put(Dispatch.USERNAME_PROPERTY, mcf.getAuthUserName());
			dispatch.getRequestContext().put(Dispatch.PASSWORD_PROPERTY, mcf.getAuthPassword());
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
		return dispatch;
	}

	@Override
	public void close() throws ResourceException {
		
	}

}
