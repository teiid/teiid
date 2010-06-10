package org.teiid.resource.adapter.ws;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;
import javax.xml.namespace.QName;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.jboss.ws.core.ConfigProvider;
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
	private static QName svcQname = new QName("http://teiid.org", "teiid"); //$NON-NLS-1$ //$NON-NLS-2$
	private static QName portQName = new QName("http://teiid.org", "teiid");//$NON-NLS-1$ //$NON-NLS-2$
	
	private WSManagedConnectionFactory mcf;
	
	public WSConnectionImpl(WSManagedConnectionFactory mcf) {
		this.mcf = mcf;
	}

	public <T> Dispatch<T> createDispatch(String binding, String endpoint, Class<T> type, Mode mode) {
		Service svc = Service.create(svcQname);
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
		}
		
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_WS, MessageLevel.DETAIL)) {
			LogManager.logDetail(LogConstants.CTX_WS, "Creating a dispatch with endpoint", endpoint); //$NON-NLS-1$
		}
		
		svc.addPort(portQName, binding, endpoint);

		Dispatch<T> dispatch = svc.createDispatch(portQName, type, mode);
		
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
		} else if (mcf.getSecurityType() == WSManagedConnectionFactory.SecurityType.WSSecurity) {
			//  JBoss WS-Security
			((ConfigProvider) dispatch).setSecurityConfig(mcf.getWsSecurityConfigURL());
			((ConfigProvider) dispatch).setConfigName(mcf.getWsSecurityConfigName());
		}
		return dispatch;
	}

	@Override
	public void close() throws ResourceException {
		
	}

}
