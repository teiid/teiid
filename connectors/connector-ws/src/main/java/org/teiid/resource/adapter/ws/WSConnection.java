package org.teiid.resource.adapter.ws;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.resource.ResourceException;
import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.AsyncHandler;
import javax.xml.ws.Binding;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Dispatch;
import javax.xml.ws.EndpointReference;
import javax.xml.ws.Response;
import javax.xml.ws.Service;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;
import javax.xml.ws.soap.SOAPBinding;

import org.jboss.ws.core.ConfigProvider;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.ws.WSManagedConnectionFactory.InvocationType;
import org.teiid.resource.spi.BasicConnection;

public class WSConnection extends BasicConnection implements Dispatch<Source>{
	private static QName svcQname = new QName("http://teiid.org", "teiid"); //$NON-NLS-1$ //$NON-NLS-2$
	private static QName portQName = new QName("http://teiid.org", "teiid");//$NON-NLS-1$ //$NON-NLS-2$
	
	private Dispatch<Source> delegate;
	private WSManagedConnectionFactory mcf;

	public WSConnection(WSManagedConnectionFactory mcf) {
		this.mcf = mcf;
		if (mcf.getInvocationType() == WSManagedConnectionFactory.InvocationType.HTTP_GET) {
			this.delegate = createHTTPDispatch("GET"); //$NON-NLS-1$
		}
		else if (mcf.getInvocationType() == WSManagedConnectionFactory.InvocationType.HTTP_POST) {
			this.delegate = createHTTPDispatch("POST"); //$NON-NLS-1$
		} else {
			this.delegate = createSOAPDispatch();
		}
	}

	private Dispatch<Source> createHTTPDispatch(String requestMethod){
		Service svc = Service.create(svcQname);
		svc.addPort(portQName, HTTPBinding.HTTP_BINDING, mcf.getEndPoint());

		Dispatch<Source> dispatch = svc.createDispatch(portQName, Source.class, Service.Mode.PAYLOAD);
		dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_METHOD, requestMethod);
		if (mcf.getSecurityType() == WSManagedConnectionFactory.SecurityType.HTTPBasic){
			dispatch.getRequestContext().put(Dispatch.USERNAME_PROPERTY, mcf.getAuthUserName());
			dispatch.getRequestContext().put(Dispatch.PASSWORD_PROPERTY, mcf.getAuthPassword());
		}
		        
        Map<String, List<String>> httpHeaders = (Map<String, List<String>>)dispatch.getRequestContext().get(MessageContext.HTTP_REQUEST_HEADERS);
        if(httpHeaders == null) {
        	httpHeaders = new HashMap<String, List<String>>();
        }
        httpHeaders.put("Content-Type", Collections.singletonList("text/xml; charset=utf-8"));//$NON-NLS-1$ //$NON-NLS-2$
        httpHeaders.put("User-Agent", Collections.singletonList("Teiid Server"));//$NON-NLS-1$ //$NON-NLS-2$
        dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_HEADERS, httpHeaders);
        		
		return dispatch;
	}

	private Dispatch<Source> createSOAPDispatch() {
		Service svc = Service.create(svcQname);
		
		svc.addPort(portQName, mcf.getInvocationType() == InvocationType.SOAP11 ? SOAPBinding.SOAP11HTTP_BINDING : SOAPBinding.SOAP12HTTP_BINDING, mcf.getEndPoint());

		Dispatch<Source> dispatch = svc.createDispatch(portQName, Source.class, Service.Mode.PAYLOAD);
		if (mcf.getSecurityType() == WSManagedConnectionFactory.SecurityType.WSSecurity) {
			//  JBoss WS-Security
			((ConfigProvider) this.delegate).setSecurityConfig(mcf.getWsSecurityConfigURL());
			((ConfigProvider) delegate).setConfigName(mcf.getWsSecurityConfigName());
		}
		return dispatch;
	}	
	
	public Binding getBinding() {
		return delegate.getBinding();
	}

	public EndpointReference getEndpointReference() {
		return delegate.getEndpointReference();
	}

	public <T extends EndpointReference> T getEndpointReference(Class<T> clazz) {
		return delegate.getEndpointReference(clazz);
	}

	public Map<String, Object> getRequestContext() {
		return delegate.getRequestContext();
	}

	public Map<String, Object> getResponseContext() {
		return delegate.getResponseContext();
	}

	
	/**
	 * NOTE: the source msg from the teiid translator going to be SOAP. If execution is different from
	 * soap then fix it now.
	 */
	@Override
	public Source invoke(Source msg) {
		
		String xmlPayload = null;
        Map<String, Object> map = (Map)getRequestContext().get(MessageContext.INBOUND_MESSAGE_ATTACHMENTS);
        if (map != null) {
        	xmlPayload = (String)map.remove("xml"); //$NON-NLS-1$
        }

		if (this.mcf.getInvocationType() == WSManagedConnectionFactory.InvocationType.HTTP_GET) {
			if (this.mcf.getXMLParamName() != null) {
				if (xmlPayload != null) {
					try {
						getRequestContext().put(MessageContext.QUERY_STRING, this.mcf.getXMLParamName()+"="+URLEncoder.encode(xmlPayload, "UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
					} catch (UnsupportedEncodingException e) {
						throw new RuntimeException(e);
					} 
				} else {
					LogManager.logDetail(LogConstants.CTX_CONNECTOR, "XML Param specified, but no request document was generated."); //$NON-NLS-1$
				}
			}
			else if (getRequestContext().get(BindingProvider.ENDPOINT_ADDRESS_PROPERTY) == null) {
					
				String path = (String)getRequestContext().get(MessageContext.PATH_INFO);
				String queryString = (String)getRequestContext().get(MessageContext.QUERY_STRING);
				String url = this.mcf.getEndPoint();
				if (path != null) {
					url = url + "/" + path; //$NON-NLS-1$
				}
				if (queryString != null) {
					url = url + "?" + queryString; //$NON-NLS-1$
				}
				getRequestContext().put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY, url);
			}
    		msg = null;
		}
		else if (this.mcf.getInvocationType() == WSManagedConnectionFactory.InvocationType.HTTP_POST) {
			getRequestContext().put(MessageContext.QUERY_STRING, ""); //$NON-NLS-1$
			if (xmlPayload != null) {
				msg = new StreamSource(new StringReader(xmlPayload));
			}
			else {
				msg = null;
			}
		}
		else {
			// JBossWS native adds the null based address property somewhere and results in error if this 
			// is corrected
			if (getRequestContext().get(BindingProvider.ENDPOINT_ADDRESS_PROPERTY) == null) {
				getRequestContext().remove(BindingProvider.ENDPOINT_ADDRESS_PROPERTY);
			}
		}
	
		if (msg == null) {
			// JBoss Native DispatchImpl throws exception when the source is null
			msg = new StreamSource(new StringReader("<none/>")); //$NON-NLS-1$
		}
		return delegate.invoke(msg);
	}
	
	@Override
	public Future invokeAsync(Source msg, AsyncHandler<Source> handler) {
		return delegate.invokeAsync(msg, handler);
	}

	@Override
	public Response<Source> invokeAsync(Source msg) {
		return delegate.invokeAsync(msg);
	}

	@Override
	public void invokeOneWay(Source msg) {
		delegate.invokeOneWay(msg);
	}

	@Override
	public void close() throws ResourceException {
		this.delegate = null;
	}
}
