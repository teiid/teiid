package org.teiid.resource.adapter.salesforce;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPHeader;
import javax.xml.soap.SOAPHeaderElement;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;
import javax.xml.ws.handler.Handler;
import javax.xml.ws.handler.HandlerResolver;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.handler.PortInfo;
import javax.xml.ws.handler.soap.SOAPHandler;
import javax.xml.ws.handler.soap.SOAPMessageContext;

import com.sforce.soap.partner.SessionHeader;

/* Handler which adds the SessionId info to the SOAP Header
 * 
 */
class SalesforceHandlerResolver implements HandlerResolver {

	private SalesforceHeaderHandler headerHandler = null;

	public SalesforceHandlerResolver(SessionHeader sh) {
		this.headerHandler = new SalesforceHeaderHandler(sh);
	}

	public List<Handler> getHandlerChain(PortInfo portInfo) {
		List<Handler> handlerChain = new ArrayList<Handler>();

		handlerChain.add(this.headerHandler);

		return handlerChain;
	}
	
	class SalesforceHeaderHandler implements SOAPHandler<SOAPMessageContext> {

		SessionHeader sh = null;

	    public SalesforceHeaderHandler(SessionHeader sh) {
			this.sh = sh;
		}

	    public boolean handleMessage(SOAPMessageContext smc) {
	    	QName sessionHeader = new QName("urn:partner.soap.sforce.com", "SessionHeader");  //$NON-NLS-1$ //$NON-NLS-2$
	    	QName sessionId = new QName("urn:partner.soap.sforce.com", "sessionId");
	     	try {
	     		// If the SessionHeader is null, or the session id is null - do nothing.
	     		if (sh!=null && sh.getSessionId()!=null) {
	     			SOAPMessage message = smc.getMessage();
	     			SOAPPart part = message.getSOAPPart();
	     			SOAPEnvelope envelope = part.getEnvelope();
	     			SOAPHeader header = envelope.getHeader();
	     			if (header == null) header = envelope.addHeader();
	     			SOAPHeaderElement sessionHeaderElement = header.addHeaderElement(sessionHeader);
	     			SOAPElement sessionIdElement = sessionHeaderElement.addChildElement(sessionId);
	     			sessionIdElement.addTextNode(sh.getSessionId());
	     		}

			} catch (SOAPException e) {
				e.printStackTrace();
				return false;
			}
	    	return true;
	    }

		public Set getHeaders() {
	        return null;
	    }

	    public boolean handleFault(SOAPMessageContext context) {
	        return true;
	    }

	    public void close(MessageContext context) {
	    }
	}
}
