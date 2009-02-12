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

package com.metamatrix.soap.handler;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletRequest;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMNamespace;
import org.apache.axiom.om.impl.OMNamespaceImpl;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axiom.soap.SOAPHeader;
import org.apache.axis2.AxisFault;
import org.apache.axis2.addressing.EndpointReference;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.Handler;
import org.apache.axis2.handlers.AbstractHandler;
import org.apache.axis2.transport.http.HTTPConstants;

import com.metamatrix.common.util.WSDLServletUtil;
import com.metamatrix.soap.exceptions.SOAPProcessingException;
import com.metamatrix.soap.util.WebServiceUtil;

/**
 * This class is the handler for MetaMatrix Web Services to update the SOAP
 * Action value and save the virtual procedure name off to the <@link
 * org.apache.axis2.context.MessageContext> as a property to be accessed in the
 * endpoint web service.
 */
public class ActionUpdateHandler extends AbstractHandler implements Handler {

	// Static constant for the MetaMatrix Web operation name
	public static final String MM_WEB_SERVICE_OPERATION = "executeDataService"; //$NON-NLS-1$

	// Static constant for the MetaMatrix Web Service EPR path
	public static final String MM_WEB_SERVICE = "services/service"; //$NON-NLS-1$
	
	// Static constant for the (pre-5.5) MetaMatrix Web Service EPR path
	public static final String MM_PRE55_WEB_SERVICE = "DataService"; //$NON-NLS-1$

	//Static constant for the MetaMatrix Web Service path
	public static final String MM_WEB_SERVICE_NAME = "service"; //$NON-NLS-1$

	// Static constant for the virtual procedure property
	public static final String VIRTUAL_PROCEDURE = "procedure"; //$NON-NLS-1$
	
	// Static constant representing an ampersand
	public static final String AMP = "&"; //$NON-NLS-1$
	
	// Static constant representing an equal sign
	public static final String EQ = "="; //$NON-NLS-1$
	
	// Static constant representing a semi colon
	public static final String SEMI = ";"; //$NON-NLS-1$

	// Static constant for the Endpoint URI value. This value contains
	// the parameters required to connect to the MM Server.
	public static final String ENDPOINT_URI_KEY = "endpointURI"; //$NON-NLS-1$
	
	// Static constant for SOAPEncoding
	private static final String soapEncoding = "UTF-8"; //$NON-NLS-1$
	
	//Static constant for the OMNamespace used to store the jdbc connection properties in the SOAP Header
	public static final	OMNamespace jdbcPropNS = new OMNamespaceImpl("http://com.metamatrix/jdbc/property", "mmx"); //$NON-NLS-1$ //$NON-NLS-2$
 
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.axis2.engine.Handler#invoke(org.apache.axis2.context.MessageContext)
	 */
	public InvocationResponse invoke(MessageContext messageContext)
			throws AxisFault {
		// If this is a dataservice call (Enterprise or Dimension), get the
		// procedure
		// name and update the soap action accordingly.
		final EndpointReference endpoint = messageContext.getTo();
		boolean isDataService = false;
		if (endpoint!=null){
			boolean pre55Endpoint = endpoint.getAddress().indexOf(MM_PRE55_WEB_SERVICE)>-1;
			isDataService = ((pre55Endpoint) || (endpoint.getAddress().indexOf(MM_WEB_SERVICE)>-1));
			//Update pre-5.5 dataservice endpoint
			if (pre55Endpoint){
				String updatedEndpoint = endpoint.getAddress();
				updatedEndpoint=updatedEndpoint.replaceAll(MM_PRE55_WEB_SERVICE, MM_WEB_SERVICE);
				messageContext.setTo(new EndpointReference(updatedEndpoint));
			}
		}else{
			if (messageContext.getAxisService()!=null){
				isDataService = MM_WEB_SERVICE_NAME.equals(messageContext.getAxisService().getName()); 
			}
		}

		/*
		 * If this is a dataservice invocation, we will interrogate the
		 * SOAPAction for the virtual procedure value. If that is not sourced
		 * (as may be the case with JMS transport), we will check the WSAAction
		 * value.
		 */

		if (isDataService) {
			String action = messageContext.getOptions().getAction();
				try {
					WebServiceUtil.validateActionIsSet(action);
				} catch (SOAPProcessingException e) {
					throw new AxisFault(e.getMessage());
				}
		
			ServletRequest request = (ServletRequest) messageContext
					.getProperty(HTTPConstants.MC_HTTP_SERVLETREQUEST);
			Map parms = getServerPropertyMap(request, messageContext); 
			
			messageContext.setProperty(ENDPOINT_URI_KEY, parms);
			messageContext.getOptions().setAction(MM_WEB_SERVICE_OPERATION);
			
		}
		
		return InvocationResponse.CONTINUE;
	}

	/**
	 * @param request
	 * @return
	 */
	protected static Map getServerPropertyMap(ServletRequest request, MessageContext messageContext) {
		Map valueMap = null;
		Map updatedParms = null;
			
		final String action = messageContext.getOptions().getAction();
				
		/*
		 * Check for post 5.0.3 WSDL and source the values accordingly. 5.5 WSDL contains server 
		 * properties in the action value in addition to the fully qualified stored procedure whereas 
		 * previous versions of the WSDL only contained the fully qualified stored procedure.
		 */
		if (action.indexOf(AMP+VIRTUAL_PROCEDURE+EQ)>-1){
			valueMap = getValueMap(action);
		}
		
		
		/* Check the ServletRequest parameter map. If it's not
		 * null we need to get the map and determine what the virtual procedure
		 * to be executed is. Prior to 5.5, this was always the action value. With 
		 * the 5.5 release, the action contains the server properties as well as
		 * the procedure name. We need to handle both cases in order to support
		 * pre-5.5 WSDL.
		 */		
		if (request != null){
			updatedParms = new HashMap(request.getParameterMap());
			Iterator paramIter = updatedParms.keySet().iterator();
			
			/* Replace String[] values with Strings. This allows us to be consistant 
			 * with non-HTTP transport where the values will be saved to the map
			 * as Strings and retrieved as such in EndpointUriTranslatorStrategyImpl.
			 */
			while (paramIter.hasNext()){
				Object key = paramIter.next();
				String[] parameter = (String[])updatedParms.get(key);
				updatedParms.put(key, parameter[0]);				
			}
			
				if (valueMap != null){
					/*
					 * Update the map with the fully qualified procedure 
					 * name in the map from values created from the action string. 
					 */
					updatedParms = new HashMap(valueMap);
				}else{
					/*
					 * Update the map with the fully qualified procedure 
					 * name in the action string. 
					 */
					updatedParms.put(VIRTUAL_PROCEDURE,action);			
				}
		}else if (action != null && valueMap != null){ 
				/*
				 * This is a non-http transport since the request is null.
				 * Update the map with the fully qualified procedure 
				 * name in the map from values created from the action string. 
				 */
				updatedParms = new HashMap(valueMap);						
		}
		
		/*
		 * Add any additional JDBC connection properties passed in via the SOAP Header
		 */
		SOAPEnvelope envelope = messageContext.getEnvelope();
		SOAPHeader header = null;
		Iterator soapHeaderIter = null;
		Map jdbcPropMap = new HashMap();
		if (envelope!=null){
			header = envelope.getHeader();
			if (header!=null){
				soapHeaderIter = header.getChildElements();
				getJDBCProps(soapHeaderIter, jdbcPropMap);
			}
		}	
			
		if (jdbcPropMap.size()>0){
			String name = null;
			String value = null;
			StringBuffer additionalProperties = new StringBuffer();
			Iterator propIter=jdbcPropMap.keySet().iterator();
			while (propIter.hasNext()){
				name=(String)propIter.next();
				value=(String)jdbcPropMap.get(name);
				additionalProperties.append(name).append(EQ).append(value);
				additionalProperties.append(SEMI);
			}
			try {
				additionalProperties=new StringBuffer(URLEncoder.encode(additionalProperties.toString(),soapEncoding));
			} catch (UnsupportedEncodingException e) {
				//should never happen
			}
			updatedParms.put(WSDLServletUtil.ADD_EXEC_PROPS,additionalProperties.toString());
		}
					
		return updatedParms;				
	}

	private static void getJDBCProps(Iterator soapHeaderIter, Map jdbcPropMap) {
		while (soapHeaderIter.hasNext()){
			OMElement element = (OMElement)soapHeaderIter.next();
			OMNamespace ns = element.getNamespace();
			if (ns.equals(jdbcPropNS)){
				jdbcPropMap.put(element.getLocalName(),element.getText());
			}
		}
	}

	
	/**
	 * @param parms
	 * @param action
	 */
	private static Map getValueMap(String action) {
		String[] valuesArray = action.split(AMP);
		Map valueMap = new HashMap();
		
		for (int i=0;i<valuesArray.length;i++){
			String value = valuesArray[i];
			String[] valueSet = value.split(EQ);
			if (valueSet.length==1){
				valueMap.put(valueSet[0],null);
			}else{
				valueMap.put(valueSet[0],valueSet[1]);
			}			
		}
		return valueMap;
	}
}
