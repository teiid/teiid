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

package com.metamatrix.soap.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import javax.xml.soap.SOAPException;

import org.apache.axis2.context.MessageContext;

import com.metamatrix.soap.exceptions.SOAPProcessingException;
import com.metamatrix.soap.handler.ActionUpdateHandler;
import com.metamatrix.soap.service.DataServiceInfo;

/**
 * This strategy will translate the end point url into the MetaMatrix Server
 * connection properties.
 */
public class EndpointUriTranslatorStrategyImpl {

	private static final String URL_PREFIX = "jdbc:metamatrix:"; //$NON-NLS-1$

	private static final String URL_AT = "@"; //$NON-NLS-1$

	private static final String URL_SUFFIX = ";version="; //$NON-NLS-1$
	
	private static final String soapEncoding = "UTF-8"; //$NON-NLS-1$

	/**
	 * <p>
	 * This implemementation of the Uri translator will get the proper JDBC
	 * server url from the extra path information in the endpoint URL. NOTE that
	 * this method MUST be called from within the SOAP request processing thread
	 * that the URI endpoint is to be called from.
	 * </p>
	 * 
	 * <p>
	 * The prototype of the endpoint URL is as follows:
	 * </p>
	 * 
	 * <pre>
	 *       
	 *        &lt;protocol&gt;://&lt;Soap Server Host&gt;:&lt;Soap Server Port&gt;/&lt;Data Service endpoint path&gt;/&lt;MM Server Host Name&gt;/&lt;MM Server Host Port&gt;/&lt;MM target VDB Name&gt;/&lt;MM target VDB version (optional)&gt;
	 *        
	 * </pre>
	 * 
	 */
	public static DataServiceInfo getDataServiceInfo() throws SOAPProcessingException{

		DataServiceInfo info = new DataServiceInfo();

		/*
		 * Build the MetaMatrix server routing information required to make the
		 * subsequent call against the MetaMatrix Server.
		 */
		final MessageContext context = MessageContext
				.getCurrentMessageContext();
		final Map map = (Map) context
				.getProperty(ActionUpdateHandler.ENDPOINT_URI_KEY);

		String vdbContextualWebServiceName = (String) map.get(ActionUpdateHandler.VIRTUAL_PROCEDURE);

		info.setDataServiceFullPath(vdbContextualWebServiceName);

		final String serverUrl = (String)map.get(WSDLServletUtil.SERVER_URL_KEY);
		final String vdbName = (String) map.get(WSDLServletUtil.VDB_NAME_KEY);
		final String vdbVersion = (String) map
				.get(WSDLServletUtil.VDB_VERSION_KEY);
		final String additionalProps = (String) map
				.get(WSDLServletUtil.ADD_EXEC_PROPS);

		final String jdbcUrl = createJdbcUrl(serverUrl, vdbName == null ? null : vdbName, vdbVersion == null ? null : vdbVersion,
				additionalProps == null ? null : additionalProps);

		info.setServerURL(jdbcUrl);

		return info;
	}

	/**
	 * Create MM JDBC url value. 
	 * NOTE: Need to make sure the url is URL decoded. URL encoded urls will
	 * prevent us from connecting to the VDB. 
	 * @param serverUrl
	 * @param vdbName
	 * @param vdbVersion
	 * @param additionalProps
	 * @return
	 * @throws SOAPException
	 */
	protected static String createJdbcUrl(String serverUrl, String vdbName,
			String vdbVersion, String additionalProps) throws SOAPProcessingException{
		StringBuffer result = new StringBuffer();
		try {
			result = new StringBuffer(URL_PREFIX).append(URLDecoder.decode(vdbName,soapEncoding))
					.append(URL_AT).append(URLDecoder.decode(serverUrl,soapEncoding));
		
			if (vdbVersion != null) {
				result.append(URL_SUFFIX).append(vdbVersion);
			}
	
			if (additionalProps != null) {
				result.append(';').append(URLDecoder.decode(additionalProps,soapEncoding)); 
			}
		
		} catch (UnsupportedEncodingException e) {
			throw new SOAPProcessingException(e);					
		} 

		return result.toString();
	}
}