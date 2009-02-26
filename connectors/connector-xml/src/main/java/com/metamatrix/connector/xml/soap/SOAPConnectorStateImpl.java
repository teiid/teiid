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



package com.metamatrix.connector.xml.soap;

import java.text.MessageFormat;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.base.Messages;
import com.metamatrix.connector.xml.http.HTTPConnectorState;

/**
 * Contains the data needed to create the SOAP Envelope around an XML Document.
 * Gathers that data from a Properties derived from a connector binding.  This
 * class is not intended to be used as a base, but should be used in composition
 * of a state class.
 */
public class SOAPConnectorStateImpl implements SOAPConnectorState {
	
	public static final String ENCODING_STYLE_PROPERTY_NAME = "EncodingStyle"; //NON-NSL-$1 //$NON-NLS-1$
	public static final String RPC_ENC_STYLE = "RPC - Encoded"; //$NON-NLS-1$
	public static final String RPC_LITERAL_STYLE = "RPC - Literal"; //$NON-NLS-1$
	public static final String DOC_ENCODED_STYLE = "Document - Encoded"; //$NON-NLS-1$
	public static final String DOC_LITERAL_STYLE = "Document - Literal"; //$NON-NLS-1$
	public static final String CONNECTOR_EXCEPTION_ON_SOAP_FAULT = "ExceptionOnSOAPFault"; //$NON-NLS-1$
	
	private boolean m_encoded;
	private boolean m_RPC;
	private boolean m_exceptionOnFault; // throw connector exception by default
	private ConnectorLogger logger;
	private String hostnameVerifierClassName;

    public SOAPConnectorStateImpl() {
        setEncoded(true); //default to RPC/Encoded
        setRPC(true);
        setExceptionOnFault(false);
    }
    
	private boolean isNotNullOrEmpty(String value) {
		return (value != null && !value.equals(""));
	}
    
    public void setState(ConnectorEnvironment env) throws ConnectorException {
		// set the encoding style
		Properties props = env.getProperties();
		String enc = props.getProperty(ENCODING_STYLE_PROPERTY_NAME);
		if (isNotNullOrEmpty(enc)) {
			if (enc.equalsIgnoreCase(RPC_ENC_STYLE)
					|| enc.equalsIgnoreCase(RPC_LITERAL_STYLE)
					|| enc.equalsIgnoreCase(DOC_ENCODED_STYLE)
					|| enc.equalsIgnoreCase(DOC_LITERAL_STYLE)) {
				if (enc.equalsIgnoreCase(RPC_ENC_STYLE)
						|| enc.equalsIgnoreCase(RPC_LITERAL_STYLE)) {
					setRPC(true);
				} else {
					setRPC(false);
				}
				if (enc.equalsIgnoreCase(RPC_ENC_STYLE)
						|| enc.equalsIgnoreCase(DOC_ENCODED_STYLE)) {
					setEncoded(true);
				} else {
					setEncoded(false);
				}
			} else {
				String rawMsg = Messages
						.getString("SOAPConnectorStateImpl.invalid.ENCODING_STYLE_PROPERTY_NAME");
				Object[] objs = new Object[4];
				objs[0] = RPC_ENC_STYLE;
				objs[1] = RPC_LITERAL_STYLE;
				objs[2] = DOC_ENCODED_STYLE;
				objs[3] = DOC_LITERAL_STYLE;
				String msg = MessageFormat.format(rawMsg, objs);
				throw new ConnectorException(msg);
			}
		} else {
			throw new ConnectorException(
					Messages.getString("SOAPConnectorStateImpl.empty.ENCODING_STYLE_PROPERTY_NAME"));
		}
    	   
    	String strExceptionOnFault = props.getProperty(CONNECTOR_EXCEPTION_ON_SOAP_FAULT);
    	if (isNotNullOrEmpty(strExceptionOnFault)) { 
    		boolean exOnFault = false;
    		exOnFault = Boolean.valueOf(strExceptionOnFault).booleanValue();
    		setExceptionOnFault(exOnFault);
    	} else {
    		throw new ConnectorException(Messages.getString("SOAPConnectorStateImpl.empty.CONNECTOR_EXCEPTION_ON_SOAP_FAULT"));
    	}
    	
        setHostnameVerifierClassName(props.getProperty(HTTPConnectorState.HOSTNAME_VERIFIER));
        if(getHostnameVerifierClassName() != null) {
        	try {
        		Class clazz = Thread.currentThread().getContextClassLoader().loadClass(getHostnameVerifierClassName());
				HostnameVerifier verifier = (HostnameVerifier) clazz.newInstance();
				HttpsURLConnection.setDefaultHostnameVerifier(verifier);
			} catch (Exception e) {
				throw new ConnectorException(e, "Unable to load HostnameVerifier");
    		}
        }
    }
    
    public Properties getState() {
    	Properties props = new Properties();
    	String style = null;
    	if(isRPC()) {
    		if(isEncoded()) {
    			style = RPC_ENC_STYLE;
    		} else {
    			style = RPC_LITERAL_STYLE;
    		}    		
    	} else {
    		if(isEncoded()) {
    			style = DOC_ENCODED_STYLE;
    		} else {
    			style = DOC_LITERAL_STYLE;
    		}
    	}
    	props.setProperty(ENCODING_STYLE_PROPERTY_NAME, style);   
    	props.setProperty(CONNECTOR_EXCEPTION_ON_SOAP_FAULT, Boolean.toString(isExceptionOnFault()));
    	return props;
    }
    
	private void setEncoded(boolean encoded) {
		m_encoded = encoded;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.SOAPConnectorState#isEncoded()
	 */
	public boolean isEncoded() {
		return m_encoded;
	}

	private void setRPC(boolean rpc) {
		m_RPC = rpc;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.SOAPConnectorState#isRPC()
	 */
	public boolean isRPC() {
		return m_RPC;
	}

	private void setExceptionOnFault(boolean exceptionOnFault) {
		m_exceptionOnFault = exceptionOnFault;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.SOAPConnectorState#isExceptionOnFault()
	 */
	public boolean isExceptionOnFault() {
		return m_exceptionOnFault;
	}

	public void setLogger(ConnectorLogger logger) {
		this.logger = logger;
	}

	public ConnectorLogger getLogger() {
		return logger;
	}

    private void setHostnameVerifierClassName(String property) {
		this.hostnameVerifierClassName = property;
	}
	
	private String getHostnameVerifierClassName() {
		return hostnameVerifierClassName;
	}
}
