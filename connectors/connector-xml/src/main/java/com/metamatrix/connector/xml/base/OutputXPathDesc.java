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


package com.metamatrix.connector.xml.base;

import java.text.MessageFormat;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.metadata.runtime.Element;

public class OutputXPathDesc extends ParameterDescriptor {

	// Describes an item in the xpath structure, and all the attributes of it
	// Added to allow all the attributes of an Xpath to be stored in a single structure,
	// and to make the code more manageable	
	private Object m_currentValue = null;
	private Class m_dataType = null;
   

	/**
	 * @see com.metamatrix.server.datatier.SynchConnectorConnection#submitRequest(java.lang.Object)
	 */
	public OutputXPathDesc (Element myElement)	throws ConnectorException {
		super(myElement);
		m_dataType = myElement.getJavaType();
		if (getXPath() == null) {
            if (!isSpecial()) {
            	String rawMsg = Messages.getString("OutputXPathDesc.name.in.source.required.on.column"); //$NON-NLS-1$
            	String msg = MessageFormat.format(rawMsg, new Object[] {getColumnName()});
            	throw new ConnectorException(msg);
            }
        }
	}

	public boolean isSpecial() {
		return isParameter() || isResponseId() || isLocation();
	}

	public OutputXPathDesc (ILiteral literal) throws ConnectorException {
		super();
		if (literal.getValue() == null) {
			setCurrentValue(null);
        } else {
			setCurrentValue(literal.getValue().toString());
        }
		m_dataType = literal.getType();
	}


	public void setCurrentValue(Object obj) {
		m_currentValue = obj;
	}

	public Object getCurrentValue() {
		return m_currentValue;
	}
   

	public Class getDataType() throws ConnectorException {
		return m_dataType;
	}

}
