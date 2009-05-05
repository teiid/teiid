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

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.metadata.runtime.Element;

public abstract class ParameterDescriptor {

    private String m_xPath;
    private boolean m_param;
    private boolean m_responseId;
    private boolean m_location;
    private String m_columnName;
    private int m_columnNumber = -1;
    private Element m_element;

    public static final String PARM_INPUT_COLUMN_PROPERTY_NAME = "IsInputParameter"; //$NON-NLS-1$
    public static final String ROLE_COLUMN_PROPERTY_NAME = "Role"; //$NON-NLS-1$
    public static final String ROLE_COLUMN_PROPERTY_NAME_RESPONSE_IN = "ResponseIn"; //$NON-NLS-1$
    public static final String ROLE_COLUMN_PROPERTY_NAME_RESPONSE_OUT = "ResponseOut"; //$NON-NLS-1$
    public static final String ROLE_COLUMN_PROPERTY_NAME_LOCATION = "Location"; //$NON-NLS-1$
    public static final String ROLE_COLUMN_PROPERTY_NAME_DATA = "Data"; //$NON-NLS-1$

    public ParameterDescriptor( Element element ) throws ConnectorException {
        setElement(element);
        setIsParameter(testForParam(m_element));
        testRole();
        if (getElement().getNameInSource() != null) {
            setColumnName(getElement().getNameInSource());
        } else {
            setColumnName(getElement().getName().trim());
        }
        String nis = getElement().getNameInSource();
        if (nis != null) {
            nis = nis.trim();
        }
        setXPath(nis);
    }

    protected ParameterDescriptor() {
        setIsParameter(false);
        setIsResponseId(false);
        setIsLocation(false);
        setColumnName(null);
        setXPath(null);
    }

    public final void setXPath( String xPath ) {
        m_xPath = xPath;
    }

    public String getXPath() {
        return m_xPath;
    }

    public final void setIsParameter( boolean param ) {
        m_param = param;
    }

    public final void setIsResponseId( boolean responseId ) {
        m_responseId = responseId;
    }

    public final void setIsLocation( boolean location ) {
        m_location = location;
    }

    public final boolean isParameter() {
        return m_param;
    }

    public final boolean isResponseId() {
        return m_responseId;
    }

    public final boolean isLocation() {
        return m_location;
    }

    public final void setColumnName( String columnName ) {
        m_columnName = columnName;
    }

    public final String getColumnName() {
        return m_columnName;
    }

    public final void setColumnNumber( int columnNumber ) {
        m_columnNumber = columnNumber;
    }

    public final int getColumnNumber() {
        return m_columnNumber;
    }

    protected void setElement( Element elem ) {
        m_element = elem;
    }

    protected Element getElement() {
        return m_element;
    }

    protected static boolean testForParam( Element element ) throws ConnectorException {
        boolean param = false;
        param = Boolean.valueOf(element.getProperties().getProperty(PARM_INPUT_COLUMN_PROPERTY_NAME)).booleanValue();
        return param;
    }

    public String getRole() throws ConnectorException {
        return m_element.getProperties().getProperty(ROLE_COLUMN_PROPERTY_NAME);
    }

    protected void testRole() throws ConnectorException {
        String role = getRole();
        if (role == null) {
            setIsResponseId(false);
            setIsLocation(false);
        } else {
            if (role.equalsIgnoreCase(ROLE_COLUMN_PROPERTY_NAME_RESPONSE_IN)) {
                setIsResponseId(true);
                setIsLocation(false);
            } else if (role.equalsIgnoreCase(ROLE_COLUMN_PROPERTY_NAME_RESPONSE_OUT)) {
                setIsResponseId(true);
                setIsLocation(false);
            } else if (role.equalsIgnoreCase(ROLE_COLUMN_PROPERTY_NAME_LOCATION)) {
                setIsResponseId(false);
                setIsLocation(true);
            } else { // if (role.equalsIgnoreCase(ROLE_COLUMN_PROPERTY_NAME_DATA))
                setIsResponseId(false);
                setIsLocation(false);
            }
        }
    }
}
