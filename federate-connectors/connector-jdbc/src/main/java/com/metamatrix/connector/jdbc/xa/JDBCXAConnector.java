/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

/*
 */
package com.metamatrix.connector.jdbc.xa;

import java.util.List;
import java.util.Properties;

import com.metamatrix.connector.jdbc.JDBCConnector;
import com.metamatrix.connector.jdbc.JDBCPropertyNames;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.api.ConnectorAnnotations.ConnectionPooling;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.xa.api.TransactionContext;
import com.metamatrix.data.xa.api.XAConnection;
import com.metamatrix.data.xa.api.XAConnector;

@ConnectionPooling
public class JDBCXAConnector extends JDBCConnector implements XAConnector{

    public void start() throws ConnectorException {
        Properties appEnvProps = environment.getProperties();          

        // Get and parse URL for some DataSource properties - add to connectionProps
        final String url = appEnvProps.getProperty(JDBCPropertyNames.URL);
        if ( url == null || url.trim().length() == 0 ) {
            throw new ConnectorException("Missing required property: " + JDBCPropertyNames.URL); //$NON-NLS-1$
        }
        
        parseURL(url, appEnvProps);

        super.start();

        //TODO: this assumes single identity support
        Connection conn = this.getXAConnection(null, null);
        conn.release();
    }
    
    /*
     * @see com.metamatrix.data.api.xa.XAConnector#getXAConnection(com.metamatrix.data.api.SecurityContext)
     */
    public XAConnection getXAConnection(SecurityContext context, final TransactionContext transactionContext) throws ConnectorException {
    	return (XAConnection)this.getConnection(context);
    }

    /**
     * Parse URL for DataSource connection properties and add to connectionProps.
     * @param url
     * @param connectionProps
     */
    static void parseURL(final String url, final Properties connectionProps) {
        // Will be: [jdbc:mmx:dbType://aHost:aPort], [DatabaseName=aDataBase], [CollectionID=aCollectionID], ...
        final List urlParts = StringUtil.split(url, ";"); //$NON-NLS-1$

        // Will be: [jdbc:mmx:dbType:], [aHost:aPort]
        final List protoHost = StringUtil.split((String)urlParts.get(0), "//"); //$NON-NLS-1$

        // Will be: [aHost], [aPort]
        final List hostPort = StringUtil.split((String) protoHost.get(1), ":"); //$NON-NLS-1$
        connectionProps.setProperty(XAJDBCPropertyNames.SERVER_NAME, (String)hostPort.get(0));
        connectionProps.setProperty(XAJDBCPropertyNames.PORT_NUMBER, (String)hostPort.get(1));

        // For "databaseName", "SID", and all optional props
        // (<propName1>=<propValue1>;<propName2>=<propValue2>;...)
        for ( int i = 1; i < urlParts.size(); i++ ) {
            final String nameVal = (String) urlParts.get( i );
            // Will be: [propName], [propVal]
            final List aProp = StringUtil.split(nameVal, "="); //$NON-NLS-1$
            if ( aProp.size() > 1) {
                final String propName = (String) aProp.get(0);
                if ( propName.equalsIgnoreCase(XAJDBCPropertyNames.DATABASE_NAME) ) {
                    connectionProps.setProperty(XAJDBCPropertyNames.DATABASE_NAME, (String) aProp.get(1));
                } else {
                    // Set optional prop names lower case so that we can find
                    // set method names for them when we introspect the DataSource
                    connectionProps.setProperty(propName.toLowerCase(), (String) aProp.get(1));
                }
            }
        }
    }
}
