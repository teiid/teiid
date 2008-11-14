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

package com.metamatrix.connector.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import com.metamatrix.data.api.ConnectorEnvironment;

/**
 * A default connection listener object, on a given connection factory. 
 * @author Ramesh Reddy
 */
public class DefaultConnectionListener implements ConnectionListener {
    ConnectionListener customListener = null;
    
    //  Since this going to used inside a pool, we would like to report only once  
    boolean alreadyReportedDetails = false;  

    /**
     * defect request 13979 & 13978
     */
    public void afterConnectionCreation(Connection connection, ConnectorEnvironment env) {
        
        if (alreadyReportedDetails) {
            return;            
        }
        
        alreadyReportedDetails = true;
        // now dig some details about this driver/database for log.
        try {
            StringBuffer sb = new StringBuffer();
            DatabaseMetaData dbmd = connection.getMetaData();
            sb.append("Commit=").append(connection.getAutoCommit()); //$NON-NLS-1$
            sb.append(";DatabaseProductName=").append(dbmd.getDatabaseProductName()); //$NON-NLS-1$
            sb.append(";DatabaseProductVersion=").append(dbmd.getDatabaseProductVersion()); //$NON-NLS-1$
            sb.append(";DriverMajorVersion=").append(dbmd.getDriverMajorVersion()); //$NON-NLS-1$
            sb.append(";DriverMajorVersion=").append(dbmd.getDriverMinorVersion()); //$NON-NLS-1$
            sb.append(";DriverName=").append(dbmd.getDriverName()); //$NON-NLS-1$
            sb.append(";DriverVersion=").append(dbmd.getDriverVersion()); //$NON-NLS-1$
            sb.append(";IsolationLevel=").append(dbmd.getDefaultTransactionIsolation()); //$NON-NLS-1$
            
            env.getLogger().logInfo(sb.toString());
        } catch (SQLException e) {
            String errorStr = JDBCPlugin.Util.getString("ConnectionListener.failed_to_report_jdbc_connection_details"); //$NON-NLS-1$            
            env.getLogger().logInfo(errorStr); 
        }
        
        if (customListener != null) {
            customListener.afterConnectionCreation(connection, env);
        }
    }

    /**
     * defect request 13979 & 13978
     */
    public void beforeConnectionClose(Connection connection, ConnectorEnvironment env) {
        if (customListener != null) {
            customListener.beforeConnectionClose(connection, env);
        }        
    }
}
