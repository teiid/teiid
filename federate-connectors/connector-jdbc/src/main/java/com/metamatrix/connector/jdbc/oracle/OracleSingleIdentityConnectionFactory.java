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
package com.metamatrix.connector.jdbc.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.metamatrix.connector.jdbc.ConnectionListener;
import com.metamatrix.connector.jdbc.ConnectionQueryStrategy;
import com.metamatrix.connector.jdbc.ConnectionStrategy;
import com.metamatrix.connector.jdbc.DefaultConnectionListener;
import com.metamatrix.connector.jdbc.JDBCPlugin;
import com.metamatrix.connector.jdbc.JDBCSingleIdentityConnectionFactory;
import com.metamatrix.data.api.ConnectorEnvironment;

public class OracleSingleIdentityConnectionFactory extends JDBCSingleIdentityConnectionFactory{
    private String queryTest = "Select 'x' from DUAL"; //$NON-NLS-1$
    private ConnectionListener connectionListener = new OracleConnectionListener();
    
    protected ConnectionStrategy createConnectionStrategy() {
        return new ConnectionQueryStrategy(queryTest, this.sourceConnectionTestInterval);        
    }
  
    /**
     * @see com.metamatrix.connector.jdbc.JDBCSourceConnectionFactory#getConnectionListener()
     */
    protected ConnectionListener getConnectionListener() {
        return connectionListener;
    }
    
    /**
     * A connection listener strategy class, where gets called after the connection 
     * is created and before connection is terminated.
     */
    private static class OracleConnectionListener extends DefaultConnectionListener{
        //  Since this going to used inside a pool, we would like to report only once  
        boolean alreadyReportedDetails = false;  
                
        /**
         * log some debug information about the oracle driver being used.
         * defect request 13979 & 13978
         * @see com.metamatrix.connector.jdbc.ConnectionStrategy#afterConnectionCreation(java.sql.Connection)
         */
        public void afterConnectionCreation(Connection connection, ConnectorEnvironment env) {
            super.afterConnectionCreation(connection, env);
            
            if (alreadyReportedDetails) {                
                return;
            }
            
            alreadyReportedDetails = true;            
            String errorStr = JDBCPlugin.Util.getString("ConnectionListener.failed_to_report_oracle_connection_details"); //$NON-NLS-1$
            executeSQL(connection, env, "select * from v$instance", errorStr); //$NON-NLS-1$
        }

        /**
         * Execute any SQL aginst the connection
         * @param connection
         * @param env
         * @param sql
         */
        private void executeSQL(Connection connection, ConnectorEnvironment env, String sql, String errorStr) {    
            ResultSet rs = null;
            Statement stmt = null;
            try {                
                stmt = connection.createStatement();
                rs = stmt.executeQuery(sql); 
                
                
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    StringBuffer sb = new StringBuffer();
                    for (int i = 1; i <= columnCount; i++) {
                        sb.append(rs.getMetaData().getColumnName(i)).append("=").append(rs.getString(i)).append(";"); //$NON-NLS-1$ //$NON-NLS-2$
                    }                    
                    // log the queried information
                    env.getLogger().logInfo(sb.toString());                    
                }                
                
            } catch (SQLException e) {
                env.getLogger().logInfo(errorStr); 
            }finally {
                try {
                    if (rs != null) {
                        rs.close();
                    } 
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException e1) {
                    env.getLogger().logInfo(errorStr);
                }
            }
        }        
    }
}
