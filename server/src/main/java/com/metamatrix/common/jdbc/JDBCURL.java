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

package com.metamatrix.common.jdbc;

import java.util.List;

import com.metamatrix.core.util.StringUtil;


/** 
 * @since 4.3
 */
public class JDBCURL {
    
    private static final String DELIMITER_AT = "@";//$NON-NLS-1$
    private static final String DELIMITER_FORWARDSLASH = "//"; //$NON-NLS-1$
    private static final String JDBC_PROTOCOL = "jdbc:"; //$NON-NLS-1$
    
    private String connectionURL;
    private String protocol;
    private String databaseInfo;
    
    public JDBCURL(String jdbcURL) {
        parseURL(jdbcURL);
        connectionURL = jdbcURL;
    }
    
    
    public String getConnectionURL() {
        return connectionURL;
    }
    
    public String getProtocol() {
        return protocol;
    }    
    
    public String getDataConnectionInfo() {
        return databaseInfo;
    }       
    
    
    private void parseURL(String jdbcURL) {
        if (jdbcURL == null) {
            throw new IllegalArgumentException();
        }
        // Trim extra spaces
        jdbcURL = jdbcURL.trim();
        if (jdbcURL.length() == 0) {
            throw new IllegalArgumentException();
        }             
        
        int delimiter = jdbcURL.indexOf(DELIMITER_AT);
        if (delimiter > 0) {
            parseWithAt(jdbcURL);
        } else {
            parseForSlash(jdbcURL);
        }
        
        
    }
    
    private void parseForSlash(String jdbcURL) {
        
        parseOnDelimiter(jdbcURL, DELIMITER_FORWARDSLASH);
    }
    private void parseOnDelimiter(String jdbcURL, String delim) {
        List urlParts = StringUtil.splitOnEntireString(jdbcURL, delim);
       // String[] urlParts = split(jdbcURL, DELIMITER_FORWARDSLASH);
        if (urlParts.size() != 2) {
            throw new IllegalArgumentException();
        }
        parseJDBCProtocol((String) urlParts.get(0));
        parseConnectionPart((String)urlParts.get(1));
    }    
    
    private void parseWithAt(String jdbcURL) {
        parseOnDelimiter(jdbcURL, DELIMITER_AT);
    }

    private void parseJDBCProtocol(String protocolurl) {
        if (!protocolurl.startsWith(JDBC_PROTOCOL)) {
            throw new IllegalArgumentException();
        }
        if (protocolurl.length() == JDBC_PROTOCOL.length()) {
            throw new IllegalArgumentException();
        }
        protocol = protocolurl.substring(JDBC_PROTOCOL.length());
    }
    
    private void parseConnectionPart(String connectionInfo) {
        databaseInfo = connectionInfo;
        
    }
        
    public String getJDBCURL() {
        return connectionURL;
    }
    
    public String toString() {
        return getJDBCURL();
    }
        
    

}
