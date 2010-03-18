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

package org.teiid.jdbc;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * This warning class is sent when using partial results mode if one or more
 * sources fails.  In this case, results will be returned (with 0 rows sent from
 * failing source).  This warning can be obtained from the ResultSet to determine
 * which sources failed and provided 0 rows and why they failed.  
 */
public class PartialResultsWarning extends SQLWarning {

	private static final long serialVersionUID = 5301215068719177369L;
	private Map failures; 

    /**
     * Construct partial results warning.
     * @param reason Reason for the exception
     * @param SQLstate SQL state code
     * @param vendorCode Vendor code
     */
    public PartialResultsWarning(String reason, String SQLstate, int vendorCode) {
        super(reason, SQLstate, vendorCode);
    }

    /**
     * Construct partial results warning.
     * @param reason Reason for the exception
     * @param SQLstate SQL state code
     */
    public PartialResultsWarning(String reason, String SQLstate) {
        super(reason, SQLstate);
    }

    /**
     * Construct partial results warning.
     * @param reason Reason for the exception
     */
    public PartialResultsWarning(String reason) {
        super(reason);
    }

    /**
     * Construct partial results warning.
     */
    public PartialResultsWarning() {
        super();
    }
    
    /**
     * Add a connector failure to the warning
     * @param name Connector name
     * @param exception Connector exception
     */
    public void addConnectorFailure(String name, SQLException exception) {
        if(this.failures == null) {
            this.failures = new HashMap();
        }
        this.failures.put(name, exception);
    }
    
    /**
     * Obtain list of connectors that failed.  
     * @return List of connectors that failed - List contains String names
     */
    public Collection getFailedConnectors() {
        if(this.failures != null) {
            return new HashSet(this.failures.keySet());
        }
        return Collections.EMPTY_SET;
    }
    
    /**
     * Obtain failure for a particular connector.
     * @param name Connector name
     * @return Exception that occurred for this connector or null if
     * the exception was unknown
     */
    public SQLException getConnectorException(String connectorName) {
        if(this.failures != null) {
            return (SQLException) this.failures.get(connectorName);    
        }
        return null;
    }

}
