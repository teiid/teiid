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

package com.metamatrix.jdbc;

import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/** 
 * @since 5.0
 */
public abstract class BaseDriver implements Driver {

    /**
     * Get's the name of the driver.
     * @return name of the driver
     */
    public abstract String getDriverName();
    
    /**
     * This method returns true if the driver passes jdbc compliance tests.
     * @return true if the driver is jdbc compliant, else false.
     */
    public boolean jdbcCompliant() {
        return false;
    }
    
    /**
     * This method could be used to prompt the user for properties to connect to
     * metamatrix (properties that are not already specified for obtaining connection).
     * @param The URL used to establish a connection.
     * @param A properties object containing properties needed to obtain a connection.
     * @return An array containing DriverPropertyInfo objects
     * @throws SQLException, if parsing error occurs
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if(info == null) {
            info = new Properties();
        }

        // parse the url and update properties object
        parseURL(url, info);

        // construct list of driverPropertyInfo objects
        List<DriverPropertyInfo> driverProps = new LinkedList<DriverPropertyInfo>();

        DriverPropertyInfo dpi = new DriverPropertyInfo(BaseDataSource.VDB_NAME, info.getProperty(BaseDataSource.VDB_NAME));
        dpi.required = true;
        driverProps.add(dpi);

        driverProps.add(new DriverPropertyInfo(BaseDataSource.VDB_VERSION, info.getProperty(BaseDataSource.VDB_VERSION)));
        
        driverProps.addAll(getAdditionalPropertyInfo(url, info));
        
        // create an array of DriverPropertyInfo objects
        DriverPropertyInfo [] propInfo = new DriverPropertyInfo[driverProps.size()];

        // copy the elements from the list to the array
        return driverProps.toArray(propInfo);
    }
    
    protected abstract List<DriverPropertyInfo> getAdditionalPropertyInfo(String url, Properties info);
    
    protected abstract void parseURL(String url, Properties info) throws SQLException;

}