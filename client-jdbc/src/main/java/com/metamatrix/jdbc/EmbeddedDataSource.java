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

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.common.protocol.URLHelper;

/**
 * @since 4.3
 */
public class EmbeddedDataSource extends BaseDataSource {
	
    //*************************** EmbeddedDataSource Specific Properties
    /**
     * configFile - 
     * The path and file name to which embedded DQP configuration info will be read. This property is <i>optional</i>; if none is
     * specified, then embedded DQP access cannot be used.
     */
    private String bootstrapFile;
    
    //  string constant for the embedded configuration file property
    public static final String DQP_BOOTSTRAP_FILE = "bootstrapFile"; //$NON-NLS-1$
    
    // The driver used to connect
    private final transient EmbeddedDriver driver = new EmbeddedDriver();
    
    /**
     * Constructor for EmbeddedDataSource.
     */
    public EmbeddedDataSource() {
        
    }
    
    protected Properties buildProperties(final String userName,
                                         final String password) {
        Properties props = super.buildProperties(userName, password);

    	if (this.getBootstrapFile().equals(EmbeddedDriver.getDefaultConnectionURL())) {
    		props.put("vdb.definition", getDatabaseName() +".vdb"); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	props.put(DQP_BOOTSTRAP_FILE, this.bootstrapFile);
        return props;
    }

    protected void validateProperties(final String userName,
                                      final String password) throws java.sql.SQLException {
        super.validateProperties(userName, password);

        // we do not have bootstrap file, make sure we have a default one.
        if (getBootstrapFile() == null && getDatabaseName() != null) {
            setBootstrapFile(EmbeddedDriver.getDefaultConnectionURL());
        }
        
        String reason = reasonWhyInvalidConfigFile(this.bootstrapFile);
        if (reason != null) {
            throw new SQLException(reason);
        }
    }

    /**
     * Return the reason why the supplied config file may be invalid, or null if it is considered valid.
     * 
     * @param configFile
     *            a possible value for the property
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setBootstrapFile(String)
     */
    public static String reasonWhyInvalidConfigFile(final String configFile) {
        if(configFile == null) {
            return getResourceMessage("EmbeddedDataSource.The_configFile_property_is_null"); //$NON-NLS-1$
        }
        
        try {
            URL url = URLHelper.buildURL(configFile);
            url.openStream();
        } catch (Exception e) {
            return getResourceMessage("EmbeddedDataSource.The_configFile_does_not_exist_or_cant_be_read"); //$NON-NLS-1$
        }        
        return null;
    }

    /**
     * @see com.metamatrix.jdbc.BaseDataSource#getConnection(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public Connection getConnection(String userName, String password) throws SQLException {
        validateProperties(userName, password);
        final Properties props = buildProperties(userName, password);
        return this.driver.createConnection(props);
     }
    
    /**
     * Returns the path and file name from which embedded DQP configuration information will be read.
     * 
     * @return the name of the config file for this data source; may be null
     */
    public String getBootstrapFile() {
        return bootstrapFile;
    }

    /**
     * Sets file name from which embedded DQP configuration information * will be read.
     * 
     * @param configFile
     *            The name of the config file name to set
     */
    public void setBootstrapFile(final String configFile) {
        this.bootstrapFile = configFile;
    }
            
}
