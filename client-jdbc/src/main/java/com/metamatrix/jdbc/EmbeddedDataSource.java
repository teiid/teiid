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

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.common.protocol.URLHelper;

/**
 * @since 4.3
 */
public class EmbeddedDataSource extends BaseDataSource {
	
    public static final String USE_LATEST_VDB_VERSION = "useLatestVDBVersion";  //$NON-NLS-1$

    //*************************** EmbeddedDataSource Specific Properties
    /**
     * configFile - 
     * The path and file name to which embedded DQP configuration info will be read. This property is <i>optional</i>; if none is
     * specified, then embedded DQP access cannot be used.
     */
    private String bootstrapFile;
    
    //*************************** ConnectionPoolDataSource Specific Properties
    /**
     * maxstatements -
     * The total number of statements that the pool should keep open. 
     * 0 (zero) indicates that caching of statements is disabled.
     */
    private int maxStatements;
    /**
     * initialPoolSize - 
     * The number of physical connections the pool should contain when it is created
     */
    private int initialPoolSize;
    /**
     * minPoolSize -
     * The number of physical connections the pool should keep available at all times. 
     * 0 (zero) indicates that connections should be created as needed.
     */
    private int minPoolSize;
    /**
     * maxPoolSize -
     * The maximum number of physical connections that the pool should contain. 
     * 0 (zero) indicates no maximum size.
     */
    private int maxPoolSize;
    /**
     * maxIdleTime -
     * The number of seconds that a physical connection should remain unused in the pool 
     * before the connection is closed. 0 (zero) indicates no limit.
     */
    private int maxIdleTime;
    /**
     * propertyCycle -
     * The interval, in seconds, that the pool should wait before enforcing the current 
     * policy defined by the values of the above connection pool properties
     */
    private int propertyCycle;
    
    
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

        if (this.getBootstrapFile() != null && this.getBootstrapFile().trim().length() != 0) {
            try {
            	if (this.getBootstrapFile().equals(EmbeddedDriver.getDefaultConnectionURL())) {
            		props.put("vdb.definition", getDatabaseName() +".vdb"); //$NON-NLS-1$ //$NON-NLS-2$
            	}
                props.put(EmbeddedDataSource.DQP_BOOTSTRAP_FILE, URLHelper.buildURL(this.getBootstrapFile().trim()));
            } catch (MalformedURLException e) {
                // we can safely ignore as this will would have caught in validate..
            }
        }
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
            
    /** 
     * @return Returns the initialPoolSize.
     * @since 4.3
     */
    public int getInitialPoolSize() {
        return this.initialPoolSize;
    }

    
    /** 
     * @param initialPoolSize The initialPoolSize to set.
     * @since 4.3
     */
    public void setInitialPoolSize(int initialPoolSize) {
        this.initialPoolSize = initialPoolSize;
    }

    
    /** 
     * @return Returns the maxIdleTime.
     * @since 4.3
     */
    public int getMaxIdleTime() {
        return this.maxIdleTime;
    }

    
    /** 
     * @param maxIdleTime The maxIdleTime to set.
     * @since 4.3
     */
    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    
    /** 
     * @return Returns the maxPoolSize.
     * @since 4.3
     */
    public int getMaxPoolSize() {
        return this.maxPoolSize;
    }

    
    /** 
     * @param maxPoolSize The maxPoolSize to set.
     * @since 4.3
     */
    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    
    /** 
     * @return Returns the maxStatements.
     * @since 4.3
     */
    public int getMaxStatements() {
        return this.maxStatements;
    }

    
    /** 
     * @param maxStatements The maxStatements to set.
     * @since 4.3
     */
    public void setMaxStatements(int maxStatements) {
        this.maxStatements = maxStatements;
    }

    
    /** 
     * @return Returns the minPoolSize.
     * @since 4.3
     */
    public int getMinPoolSize() {
        return this.minPoolSize;
    }

    
    /** 
     * @param minPoolSize The minPoolSize to set.
     * @since 4.3
     */
    public void setMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
    }

    
    /** 
     * @return Returns the propertyCycle.
     * @since 4.3
     */
    public int getPropertyCycle() {
        return this.propertyCycle;
    }

    
    /** 
     * @param propertyCycle The propertyCycle to set.
     * @since 4.3
     */
    public void setPropertyCycle(int propertyCycle) {
        this.propertyCycle = propertyCycle;
    }

}
