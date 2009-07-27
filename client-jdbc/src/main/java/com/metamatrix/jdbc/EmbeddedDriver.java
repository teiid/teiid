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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.jdbc.TeiidDriver;

/**
 * <p> The java.sql.DriverManager class uses this class to connect to MetaMatrix.
 * The Driver Manager maintains a pool of MMDriver objects, which it could use
 * to connect to MetaMatrix.
 * </p>
 */

public final class EmbeddedDriver extends TeiidDriver {
    /** 
     * Match URL like
     * - jdbc:metamatrix:BQT@c:/foo.properties;version=1..
     * - jdbc:metamatrix:BQT@c:\\foo.properties;version=1..
     * - jdbc:metamatrix:BQT@\\foo.properties;version=1..
     * - jdbc:metamatrix:BQT@/foo.properties;version=1..
     * - jdbc:metamatrix:BQT@../foo.properties;version=1..
     * - jdbc:metamatrix:BQT@./foo.properties;version=1..
     * - jdbc:metamatrix:BQT@file:///c:/foo.properties;version=1..
     * - jdbc:metamatrix:BQT
     * - jdbc:metamatrix:BQT;verson=1  
     */
    static final String URL_PATTERN = "jdbc:metamatrix:(\\w+)@(([^;]*)[;]?)((.*)*)"; //$NON-NLS-1$
    static final String BASE_PATTERN = "jdbc:metamatrix:((\\w+)[;]?)(;([^@])+)*"; //$NON-NLS-1$
    public static final String DRIVER_NAME = "Teiid Embedded JDBC Driver"; //$NON-NLS-1$

    private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
    
    static Pattern urlPattern = Pattern.compile(URL_PATTERN);
    static Pattern basePattern = Pattern.compile(BASE_PATTERN);
    
    //  Static initializer
    static {   
        try {
            DriverManager.registerDriver(new EmbeddedDriver());
        } catch(SQLException e) {
            // Logging
            String logMsg = BaseDataSource.getResourceMessage("EmbeddedDriver.MMDQP_DRIVER_could_not_be_registered"); //$NON-NLS-1$
            logger.log(Level.SEVERE, logMsg);
        }                
    }

    /**
     * Returns true if the driver thinks that it can open a connection to the given URL. Typically drivers will return true if
     * they understand the subprotocol specified in the URL and false if they don't. Expected URL format is
     * jdbc:metamatrix:VDB@pathToPropertyFile;version=1;logFile=<logFile.log>;logLevel=<logLevel>;txnAutoWrap=<?>
     * 
     * @param The URL used to establish a connection.
     * @return A boolean value indicating whether the driver understands the subprotocol.
     * @throws SQLException, should never occur
     */
    public boolean acceptsURL(String url) throws SQLException {
        Matcher m = urlPattern.matcher(url);
        boolean matched = m.matches();
        if (matched) {
            // make sure the group (2) which is the name of the file 
            // does not start with mm:// or mms://
            String name = m.group(2).toLowerCase();
            return (!name.startsWith("mm://") && !name.startsWith("mms://")); //$NON-NLS-1$ //$NON-NLS-2$
        }

        // Check if this can match our default one, then allow it.
        m = basePattern.matcher(url);
        matched = m.matches();
        return matched;
    }

}
