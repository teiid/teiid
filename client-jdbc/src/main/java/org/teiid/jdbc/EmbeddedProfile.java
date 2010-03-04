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

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.transport.LocalServerConnection;

import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.jdbc.BaseDataSource;
import com.metamatrix.jdbc.JDBCPlugin;
import com.metamatrix.jdbc.MMConnection;
import com.metamatrix.jdbc.util.MMJDBCURL;


final class EmbeddedProfile {
    
    private static final String BUNDLE_NAME = "com.metamatrix.jdbc.basic_i18n"; //$NON-NLS-1$
    
	/** 
     * Match URL like
     * - jdbc:teiid:BQT
     * - jdbc:teiid:BQT;verson=1  
     */
    static final String BASE_PATTERN = "jdbc:teiid:((\\w+)[;]?)(;([^@])+)*"; //$NON-NLS-1$

    private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
    
    static Pattern basePattern = Pattern.compile(BASE_PATTERN);
    
    /**
     * This method tries to make a connection to the given URL. This class
     * will return a null if this is not the right driver to connect to the given URL.
     * @param The URL used to establish a connection.
     * @return Connection object created
     * @throws SQLException if it is unable to establish a connection to the MetaMatrix server.
     */
    public static Connection connect(String url, Properties info) 
        throws SQLException {
        // create a properties obj if it is null
        if (info == null) {
            info = new Properties();
        } else {
        	info = PropertiesUtils.clone(info);
        }

        // parse the URL to add it's properties to properties object
        parseURL(url, info);            
        MMConnection conn = createConnection(url, info);
        logger.fine(JDBCPlugin.Util.getString("JDBCDriver.Connection_sucess")); //$NON-NLS-1$ 
        return conn;
    }
    
    static MMConnection createConnection(String url, Properties info) throws SQLException{
        
        // first validate the properties as this may called from the EmbeddedDataSource
        // and make sure we have all the properties we need.
        validateProperties(info);
        try {
			return new MMConnection(new LocalServerConnection(info), info, url);
		} catch (MetaMatrixRuntimeException e) {
			throw new SQLException(e);
		} catch (ConnectionException e) {
			throw new SQLException(e);
		} catch (CommunicationException e) {
			throw new SQLException(e);
		}
    }
    
    /**
     * This method parses the URL and adds properties to the the properties object. These include required and any optional
     * properties specified in the URL. 
     * Expected URL format -- 
     * jdbc:teiid:VDB;[name=value]*;
     * 
     * @param The URL needed to be parsed.
     * @param The properties object which is to be updated with properties in the URL.
     * @throws SQLException if the URL is not in the expected format.
     */
     static void parseURL(String url, Properties info) throws SQLException {
        if (url == null || url.trim().length() == 0) {
            String logMsg = getResourceMessage("EmbeddedDriver.URL_must_be_specified"); //$NON-NLS-1$
            throw new SQLException(logMsg);
        }
                
        try {
            MMJDBCURL jdbcURL = new MMJDBCURL(url);

            // Set the VDB Name
            info.setProperty(BaseDataSource.VDB_NAME, jdbcURL.getVDBName());
                       
            Properties optionalParams = jdbcURL.getProperties();
            MMJDBCURL.normalizeProperties(info);
            
            Enumeration keys = optionalParams.keys();
            while (keys.hasMoreElements()) {
                String propName = (String)keys.nextElement();
                // Don't let the URL properties override the passed-in Properties object.
                if (!info.containsKey(propName)) {
                    info.setProperty(propName, optionalParams.getProperty(propName));
                }
            }
            // add the property only if it is new because they could have
            // already been specified either through url or otherwise.
            if(! info.containsKey(BaseDataSource.VDB_VERSION) && jdbcURL.getVDBVersion() != null) {
                info.setProperty(BaseDataSource.VDB_VERSION, jdbcURL.getVDBVersion());
            }
            if(!info.containsKey(BaseDataSource.APP_NAME)) {
                info.setProperty(BaseDataSource.APP_NAME, BaseDataSource.DEFAULT_APP_NAME);
            }
        } catch (Exception e) {
            throw new SQLException(e); 
        }        
    }

    /** 
     * validate some required properties 
     * @param info the connection properties to be validated
     * @throws SQLException
     * @since 4.3
     */
    static void validateProperties(Properties info) throws SQLException {
        // VDB Name has to be there
        String value = null;
        value = info.getProperty(BaseDataSource.VDB_NAME);
        if (value == null || value.trim().length() == 0) {
            String logMsg = getResourceMessage("MMDataSource.Virtual_database_name_must_be_specified"); //$NON-NLS-1$
            throw new SQLException(logMsg);
        }

    }
    
    public static boolean acceptsURL(String url) {
        // Check if this can match our default one, then allow it.
        Matcher m = basePattern.matcher(url);
        boolean matched = m.matches();
        return matched;
    }    
    
    static String getResourceMessage(String key) {
        ResourceBundle messages = ResourceBundle.getBundle(BUNDLE_NAME);          
        String messageTemplate = messages.getString(key);
        return MessageFormat.format(messageTemplate, (Object[])null);
    }    

}
