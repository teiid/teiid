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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;

import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.classloader.PostDelegatingClassLoader;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.protocol.MMURLConnection;
import com.metamatrix.common.protocol.MetaMatrixURLStreamHandlerFactory;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.jdbc.BaseDataSource;
import com.metamatrix.jdbc.JDBCPlugin;
import com.metamatrix.jdbc.MMConnection;
import com.metamatrix.jdbc.MMSQLException;
import com.metamatrix.jdbc.api.SQLStates;
import com.metamatrix.jdbc.util.MMJDBCURL;


final class EmbeddedProfile {
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
    static final String URL_PATTERN = "jdbc:(teiid|metamatrix):(\\w+)@(([^;]*)[;]?)((.*)*)"; //$NON-NLS-1$
    static final String BASE_PATTERN = "jdbc:(teiid|metamatrix):((\\w+)[;]?)(;([^@])+)*"; //$NON-NLS-1$

    private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
    
    private static EmbeddedTransport currentTransport = null;
    static Pattern urlPattern = Pattern.compile(URL_PATTERN);
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
        MMConnection conn = createConnection(info);
        boolean shutdown = Boolean.parseBoolean(info.getProperty(MMURL.CONNECTION.SHUTDOWN, "false")); //$NON-NLS-1$
        if (shutdown) {
        	Admin admin = conn.getAdminAPI();
        	try {
        		// this will make sure the user has permissions to do the shutdown.
				admin.shutdown(0); 
				shutdown();
				throw new MMSQLException(getResourceMessage("EmbeddedDriver.shutdown_sucessful"), SQLStates.SUCESS); //$NON-NLS-1$
			} catch (AdminException e) {
				conn.close();
				throw new MMSQLException(e, getResourceMessage("EmbeddedDriver.shutdown_failure"), SQLStates.DEFAULT); //$NON-NLS-1$
			}
        }
        
        // logging
        String logMsg = JDBCPlugin.Util.getString("JDBCDriver.Connection_sucess"); //$NON-NLS-1$
        logger.fine(logMsg);
        
        return conn;

    }
    
    static MMConnection createConnection(Properties info) throws SQLException{
        
        // first validate the properties as this may called from the EmbeddedDataSource
        // and make sure we have all the properties we need.
        validateProperties(info);
        
        URL dqpURL;
		try {
			dqpURL = URLHelper.buildURL(info.getProperty(DQPEmbeddedProperties.DQP_BOOTSTRAP_FILE));
		} catch (MalformedURLException e) {
			throw MMSQLException.create(e);
		}
        
        // now create the connection
        EmbeddedTransport transport = getDQPTransport(dqpURL, info);                        
        
        MMConnection conn = transport.createConnection(dqpURL, info);
        
        return conn;
    }
    
    /**
     * Get the DQP transport or build the transport if one not available from the 
     * DQP URL supplied. DQP transport contains all the details about DQP.   
     * @param dqpURL - URL to the DQP.properties file
     * @return EmbeddedTransport
     * @throws SQLException
     * @since 4.4
     */
    private synchronized static EmbeddedTransport getDQPTransport(URL dqpURL, Properties info) throws SQLException {      
        EmbeddedTransport transport = currentTransport;
        if (transport == null || !currentTransport.getURL().equals(dqpURL)) {
        	// shutdown any previous instance; we do encourage single instance in a given VM
       		shutdown();
       		try {
       			transport = new EmbeddedTransport(dqpURL, info);
       		} catch (SQLException e) {
                logger.log(Level.SEVERE, "Could not start the embedded engine", e); //$NON-NLS-1$
       			throw e;
       		}
        }
        currentTransport = transport;
        return transport;
    }

    /**
     * This method parses the URL and adds properties to the the properties object. These include required and any optional
     * properties specified in the URL. 
     * Expected URL format -- 
     * jdbc:metamatrix:local:VDB@<pathToConfigFile>logFile=<logFile.log>; logLevel=<logLevel>;txnAutoWrap=<?>;credentials=mycredentials;
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

            // Need to resolve the URL fully, if we are using the default URL like
            // jdbc:metamatrix:<vdbName>.., where as this fully qualifies to
            // jdbc:metamatrix:<vdbName>@classpath:<vdbName>/mm.properties;...
            String connectionURL = jdbcURL.getConnectionURL();
            if (connectionURL == null) {
                connectionURL = getDefaultConnectionURL();
                info.setProperty(DQPEmbeddedProperties.VDB_DEFINITION, jdbcURL.getVDBName()+".vdb"); //$NON-NLS-1$
            }
            info.setProperty(DQPEmbeddedProperties.DQP_BOOTSTRAP_FILE, connectionURL);
                       
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
     * Create the default connection URL, if one is not supplied
     * @param jdbcURL
     * @return default connection URL
     */
    static String getDefaultConnectionURL() {        
        return "classpath:/deploy.properties"; //$NON-NLS-1$
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
        Matcher m = urlPattern.matcher(url);
        boolean matched = m.matches();
        if (matched) {
            // make sure the group (2) which is the name of the file 
            // does not start with mm:// or mms://
            String name = m.group(3).toLowerCase();
            return (!name.startsWith("mm://") && !name.startsWith("mms://") && (name.endsWith(".properties")||name.endsWith(".properties;"))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        }

        // Check if this can match our default one, then allow it.
        m = basePattern.matcher(url);
        matched = m.matches();
        return matched;
    }    

     
    /**
     * Shutdown the DQP instance which has been started using the given URL 
     * @param dqpURL
     */
    public static synchronized void shutdown() {
        if (currentTransport != null) {
        	currentTransport.shutdown();
        	currentTransport = null;
        }
    }
    
    /** 
     * inner class to hold DQP tansportMap object
     * @since 4.3
     */
    static class EmbeddedTransport {
		private ServerConnectionFactory connectionFactory;
        private ClassLoader classLoader; 
        private URL url;
        Properties props;

        public EmbeddedTransport(URL dqpURL, Properties info) throws SQLException {

        	this.url = dqpURL;
        	
            //Load the properties from dqp.properties file
            this.props = loadDQPProperties(dqpURL);
            this.props.putAll(info);
            
            this.props = PropertiesUtils.resolveNestedProperties(this.props);
                        
            // a non-delegating class loader will be created from where all third party dependent jars can be loaded
            ArrayList<URL> runtimeClasspathList = new ArrayList<URL>();
            String libLocation = this.props.getProperty(DQPEmbeddedProperties.DQP_LIBDIR, "./lib/"); //$NON-NLS-1$
            if (!libLocation.endsWith("/")) { //$NON-NLS-1$
            	libLocation = libLocation + "/"; //$NON-NLS-1$
            }

            // find jars in the "lib" directory; patches is reverse alpaha and not case sensitive so small letters then capitals
            if (!EmbeddedProfile.getDefaultConnectionURL().equals(dqpURL.toString())) {
	            runtimeClasspathList.addAll(libClassPath(dqpURL, libLocation+"patches/", MMURLConnection.REVERSEALPHA)); //$NON-NLS-1$
	            runtimeClasspathList.addAll(libClassPath(dqpURL, libLocation, MMURLConnection.DATE));
            
	            try {
		            String configLocation = this.props.getProperty(DQPEmbeddedProperties.DQP_DEPLOYDIR, "./deploy/"); //$NON-NLS-1$ 
		            if (!configLocation.endsWith("/")) { //$NON-NLS-1$
		            	configLocation = configLocation + "/"; //$NON-NLS-1$
		            }
		            runtimeClasspathList.add(URLHelper.buildURL(dqpURL, configLocation));
	            } catch(IOException e) {
	            	// ignore..
	            }            
            }
                        
            URL[] dqpClassPath = runtimeClasspathList.toArray(new URL[runtimeClasspathList.size()]);
            this.classLoader = new PostDelegatingClassLoader(dqpClassPath, this.getClass().getClassLoader(), new MetaMatrixURLStreamHandlerFactory());
            
            String logMsg = getResourceMessage("EmbeddedDriver.use_classpath"); //$NON-NLS-1$
            logger.log(Level.FINER, logMsg + " " + Arrays.toString(dqpClassPath)); //$NON-NLS-1$

            // Now using this class loader create the connection factory to the dqp.            
            ClassLoader current = Thread.currentThread().getContextClassLoader();            
            try {
                Thread.currentThread().setContextClassLoader(this.classLoader);            
                String className = "com.metamatrix.jdbc.EmbeddedConnectionFactoryImpl"; //$NON-NLS-1$
                Class<?> clazz = this.classLoader.loadClass(className);            
                this.connectionFactory = (ServerConnectionFactory)clazz.newInstance();                
            } catch (Exception e) {
            	throw MMSQLException.create(e, "Could not load the embedded server, please ensure that your classpath is set correctly."); //$NON-NLS-1$                
            } finally {
                Thread.currentThread().setContextClassLoader(current);
            }                        
        }
                
        URL getURL() {
        	return this.url;
        }

        /**
         * Note that this only works when embedded loaded with "mmfile" protocol in the URL.
         * @param dqpURL
         * @return
         */
        private List<URL> libClassPath (URL dqpURL, String directory, String sortStyle) {
            ObjectInputStream in =  null;
            ArrayList<URL> urlList = new ArrayList<URL>();
            try {
            	urlList.add(URLHelper.buildURL(dqpURL, directory));
                dqpURL = URLHelper.buildURL(dqpURL, directory+"?action=list&filter=.jar&sort="+sortStyle); //$NON-NLS-1$       
                in = new ObjectInputStream(dqpURL.openStream());
                String[] urls = (String[])in.readObject();
                for (int i = 0; i < urls.length; i++) {
                    urlList.add(URLHelper.buildURL(urls[i]));
                }             
            } catch(IOException e) {
            	//ignore, treat as if lib does not exist
            }  catch(ClassNotFoundException e) {
            	//ignore, treat as if lib does not exist            	
            } finally {
                if (in != null) {
                    try{in.close();}catch(IOException e) {}
                }
            }        
            return urlList;
        }        
        
        /**
         * Load DQP Properties from the URL supplied. 
         * @param dqpURL - URL to the "dqp.properties" object
         * @return Properties loaded
         * @throws SQLException
         */
        Properties loadDQPProperties(URL dqpURL) throws SQLException {
            InputStream in = null;
            try{
                in = dqpURL.openStream();
                Properties props = new Properties(System.getProperties());
                props.load(in);
                
                String logMsg = getResourceMessage("EmbeddedDriver.use_properties"); //$NON-NLS-1$
                logger.log(Level.FINER, logMsg + props);
                return props;
            }catch(IOException e) {
                String logMsg = getResourceMessage("EmbeddedTransport.invalid_dqpproperties_path", new Object[] {dqpURL}); //$NON-NLS-1$
                throw MMSQLException.create(e, logMsg);
            }finally {
                if (in != null) {
                    try{in.close();}catch(IOException e) {}
                }
            }
        }
     
        /**
         * Shutdown the current transport 
         */
        void shutdown() {
            this.connectionFactory.shutdown(false);                                    
        }
        
        /**
         * Create a connection to the DQP defined by this transport object based on 
         * properties supplied 
         * @param info
         * @return Connection
         */
        MMConnection createConnection(URL url, Properties info) throws SQLException {
            ClassLoader current = null;            
            try {
                current = Thread.currentThread().getContextClassLoader();             
                Thread.currentThread().setContextClassLoader(classLoader);       
                try {
                	info.setProperty(DQPEmbeddedProperties.BOOTURL, url.toExternalForm());
                	info.setProperty(DQPEmbeddedProperties.TEIID_HOME, getHomeDirectory(url));
					ServerConnection conn = connectionFactory.createConnection(info);
					return new MMConnection(conn, info, url.toExternalForm());
				} catch (CommunicationException e) {
					throw MMSQLException.create(e);
				} catch (ConnectionException e) {
					throw MMSQLException.create(e);
				}
            } finally {
                Thread.currentThread().setContextClassLoader(current);
            }            
        }
        
        String getHomeDirectory(URL url) throws SQLException {
        	try {
        		// check the system wide
        		String teiidHome = System.getProperty(DQPEmbeddedProperties.TEIID_HOME);

        		// then check the deploy.properties
        		if (teiidHome == null) {
        			teiidHome = this.props.getProperty(DQPEmbeddedProperties.TEIID_HOME);
        		}
        		
        		if (teiidHome == null) {
    	        	if (EmbeddedProfile.getDefaultConnectionURL().equals(url.toString())) {
    	        		teiidHome = System.getProperty("user.dir")+"/teiid"; //$NON-NLS-1$ //$NON-NLS-2$
    	        	}
    	        	else {
    	        		URL installDirectory = URLHelper.buildURL(url, "."); //$NON-NLS-1$
    	        		teiidHome = installDirectory.getPath();
    	        	}
        		}
        		File f = new File(teiidHome); 
        		return f.getCanonicalPath();
        	} catch(IOException e) {
        		throw MMSQLException.create(e);
        	}
        }
        
    }
    
    private static final String BUNDLE_NAME = "com.metamatrix.jdbc.basic_i18n"; //$NON-NLS-1$
    

    static String getResourceMessage(String key, Object[] args) {
        ResourceBundle messages = ResourceBundle.getBundle(BUNDLE_NAME);          
        String messageTemplate = messages.getString(key);
        return MessageFormat.format(messageTemplate, args);
    }
    
   
    static String getResourceMessage(String key) {
        ResourceBundle messages = ResourceBundle.getBundle(BUNDLE_NAME);          
        String messageTemplate = messages.getString(key);
        return MessageFormat.format(messageTemplate, (Object[])null);
    }    

}
