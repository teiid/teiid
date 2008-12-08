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

package com.metamatrix.jdbc;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.metamatrix.common.classloader.NonDelegatingClassLoader;
import com.metamatrix.common.protocol.MetaMatrixURLStreamHandlerFactory;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.jdbc.util.MMJDBCURL;

/**
 * <p> The java.sql.DriverManager class uses this class to connect to MetaMatrix.
 * The Driver Manager maintains a pool of MMDriver objects, which it could use
 * to connect to MetaMatrix.
 * </p>
 */

public final class EmbeddedDriver extends BaseDriver {
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
    public static final int MAJOR_VERSION = 5;
    public static final int MINOR_VERSION = 5;
    public static final String DRIVER_NAME = "MetaMatrix Query JDBC Driver"; //$NON-NLS-1$
    
    static final String DQP_IDENTITY = "dqp.identity"; //$NON-NLS-1$
    static final String MM_IO_TMPDIR = "mm.io.tmpdir"; //$NON-NLS-1$
    
    private static Hashtable transportMap = new Hashtable();
    static Pattern urlPattern = Pattern.compile(URL_PATTERN);
    static Pattern basePattern = Pattern.compile(BASE_PATTERN);
    
    //  Static initializer
    static {   
        try {
            DriverManager.registerDriver(new EmbeddedDriver());
        } catch(SQLException e) {
            // Logging
            String logMsg = BaseDataSource.getResourceMessage("EmbeddedDriver.MMDQP_DRIVER_could_not_be_registered"); //$NON-NLS-1$
            DriverManager.println(logMsg);
        }                
    }
    
    /**
     * This method tries to make a metamatrix connection to the given URL. This class
     * will return a null if this is not the right driver to connect to the given URL.
     * @param The URL used to establish a connection.
     * @return Connection object created
     * @throws SQLException if it is unable to establish a connection to the MetaMatrix server.
     */
    public Connection connect(String url, Properties info) 
        throws SQLException {
        Connection conn = null;
        // create a properties obj if it is null
        if (info == null) {
            info = new Properties();
        }
        if (!acceptsURL(url)) {
        	return null;
        }
        // parse the URL to add it's properties to properties object
        parseURL(url, info);            
        conn = createConnection(info);

        return conn;

    }

    Connection createConnection(Properties info) throws SQLException{
        
        // first validate the properties as this may called from the EmbeddedDataSource
        // and make sure we have all the properties we need.
        validateProperties(info);
        
        // now create the connection
        URL dqpURL = (URL)info.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE);
        EmbeddedTransport transport = getDQPTransport(dqpURL, info);                        
        
        Connection conn = transport.createConnection(info);

        // only upon sucessful creation of the connection, keep the transport
        // available for future connections
        transportMap.put(dqpURL, transport);
        
        return conn;
    }
    
    /**
     * Get the DQP tranport or build the transport if one not available from the 
     * DQP URL supplied. DQP transport contains all the details about DQP.   
     * @param dqpURL - URL to the DQP.proeprties file
     * @return EmbeddedTransport
     * @throws SQLException
     * @since 4.4
     */
    private EmbeddedTransport getDQPTransport(URL dqpURL, Properties info) throws SQLException {      
        EmbeddedTransport transport = (EmbeddedTransport)transportMap.get(dqpURL);
        if (transport != null) {
            String logMsg = BaseDataSource.getResourceMessage("EmbeddedDriver.use_existing_transport"); //$NON-NLS-1$
            DriverManager.println(logMsg);    
        } 
        else {
            transport = new EmbeddedTransport(dqpURL, info, this);            
            String logMsg = BaseDataSource.getResourceMessage("EmbeddedDriver.use_new_transport"); //$NON-NLS-1$
            DriverManager.println(logMsg);
        }
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
    void parseURL(String url, Properties info) throws SQLException {
        if (url == null || url.trim().length() == 0) {
            String logMsg = BaseDataSource.getResourceMessage("EmbeddedDriver.URL_must_be_specified"); //$NON-NLS-1$
            DriverManager.println(logMsg);
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
                info.setProperty("vdb.definition", jdbcURL.getVDBName()+".vdb"); //$NON-NLS-1$ //$NON-NLS-2$
            }
            
            // Set the dqp.properties file properties
            URL dqpURL = URLHelper.buildURL(connectionURL);
                       
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
            
            // Add the DQP URL as the configuration URL
            info.put(EmbeddedDataSource.DQP_BOOTSTRAP_FILE, dqpURL);            
            
        } catch (Exception e) {
            String logMsg = e.getClass() +": "+e.getMessage(); //$NON-NLS-1$
            DriverManager.println(logMsg); 
            DriverManager.println(e.getStackTrace().toString());
            throw new SQLException(logMsg); 
        }        
    }

    /** 
     * Create the default connection URL, if one is not supplied
     * @param jdbcURL
     * @return default connection URL
     */
    String getDefaultConnectionURL() {        
        return "classpath:/mm.properties"; //$NON-NLS-1$
    }
    
    /** 
     * validate some required properties 
     * @param info the connection properties to be validated
     * @throws SQLException
     * @since 4.3
     */
    void validateProperties(Properties info) throws SQLException {

        // VDB Name has to be there
        String value = null;
        value = info.getProperty(BaseDataSource.VDB_NAME);
        if (value == null || value.trim().length() == 0) {
            String logMsg = BaseDataSource.getResourceMessage("MMDataSource.Virtual_database_name_must_be_specified"); //$NON-NLS-1$
            DriverManager.println(logMsg);
            throw new SQLException(logMsg);
        }

        // DQP config file must be supplied
        URL url = (URL)info.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE);
        if (url == null) {
            String logMsg = BaseDataSource.getResourceMessage("EmbeddedDataSource.Configuration_file_must_be_specified"); //$NON-NLS-1$
            DriverManager.println(logMsg);
            throw new SQLException(logMsg);
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

	@Override
	List<DriverPropertyInfo> getAdditionalPropertyInfo(String url,
			Properties info) {
        DriverPropertyInfo dpi = new DriverPropertyInfo(EmbeddedDataSource.DQP_BOOTSTRAP_FILE, info.getProperty(EmbeddedDataSource.DQP_BOOTSTRAP_FILE));
        dpi.required = true;
        return Arrays.asList(dpi);
	}    
    
    /**
     * Get's the driver's major version number. Initially this should be 1.
     * @return major version number of the driver.
     */
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    /**
     * Get's the driver's minor version number. Initially this should be 0.
     * @return major version number of the driver.
     */
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    /**
     * Get's the name of the driver.
     * @return name of the driver
     */
    public String getDriverName() {
        return DRIVER_NAME;
    }
     
    /**
     * Shutdown the DQP instance which has been started using the given URL 
     * @param dqpURL
     */
    public synchronized void shutdown(URL dqpURL) {
        EmbeddedTransport transport = (EmbeddedTransport) transportMap.get(dqpURL);
        if (transport != null) {
            try {
                transport.shutdown();
            } catch (SQLException e) {
                DriverManager.println(e.getMessage());
            }
            transportMap.remove(dqpURL);
        }
    }
    
    /** 
     * inner class to hold DQP tansportMap object
     * @since 4.3
     */
    static class EmbeddedTransport {
        private EmbeddedConnectionFactory connectionFactory;
        private EmbeddedConnectionTracker connectionTracker;
        private ClassLoader classLoader; 
        private String workspaceDirectory;

        public EmbeddedTransport(URL dqpURL, Properties info, EmbeddedDriver driver) throws SQLException {

            // Create a temporary workspace directory
            this.workspaceDirectory = createWorkspace(getDQPIdentity());
            
            // create a connection tracker to eep track of number of connections for this
            // dqp instance.
            this.connectionTracker = new EmbeddedConnectionTracker(dqpURL, driver);

            //Load the properties from dqp.properties file
            Properties props = loadDQPProperties(dqpURL);
            props.putAll(info);
            
            this.classLoader = this.getClass().getClassLoader();
            
            // If the dqp.classpath property exists, a non-delagating classloader will be created
            // for the DQP,otherwise the DQP classes will be loaded from the existing classloader 
            // (unifiedclassloader scenario)
            String classPath = props.getProperty("dqp.classpath"); //$NON-NLS-1$
            if (classPath != null && classPath.length() > 0) {      
                // fully qualify the class path into urls.
                URL[] dqpClassPath = resolveClassPath(classPath, dqpURL);    
                this.classLoader = new NonDelegatingClassLoader(dqpClassPath, Thread.currentThread().getContextClassLoader(), new MetaMatrixURLStreamHandlerFactory());
                String logMsg = BaseDataSource.getResourceMessage("EmbeddedDriver.use_classpath"); //$NON-NLS-1$
                DriverManager.println(logMsg);
                for (int i = 0; i < dqpClassPath.length; i++) {
                    DriverManager.println(dqpClassPath[i].toString());
                }
            }

            // Now using this classloader create the connection factory to the dqp.            
            ClassLoader current = null;            
            try {
                // this is turn off shutdown thread hook for logging
                System.setProperty("shutdownHookInstalled", String.valueOf(Boolean.TRUE)); //$NON-NLS-1$ 
                
                current = Thread.currentThread().getContextClassLoader();             
                Thread.currentThread().setContextClassLoader(this.classLoader);            
                String className = "com.metamatrix.jdbc.EmbeddedConnectionFactoryImpl"; //$NON-NLS-1$
                Class clazz = this.classLoader.loadClass(className);            
                this.connectionFactory = (EmbeddedConnectionFactory)clazz.newInstance();
                this.connectionFactory.registerConnectionListener(this.connectionTracker);
            } catch (Exception e) {
                DriverManager.println(e.getClass() +": "+e.getMessage()); //$NON-NLS-1$
                DriverManager.println(e.getStackTrace().toString());                
                throw new EmbeddedSQLException(e);                
            } finally {
                Thread.currentThread().setContextClassLoader(current);
            }                        
        }
                
        /**
         * Given dqp.classpath string with relative, absolute paths convert them to
         * URLs to be used by the non delagating classloader.  
         * @param classPath - class path given in the properties file
         * @param dqpContext - context URL of the dqp.properties file
         * @return resolved fully qualified url list of class path.
         */
        URL[] resolveClassPath(String classPath, URL dqpContext) throws SQLException {            
            
            //Load and check to make sure that a classpath exists in the properties            
            if(classPath == null || classPath.trim().length() == 0) {
                String logMsg = BaseDataSource.getResourceMessage("EmbeddedTransport.no_classpath"); //$NON-NLS-1$
                DriverManager.println(logMsg);
                throw new EmbeddedSQLException(logMsg);
            }
            
            try {
                // Create URLs out of the classpath string
                List urls = new ArrayList();            
                StringTokenizer st = new StringTokenizer(classPath,";"); //$NON-NLS-1$
                while(st.hasMoreTokens()) {
                    String path = st.nextToken();
                    path = path.trim();
                    if (path.length() > 0) {
                        // URLHelper is our special URL constructor based on the MM specific
                        // protocols like "classpath:" and "mmfile:". 
                        urls.add(URLHelper.buildURL(dqpContext, path));
                    }
                }
                return (URL[])urls.toArray(new URL[urls.size()]);
            } catch (MalformedURLException e) {
                DriverManager.println(e.getClass() +": "+e.getMessage()); //$NON-NLS-1$
                DriverManager.println(e.getStackTrace().toString());                
                throw new EmbeddedSQLException(e);
            }
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
                Properties props = new Properties();
                props.load(in);
                
                String logMsg = BaseDataSource.getResourceMessage("EmbeddedDriver.use_properties"); //$NON-NLS-1$
                DriverManager.println(logMsg);
                for (Iterator i = props.keySet().iterator(); i.hasNext();) {
                    String key = (String)i.next();                    
                    DriverManager.println(key+"="+props.getProperty(key)); //$NON-NLS-1$
                }                
                return props;
            }catch(IOException e) {
                String logMsg = BaseDataSource.getResourceMessage("EmbeddedTransport.invalid_dqpproperties_path", new Object[] {dqpURL}); //$NON-NLS-1$
                DriverManager.println(e.getClass() +": "+e.getMessage()); //$NON-NLS-1$
                DriverManager.println(e.getStackTrace().toString());
                throw new EmbeddedSQLException(e, logMsg);
            }finally {
                if (in != null) {
                    try{in.close();}catch(IOException e) {}
                }
            }
        }
     
        /**
         * Shutdown the current transport 
         */
        void shutdown() throws SQLException{
            this.connectionFactory.shutdown();            
                        
            // remove any artifacts which are not cleaned-up
            if (this.workspaceDirectory != null) {
                File file = new File(this.workspaceDirectory);
                if (file.exists()) {
                    delete(file);
                }
            }
        }
        
        /**
         * Create a connection to the DQP defined by this transport object beased on 
         * properties supplied 
         * @param info
         * @return Connection
         */
        Connection createConnection(Properties info) throws SQLException {
            ClassLoader current = null;            
            try {
                current = Thread.currentThread().getContextClassLoader();             
                Thread.currentThread().setContextClassLoader(classLoader);            
                return connectionFactory.createConnection(info);            
            } finally {
                Thread.currentThread().setContextClassLoader(current);
            }            
        }
        
        /**
         * Define an identifier for the DQP 
         * @return a JVM level unique identifier
         */
        String getDQPIdentity() {
            String id = System.getProperty(DQP_IDENTITY, "0"); //$NON-NLS-1$
            int identity = Integer.parseInt(id)+1;    
            id = String.valueOf(identity);
            System.setProperty(DQP_IDENTITY, id); 
            
            return id;
        }        
        
        /**
         * Create the temporary workspace directory for the dqp  
         * @param identity - idenity of the dqp
         */
        String createWorkspace(String identity) {
            String dir = System.getProperty("java.io.tmpdir")+"/metamatrix/"+identity; //$NON-NLS-1$ //$NON-NLS-2$
            System.setProperty(MM_IO_TMPDIR, dir); 

            File f = new File(dir);

            // If directory already exists then try to delete it; because we may have
            // failed to delete at end of last run (JVM holds lock on jar files)
            if (f.exists()) {
                delete(f);
            }
            
            // since we may have cleaned it up now , create the directory again
            if (!f.exists()) {
                f.mkdirs();
            }  
            return dir;
        }
        
        /**
         * delete the any directory including sub-trees 
         * @param file
         */
        private void delete(File file) {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                for (int i = 0; i < files.length; i++) {
                    delete(files[i]);                    
                } // for
            }

            // for saftey purpose only delete the jar files
            if (file.getName().endsWith(".jar")) { //$NON-NLS-1$
                file.delete();
            }
        }
    }

}
