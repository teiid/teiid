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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.metamatrix.common.classloader.PostDelegatingClassLoader;
import com.metamatrix.common.protocol.MMURLConnection;
import com.metamatrix.common.protocol.MetaMatrixURLStreamHandlerFactory;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.util.ApplicationInfo;
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
    public static final String DRIVER_NAME = "Teiid Embedded JDBC Driver"; //$NON-NLS-1$
    public static final String POST_DELEGATION_LIBRARIES = "PostDelegationLibraries"; //$NON-NLS-1$

    static final String DQP_IDENTITY = "dqp.identity"; //$NON-NLS-1$
    static final String MM_IO_TMPDIR = "mm.io.tmpdir"; //$NON-NLS-1$
    private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
    
    private static EmbeddedTransport currentTransport = null;
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

        // logging
        String logMsg = JDBCPlugin.Util.getString("JDBCDriver.Connection_sucess"); //$NON-NLS-1$
        logger.fine(logMsg);
        
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

        // only upon successful creation of the connection, keep the transport
        // available for future connections
        currentTransport = transport;
        
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
    private static EmbeddedTransport getDQPTransport(URL dqpURL, Properties info) throws SQLException {      
        EmbeddedTransport transport = currentTransport;
        if (transport != null && currentTransport.getURL().equals(dqpURL)) {
            String logMsg = BaseDataSource.getResourceMessage("EmbeddedDriver.use_existing_transport"); //$NON-NLS-1$
            DriverManager.println(logMsg);    
        } 
        else {
        	// shutdown any previous instance; we do encourage single instance in a given VM
       		shutdown();
            transport = new EmbeddedTransport(dqpURL, info);            
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
            if(!info.containsKey(BaseDataSource.APP_NAME)) {
                info.setProperty(BaseDataSource.APP_NAME, BaseDataSource.DEFAULT_APP_NAME);
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
    static String getDefaultConnectionURL() {        
        return "classpath:/deploy.properties"; //$NON-NLS-1$
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
        return ApplicationInfo.getInstance().getMajorReleaseVersion();
    }

    /**
     * Get's the driver's minor version number. Initially this should be 0.
     * @return major version number of the driver.
     */
    public int getMinorVersion() {
        return ApplicationInfo.getInstance().getMinorReleaseVersion();
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
    public static synchronized void shutdown() {
        if (currentTransport != null) {
            try {
            	currentTransport.shutdown();
            	currentTransport = null;
            } catch (SQLException e) {
                DriverManager.println(e.getMessage());
            }
        }
    }
    
    /** 
     * inner class to hold DQP tansportMap object
     * @since 4.3
     */
    static class EmbeddedTransport {
		private EmbeddedConnectionFactory connectionFactory;
        private ClassLoader classLoader; 
        private String workspaceDirectory;
        private URL url;

        public EmbeddedTransport(URL dqpURL, Properties info) throws SQLException {

        	this.url = dqpURL;
        	
            // Create a temporary workspace directory
            this.workspaceDirectory = createWorkspace(getDQPIdentity());
            
            //Load the properties from dqp.properties file
            Properties props = loadDQPProperties(dqpURL);
            props.putAll(info);

            this.classLoader = this.getClass().getClassLoader();
            
            // a non-delegating class loader will be created from where all third party dependent jars can be loaded
            ArrayList<URL> runtimeClasspathList = new ArrayList<URL>();
            ArrayList<URL> postDelegationClasspathList = null;
            String libLocation = info.getProperty("dqp.lib", "./lib/"); //$NON-NLS-1$ //$NON-NLS-2$
            if (!libLocation.endsWith("/")) { //$NON-NLS-1$
            	libLocation = libLocation + "/"; //$NON-NLS-1$
            }

            // find jars in the "lib" directory; patches is reverse alpaha and not case sensitive so small letters then capitals
            if (!EmbeddedDriver.getDefaultConnectionURL().equals(dqpURL.toString())) {
	            runtimeClasspathList.addAll(libClassPath(dqpURL, libLocation+"patches/", MMURLConnection.REVERSEALPHA)); //$NON-NLS-1$
	            runtimeClasspathList.addAll(libClassPath(dqpURL, libLocation, MMURLConnection.DATE));

	            // check if a specific post delegation rules specified for loading
	            String postDelgationLibraries  = info.getProperty(POST_DELEGATION_LIBRARIES); 
	            if (postDelgationLibraries != null) {
	            	postDelegationClasspathList = resolvePath(dqpURL, libLocation, postDelgationLibraries);
	            	runtimeClasspathList.removeAll(postDelegationClasspathList);
	            }	         
            }
            
            URL[] dqpClassPath = runtimeClasspathList.toArray(new URL[runtimeClasspathList.size()]);
            this.classLoader = new URLClassLoader(dqpClassPath, Thread.currentThread().getContextClassLoader(), new MetaMatrixURLStreamHandlerFactory());
            
            if (postDelegationClasspathList != null && !postDelegationClasspathList.isEmpty()) {
            	URL[] path = postDelegationClasspathList.toArray(new URL[postDelegationClasspathList.size()]);
            	this.classLoader = new PostDelegatingClassLoader(path, this.classLoader, new MetaMatrixURLStreamHandlerFactory());
            }
            
            String logMsg = BaseDataSource.getResourceMessage("EmbeddedDriver.use_classpath"); //$NON-NLS-1$
            DriverManager.println(logMsg);
            for (int i = 0; i < dqpClassPath.length; i++) {
                DriverManager.println(dqpClassPath[i].toString());
            }
            
            // Now using this class loader create the connection factory to the dqp.            
            ClassLoader current = null;            
            try {
                current = Thread.currentThread().getContextClassLoader();             
                Thread.currentThread().setContextClassLoader(this.classLoader);            
                String className = "com.metamatrix.jdbc.EmbeddedConnectionFactoryImpl"; //$NON-NLS-1$
                Class clazz = this.classLoader.loadClass(className);            
                this.connectionFactory = (EmbeddedConnectionFactory)clazz.newInstance();
            } catch (Exception e) {
                DriverManager.println(e.getClass() +": "+e.getMessage()); //$NON-NLS-1$
                throw new EmbeddedSQLException(e);                
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
        
        private ArrayList<URL> resolvePath(URL dqpURL, String directory, String path) throws SQLException {
        	StringTokenizer st = new StringTokenizer(path, ","); //$NON-NLS-1$
        	ArrayList<URL> urlList = new ArrayList<URL>();
        	while (st.hasMoreTokens()) {
        		try {
        			urlList.add(URLHelper.buildURL(dqpURL, directory+st.nextToken()));
				} catch (MalformedURLException e) {
					throw new EmbeddedSQLException(e);
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
         * Create a connection to the DQP defined by this transport object based on 
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
         * @param identity - identity of the dqp
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
