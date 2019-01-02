package org.teiid.test.framework;

import org.teiid.jdbc.ExecutionProperties;
import org.teiid.test.client.TestClientTransaction;
import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.datasource.DataSource;
import org.teiid.test.framework.datasource.DataSourceMgr;
import org.teiid.test.framework.datasource.DataStore;

/**
 * The following properties can be set in 2 ways:
 * <li>set as a System property(..)</li>
 * <li>specify it in the config properties file</li>
 * 
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public interface ConfigPropertyNames {
	
	/**
	 * Specify this as a system property to set a specific configuration to use
	 * otherwise the {@link ConfigPropertyLoader#DEFAULT_CONFIG_FILE_NAME} will be loaded.
	 */
	public static final String CONFIG_FILE="config";

	
	/**
	 * For Driver/Datasource connection related properties, {@link ConnectionStrategy}.
	 */

	
	
	/**
	 * The USE_DATASOURCES_PROP is a comma delimited property that can be used to limit the
	 * datasources that are in use for the tests.   Use the directory name defined in the ddl directory. 
	 * This enables a developers to test a certain datasource without having to remove 
	 * connection.properties files.
	 */
	public static final String USE_DATASOURCES_PROP = "usedatasources";
	
	
	/**
	 * The USE_DATASOURCE_TYPES_PROP is a comma delimited property that can be used to limit the
	 * types of datasources to be used for the tests.   The database type {@link DataSource#DB_TYPE} corresponds to the
	 * defined types in the resources/ddl directory.   By specifying this property, the test will use on data sources
	 * of the specified types..
	 */
	public static final String USE_DATASOURCE_TYPES_PROP = "usedatasourcetypes";
	
		
	
	/**
	 * The EXCLUDE_DATASOURCES_PROP is a comma delimited property that can be used to exclude
	 * certain database types.    
	 * This is done so that whole sets of tests can be excluded when a datasource  has been defined
	 * for a specific database type.
	 */
	
	public static final String EXCLUDE_DATASBASE_TYPES_PROP = "excludedatasourcetypes";

	/**
	 * The {@link #OVERRIDE_DATASOURCES_LOC}, when specified, will override the default
	 * defined for {@link DataSourceMgr#DEFAULT_DATASOURCES_LOC};
	 * 
	 */
	public static final String OVERRIDE_DATASOURCES_LOC = "datasourceloc";
	
	/**
	 * If {@link DISABLE_DATASTORES} is specified, then the assumption is that configuration related
	 * to the datastores is being handled out side of the test framework.
	 * This include not performing the following:
	 * <li>{@link DataStore} will not called to configure the data prior to running a test</li>
	 * <li>The connector bindings will not be configured as part of the {@link ConnectionStrategy} vdb configuration.
	 * The vdb must have the binding(s) defined. </li>
	 * 
	 * The {@link TestClientTransaction} will be using this option because the data is already assumed configured in its respective database.
	 */
	public static final String DISABLE_DATASTORES = "disable_datastore";
	
	/**
	 * Connection Type indicates the type of connection (strategy) to use when 
	 * connecting to Teiid.
	 * Options are {@link CONNECTION_TYPES}
	 */
    public static final String CONNECTION_TYPE = "connection-type"; //$NON-NLS-1$
    
    
    /**
     * {@see #CONNECTION_TYPE} regarding setting the specific connection type to use
     * when connecting to Teiid
     * @author vanhalbert
     *
     */
    public interface CONNECTION_TYPES {
    
	     // used to create the jdb driver
	    public static final String DRIVER_CONNECTION = "driver"; //$NON-NLS-1$
	    // used to create a datasource 
	    public static final String DATASOURCE_CONNECTION = "datasource"; //$NON-NLS-1$
	    // used for when embedded is running in an appserver
	    public static final String JNDI_CONNECTION = "jndi"; //$NON-NLS-1$


    }
    
    /**
     * Connection Props are the {@link CONNECTION_STRATEGY} execution options 
     * @author vanhalbert
     *
     */
    public interface CONNECTION_STRATEGY_PROPS {

		public static final String TXN_AUTO_WRAP = ExecutionProperties.PROP_TXN_AUTO_WRAP;
		public static final String AUTOCOMMIT = "autocommit"; //$NON-NLS-1$
		public static final String FETCH_SIZE = ExecutionProperties.PROP_FETCH_SIZE;
		public static final String EXEC_IN_BATCH = "execute.in.batch"; //$NON-NLS-1$
		public static final String CONNECTOR_BATCH = "connector-batch"; //$NON-NLS-1$
		public static final String PROCESS_BATCH = "process-batch"; //$NON-NLS-1$
		public static final String JNDINAME_USERTXN = "usertxn-jndiname"; //$NON-NLS-1$  
    	
    }
    
    public interface TXN_AUTO_WRAP_OPTIONS {
	    public static final String AUTO_WRAP_OFF = ExecutionProperties.TXN_WRAP_OFF;  //$NON-NLS-1$	    
	    public static final String AUTO_WRAP_ON = ExecutionProperties.TXN_WRAP_ON;  //$NON-NLS-1$
	    public static final String AUTO_WRAP_AUTO = ExecutionProperties.TXN_WRAP_DETECT;  //$NON-NLS-1$
	  
    }

}
