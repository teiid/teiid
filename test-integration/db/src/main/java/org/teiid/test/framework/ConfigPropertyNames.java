package org.teiid.test.framework;

/**
 * The following properties can be set in 2 ways:
 * <li>set as a System property(..)</li>
 * <li>specify it in the config properties file</li>
 * 
 * @author vanhalbert
 *
 */
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
	 * Transaction Type indicates the type of transaction container to use
	 * @see TransactionFactory
	 */
    public static final String TRANSACTION_TYPE = "transaction-type"; //$NON-NLS-1$

    public interface TRANSACTION_TYPES {
		public static final String LOCAL_TRANSACTION = "local";     //$NON-NLS-1$
		public static final String XATRANSACTION = "xa"; //$NON-NLS-1$
		public static final String JNDI_TRANSACTION = "jndi"; //$NON-NLS-1$
    }
	
	
	
	/**
	 * The USE_DATASOURCES_PROP is a comma delimited system property that can be used to limit the
	 * datasources that are in use for the tests.   Use the directory name defined in the ddl directory. 
	 * This enables a developers to test a certain datasource without having to remove 
	 * connection.properties files.
	 */
	public static final String USE_DATASOURCES_PROP = "usedatasources";
		
	
	/**
	 * The EXCLUDE_DATASOURCES_PROP is a comma delimited system property that can be used to exclude
	 * certain database types.    
	 * This is done so that whole sets of tests can be excluded when a datasource  has been defined
	 * for a specific database type.
	 * Example of this is the XATransactions currently doesn't support using sqlserver (@see TEIID-559)  
	 */
	
	public static final String EXCLUDE_DATASBASE_TYPES_PROP = "excludedatasources";

	
	
	/**
	 * Connection Type indicates the type of connection (strategy) to use
	 * Options are {@link CONNECTION_TYPES}
	 */
    public static final String CONNECTION_TYPE = "connection-type"; //$NON-NLS-1$
    
    
    public interface CONNECTION_TYPES {
    
	     // used to create the jdb driver
	    public static final String DRIVER_CONNECTION = "driver"; //$NON-NLS-1$
	    // used to create a datasource 
	    public static final String DATASOURCE_CONNECTION = "datasource"; //$NON-NLS-1$
	    // used for when embedded is running in an appserver
	    public static final String JNDI_CONNECTION = "jndi"; //$NON-NLS-1$


    }

}
