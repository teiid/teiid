package org.teiid.test.framework;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.teiid.test.framework.datasource.DataSourceFactory;
import org.teiid.test.framework.exception.TransactionRuntimeException;
import org.teiid.test.util.PropUtils;


/**
 * The ConfigProperteryLoader will load the configuration properties to be used by a test.
 * Unless a different configuraton file is specified, subsequent loading of the configuration
 * fill will not occur.  However, <code>overrides</code> that are applied per test
 * 
 * 
 * @author vanhalbert
 *
 */

public class ConfigPropertyLoader {

	/**
	 * The default config file to use when #CONFIG_FILE system property isn't
	 * set
	 */
	public static final String DEFAULT_CONFIG_FILE_NAME = "default-config.properties";
	
	private static ConfigPropertyLoader _instance = null;
	private static String LAST_CONFIG_FILE = null;
	
	/**
	 * Contains any overrides specified for the test
	 */
	private Properties overrides = new Properties();

	/**
	 * Contains the properties loaded from the config file
	 */
	private Properties props = null;
	
	private Map<String, String>modelAssignedDatabaseType = new HashMap<String, String>(5);
	
	private DataSourceFactory dsfactory = null;

	private ConfigPropertyLoader() {
	}

	public static synchronized ConfigPropertyLoader getInstance() {
	    boolean diff = differentConfigProp();
	    

	    	if (_instance != null && !diff) {
	    	    return _instance;
	    	}
	    	if (_instance != null) {
	    	    cleanup();
	    	}
	    	
		_instance = new ConfigPropertyLoader();
		try {
			_instance.initialize();
		} catch (TransactionRuntimeException e) {
			throw e;
		}

		return _instance;
	}
	
	/**
	 * because a config file could be different for the subsequent test, check
	 * to see if the file is different.
	 * @return
	 */
	private static boolean differentConfigProp( ) {
		String filename = System.getProperty(ConfigPropertyNames.CONFIG_FILE);
		if (filename == null) {
			filename = DEFAULT_CONFIG_FILE_NAME;
		}
		
		if (LAST_CONFIG_FILE == null || ! LAST_CONFIG_FILE.equalsIgnoreCase(filename)) {
		    LAST_CONFIG_FILE = filename;
		    return true;
		}
		return false;

	}
	
	private static synchronized void cleanup() {
	    
	    _instance.modelAssignedDatabaseType.clear();
	    _instance.props.clear();

	    if (_instance.dsfactory != null) {
		_instance.dsfactory.cleanup();
	    }
	    
	    reset();
	    
	    _instance = null;
	    LAST_CONFIG_FILE=null;
	}
	
	/**
	 * Called after each test to reset any per test settings.
	 */
	public static synchronized void reset() {
	    _instance.overrides.clear();

	}

	
	private void initialize() {

	    props = PropUtils.loadProperties("/" + LAST_CONFIG_FILE, null);
	    dsfactory = new DataSourceFactory(this);
	}
	
	public DataSourceFactory getDataSourceFactory() {
	    return this.dsfactory;
	}


	public String getProperty(String key) {
	    String rtn = null;
	    rtn = overrides.getProperty(key);
	    if (rtn == null) {
		rtn = props.getProperty(key);
		
		if (rtn == null) {
		    rtn = System.getProperty(key);
		}
	    }
	    return rtn;
	}
	
	public void setProperty(String key, String value) {
	    	overrides.setProperty(key, value);
	}
	
	public void setProperties(Properties props) {
	    overrides.putAll(props);
	}

	public Properties getProperties() {
	    
	    Properties p = new Properties();
	    p.putAll(System.getProperties());
	    p.putAll(props);
	    p.putAll(overrides);
	    
		return p;
	}
	
	public Map<String, String> getModelAssignedDatabaseTypes() {
		return this.modelAssignedDatabaseType;
	}
	
	public void setModelAssignedToDatabaseType(String modelname, String dbtype) {
		this.modelAssignedDatabaseType.put(modelname, dbtype);
	}


	public static void main(String[] args) {
		System.setProperty("test", "value");

		ConfigPropertyLoader _instance = ConfigPropertyLoader.getInstance();
		Properties p = _instance.getProperties();
		if (p == null || p.isEmpty()) {
			throw new RuntimeException("Failed to load config properties file");

		}
		if (!p.getProperty("test").equalsIgnoreCase("value")) {
			throw new RuntimeException("Failed to pickup system property");
		}
		
		_instance.setProperty("override", "ovalue");
		
		if (!_instance.getProperties().getProperty("override").equalsIgnoreCase("ovalue")) {
				throw new RuntimeException("Override value wasnt found");
		}
		
		ConfigPropertyLoader.reset();
		if (_instance.getProperties().getProperty("override") != null) {
			throw new RuntimeException("Override value was found, should have been removed on reset");
		}
		
		if (!p.getProperty("test").equalsIgnoreCase("value")) {
			throw new RuntimeException("Failed to pickup system property");
		}
		
		System.out.println("Loaded Config Properties " + p.toString());

	}

}
