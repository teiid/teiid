package org.teiid.test.framework;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.teiid.test.framework.datasource.DataSourceFactory;
import org.teiid.test.framework.exception.TransactionRuntimeException;
import org.teiid.test.util.PropUtils;

import com.metamatrix.common.util.PropertiesUtils;


/**
 * The ConfigProperteryLoader will load the configuration properties to be used by a test.
 * These properties only live for the duration of one test.
 * 
 * NOTE: System properties set by the VM will be considered long living.   This is so the 
 * 		-Dusedatasources ( {@link ConfigPropertyNames#USE_DATASOURCES_PROP} ) option can be maintained for the duration of a set of tests. 
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
	
	public static synchronized void cleanup() {
	    _instance.overrides.clear();
	    _instance.modelAssignedDatabaseType.clear();
	    _instance.props.clear();
	    if (_instance.dsfactory != null) {
		_instance.dsfactory.cleanup();
	    }
	    _instance = null;
	    LAST_CONFIG_FILE=null;
	}

	
	private void initialize() {
	    loadProperties(LAST_CONFIG_FILE);
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

	private void loadProperties(String filename) {
		
		props = PropUtils.loadProperties("/" + filename, null);

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
		System.out.println("Loaded Config Properties " + p.toString());

	}

}
