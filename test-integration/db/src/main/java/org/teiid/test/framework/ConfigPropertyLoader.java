package org.teiid.test.framework;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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

	private Properties props = null;
	
	private Map<String, String>modelAssignedDatabaseType = new HashMap<String, String>(5);

	private ConfigPropertyLoader() {
	}

	public static synchronized ConfigPropertyLoader createInstance() {
		ConfigPropertyLoader _instance = null;
			_instance = new ConfigPropertyLoader();
			try {
				_instance.loadConfigurationProperties();
			} catch (TransactionRuntimeException e) {
				throw e;
			}

		return _instance;
	}

	private void loadConfigurationProperties() {
		String filename = System.getProperty(ConfigPropertyNames.CONFIG_FILE);
		if (filename == null) {
			filename = DEFAULT_CONFIG_FILE_NAME;
		}

		loadProperties(filename);
		
	}


	public String getProperty(String key) {
		return getProperties().getProperty(key);
	}
	
	public void setProperty(String key, String value) {
		props.setProperty(key, value);
	}

	public Properties getProperties() {
		return props;
	}
	
	public Map getModelAssignedDatabaseTypes() {
		return this.modelAssignedDatabaseType;
	}
	
	public void setModelAssignedToDatabaseType(String modelname, String dbtype) {
		this.modelAssignedDatabaseType.put(modelname, dbtype);
	}

	private void loadProperties(String filename) {
		
		Properties sysprops = PropertiesUtils.clone(System.getProperties());
		
		props = PropUtils
				.loadProperties("/" + filename, sysprops);

	}

	public static void main(String[] args) {
		System.setProperty("test", "value");

		ConfigPropertyLoader _instance = ConfigPropertyLoader.createInstance();
		Properties p = _instance.getProperties();
		if (p == null || p.isEmpty()) {
			throw new RuntimeException("Failed to load config properties file");

		}
		if (p.getProperty("test") == null) {
			throw new RuntimeException("Failed to pickup system property");
		}
		System.out.println("Loaded Config Properties " + p.toString());

	}

}
