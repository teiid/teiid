package org.teiid.test.framework;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.teiid.test.framework.connection.ConnectionStrategyFactory;

public class ConfigPropertyLoader {
	/**
	 * Specify this property to set a specific configuration to use
	 */
		public static final String CONFIG_FILE="config";
		
		/**
		 * The default config file to use when #CONFIG_FILE system property isn't set
		 */
		private static final String DEFAULT_CONFIG_FILE_NAME="default-config.properties";

		public static Properties loadConfigurationProperties() {
			return loadConfiguration();
		}
		
	    private static Properties loadConfiguration() {
	        String filename = System.getProperty(CONFIG_FILE);
	        if (filename == null) {
	            filename = DEFAULT_CONFIG_FILE_NAME;
	        }       
	        
	        return loadProperties(filename);

	    } 
	    
		private static Properties loadProperties(String filename) {
			Properties props = null;
		    try {
		        InputStream in = ConnectionStrategyFactory.class.getResourceAsStream("/"+ filename);
		        if (in != null) {
		        	props = new Properties();
		        	props.load(in);
		        	return props;
		        }
		        else {
		        	throw new RuntimeException("Failed to load properties from file '"+filename+ "' configuration file");
		        }
		    } catch (IOException e) {
		        throw new RuntimeException("Error loading properties from file '"+filename+ "'" + e.getMessage());
		    }
		}
		
		public static void main(String[] args) {
			Properties props = ConfigPropertyLoader.loadConfigurationProperties();
			if (props == null || props.isEmpty()) {
	        	throw new RuntimeException("Failed to load config properties file");
		
			}
			System.out.println("Loaded Config Properties " + props.toString());

		}
	
}
