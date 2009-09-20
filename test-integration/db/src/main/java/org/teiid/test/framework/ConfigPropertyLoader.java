package org.teiid.test.framework;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.teiid.test.framework.connection.ConnectionStrategyFactory;

public class ConfigPropertyLoader {
		
		/**
		 * The default config file to use when #CONFIG_FILE system property isn't set
		 */
		protected static final String DEFAULT_CONFIG_FILE_NAME="default-config.properties";
		
		private static Properties props = null;

		public synchronized static void loadConfigurationProperties() {
	        String filename = System.getProperty(ConfigPropertyNames.CONFIG_FILE);
	        if (filename == null) {
	            filename = DEFAULT_CONFIG_FILE_NAME;
	        }       
	        
	        loadProperties(filename);		}
		
		public static String getProperty(String key) {
			return getProperties().getProperty(key);
		}
		
		public synchronized static Properties getProperties() {
			return props;
		}
	    
		private static void loadProperties(String filename) {
			props = System.getProperties();
		    try {
		        InputStream in = ConfigPropertyLoader.class.getResourceAsStream("/"+ filename);
		        if (in != null) {
		        	Properties lprops = new Properties();
		        	lprops.load(in);
		        	props.putAll(lprops);
		        	
		        }
		        else {
		        	throw new RuntimeException("Failed to load properties from file '"+filename+ "' configuration file");
		        }
		    } catch (IOException e) {
		        throw new RuntimeException("Error loading properties from file '"+filename+ "'" + e.getMessage());
		    }
		}
		
		public static void main(String[] args) {
			System.setProperty("test", "value");
			
			ConfigPropertyLoader.loadConfigurationProperties();
			Properties p = ConfigPropertyLoader.getProperties();
			if (p == null || p.isEmpty()) {
	        	throw new RuntimeException("Failed to load config properties file");
		
			}
			if (p.getProperty("test") == null) {
				throw new RuntimeException("Failed to pickup system property");
			}
			System.out.println("Loaded Config Properties " + p.toString());

		}
	
}
