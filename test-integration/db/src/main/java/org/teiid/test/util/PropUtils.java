package org.teiid.test.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.teiid.test.framework.ConfigPropertyLoader;

public class PropUtils {
	


	public static Properties loadProperties(String filename, Properties defaults) {
		Properties props = null;
		if (defaults != null) {
			props = new Properties(defaults);
		} else {
			props = new Properties();
		}
	    try {
	        InputStream in = ConfigPropertyLoader.class.getResourceAsStream(filename);
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
	    
	    return props;
	}
}
