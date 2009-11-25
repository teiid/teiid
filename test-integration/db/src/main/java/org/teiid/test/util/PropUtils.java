package org.teiid.test.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.teiid.test.framework.ConfigPropertyLoader;

public class PropUtils {
	


	public static Properties loadProperties(String filename, Properties defaults) {
	    InputStream in = null;
		Properties props = new Properties();
		if (defaults != null) {
			props.putAll(defaults);
		} 
	    try {
	        in = ConfigPropertyLoader.class.getResourceAsStream(filename);
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
	    } finally {
		try {
		    in.close();
		} catch(Exception e){
		    
		}
	    }
	    
	    return props;
	}
}
