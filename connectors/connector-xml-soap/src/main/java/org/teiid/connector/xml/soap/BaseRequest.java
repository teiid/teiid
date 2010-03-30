package org.teiid.connector.xml.soap;

import java.net.MalformedURLException;
import java.net.URL;

import com.metamatrix.connector.xml.base.ExecutionInfo;

public abstract class BaseRequest {
	
	protected String uriString;
	
    BaseRequest(String uri, ExecutionInfo exeInfo) {
    	this.uriString = buildRawUriString(uri, exeInfo);
    }
	
	protected String getUriString() {
    	return uriString;
	}

	private String buildRawUriString(String uri, ExecutionInfo exeInfo) {
	    String location = exeInfo.getLocation();
	    if (location != null) {
	        // If the location is a URL, it replaces the full URL (first part
	        // set in the
	        // connector binding and second part set in the model).
	        try {
	            new URL(location);
	            return location;
	        } catch (MalformedURLException e) {
	        }
	    }
	
	    if (location == null) {
	        final String tableServletCallPathProp = "ServletCallPathforURL"; //$NON-NLS-1$
	        location = exeInfo.getOtherProperties().get(tableServletCallPathProp);
	    }
	
	    String retval = uri;
	    if (location != null && location.trim().length() > 0) {
	        retval = retval + "/" + location; //$NON-NLS-1$
	    }
	    return retval;
	}

}
