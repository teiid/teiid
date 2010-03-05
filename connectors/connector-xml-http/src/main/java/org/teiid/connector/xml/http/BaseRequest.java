package org.teiid.connector.xml.http;

import java.net.MalformedURLException;
import java.net.URL;

import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.xml.base.ExecutionInfo;

public abstract class BaseRequest {
	
	protected String uriString;
    protected HTTPConnectorState state;
	protected ExecutionInfo exeInfo;	
	
    BaseRequest(HTTPConnectorState state , ExecutionInfo exeInfo) {
    	this.state = state;
    	this.exeInfo = exeInfo;
    }
    
    abstract protected void initialize() throws ConnectorException;
	
	protected String getUriString() {
	    if(null != uriString) {
	    	return uriString;
	    } 
    	uriString = "<" + buildRawUriString() + ">"; //$NON-NLS-1$
    	return uriString;
	}

	protected String buildRawUriString() {
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
	        location = this.exeInfo.getOtherProperties().get(tableServletCallPathProp);
	    }
	
	    String retval = state.getUri();
	    if (location != null && location.trim().length() > 0) {
	        retval = retval + "/" + location; //$NON-NLS-1$
	    }
	    return retval;
	}

}
