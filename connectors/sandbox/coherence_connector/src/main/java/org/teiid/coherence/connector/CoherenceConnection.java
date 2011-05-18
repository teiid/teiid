package org.teiid.coherence.connector;

import java.util.List;

import javax.resource.ResourceException;


public interface CoherenceConnection {
		
	public List<Object> get(String criteria) throws ResourceException;


}
