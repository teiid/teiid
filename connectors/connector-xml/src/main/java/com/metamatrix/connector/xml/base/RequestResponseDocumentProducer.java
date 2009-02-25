/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */



package com.metamatrix.connector.xml.base;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.connector.xml.DocumentProducer;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.http.HTTPConnectorState;
import com.metamatrix.connector.xml.http.Messages;

/**
 * The RequestResponseDocumentProducer is responsible for executing a single instance of a request permutation
 * as broken out by the QueryAnalyzer/RequestGenerator and producing a Response.
 */
public abstract class RequestResponseDocumentProducer implements DocumentProducer
{
    static public final String NAMESPACE_PREFIX_PROPERTY_NAME = "NamespacePrefixes"; //$NON-NLS-1$

    private XMLConnectorState m_state;

	private XMLExecution m_execution;

	protected XMLExtractor xmlExtractor;
	
	protected RequestResponseDocumentProducer(XMLConnectorState state, XMLExecution execution) throws ConnectorException {
		m_state = state;
		m_execution = execution;
        String cacheLocation = m_state.getCacheLocation();
		File cacheFolder = (cacheLocation == null || cacheLocation.trim().length() ==0) ? null : new File(cacheLocation);
		xmlExtractor = new XMLExtractor(state.getMaxInMemoryStringSize(), state.isPreprocess(), state.isLogRequestResponse(), cacheFolder, state.getLogger());
		processOutputXPathDescs(m_execution.getInfo().getRequestedColumns()
        		, m_execution.getInfo().getParameters());
	}

	/**
     * Called by RequestResponseDocumentProducer to determine if an where criteria
     * value can be stashed for projection into a result set.  For criteria with multiple
     * calues this is generally false.
     */
    public boolean cannotProjectParameter(CriteriaDesc parmCriteria)
    {
        return false;
    }

	protected XMLConnectorState getState() {
		return m_state;
	}

	protected ExecutionInfo getExecutionInfo() {
		return m_execution.getInfo();
	}

	protected XMLExecution getExecution() {
		return m_execution;
	}

	protected ConnectorLogger getLogger() {
		return m_state.getLogger();
	}

	protected XMLExtractor getExtractor() {
		return xmlExtractor;
	}

	/**
	 * Because of the structure of relational databases it is a simple and common practice
	 * to return the vaule of a critera in a result set.  For instance, 
	 * SELECT name, ssn from people where ssn='xxx-xx-xxxx'
	 * In a Request/Response XML scenario, there is no guarantee that ssn is in the response.  
	 * In most cases it will not be.  In order to meet the relational users expectation that
	 * the value for a select critera can be returned we stash the value from the parameter 
	 * in the output value and then fetch it when gathering results if possible. In some cases
	 * this is not possible, and in those cases we throw a ConnectorException. Implementations
	 * of this class can override cannotProjectParameter(CriteriaDesc parmCriteria) to make the 
	 * determination.
	 */
	private void processOutputXPathDescs(final List requestedColumns, final List parameterPairs) throws ConnectorException {
	    for (int i = 0; i < requestedColumns.size(); i++) {
	        OutputXPathDesc xPath = (com.metamatrix.connector.xml.base.OutputXPathDesc) requestedColumns.get(i);
	        if (xPath.isParameter() && xPath.getXPath() == null) {
	            setOutputValues(parameterPairs, xPath);
	        }
	    }
	}

	/**
	 * Put the input parameter value in the result column value if possible.
	 */
	private void setOutputValues(final List parameterPairs, OutputXPathDesc xPath) throws ConnectorException {
	    int colNum = xPath.getColumnNumber();
	    for (int x = 0; x < parameterPairs.size(); x++) {
	        CriteriaDesc parmCriteria =
	                    (CriteriaDesc) parameterPairs.get(x);
	        if (parmCriteria.getColumnNumber() == colNum) {
	            if (cannotProjectParameter(parmCriteria)) {
	                throw new ConnectorException(Messages.getString("HTTPExecutor.cannot.project.repeating.values")); //$NON-NLS-1$
	            } else {
	                xPath.setCurrentValue(parmCriteria.getCurrentIndexValue());
	                break;
	            }
	        }
	    }        
	}
	
	protected void attemptConditionalLog(String message) {
	    if (getState().isLogRequestResponse()) {
	        getLogger().logInfo(message);
	    }
	}

	public InputStream addStreamFilters(InputStream response, ConnectorLogger logger)
			throws ConnectorException {
				
				if(getState().isLogRequestResponse()) {
					response = new LoggingInputStreamFilter(response, logger);
				}
				
				InputStream filter = null;
				try {
					Class pluggableFilter = Thread.currentThread().getContextClassLoader().loadClass(getState().getPluggableInputStreamFilterClass());
					Constructor ctor = pluggableFilter.getConstructor(
							new Class[] { java.io.InputStream.class, org.teiid.connector.api.ConnectorLogger.class});
					filter = (InputStream) ctor.newInstance(new Object[] {response, logger});
				} catch (Exception cnf) {
					throw new ConnectorException(cnf);
				}
				return filter;
			}

	protected String buildUriString() {
	    String uriString = "<" + buildRawUriString() + ">"; //$NON-NLS-1$
	    getLogger().logDetail("XML Connector Framework: using url " + uriString); //$NON-NLS-1$
	    return uriString;
	}

	protected String buildRawUriString() {
	    String location = getExecutionInfo().getLocation();
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
	        location = getExecutionInfo().getOtherProperties().getProperty(
	                tableServletCallPathProp);
	    }
	
	    String retval = ((HTTPConnectorState) getState()).getUri();
	    if (location != null && location.trim().length() > 0) {
	        retval = retval + "/" + location; //$NON-NLS-1$
	    }
	    return retval;
	}

	protected String removeAngleBrackets(String uri) {
	    char[] result = new char[uri.length()];
	    char[] source = uri.toCharArray();
	    int r = 0;
	    char v;
	    for(int i = 0; i < source.length; i++) {
	          v = source[i];
	          if(v == '<' || v == '>')
	             continue;
	          result[r] = v;
	          ++r;
	    }
	    return new String(result).trim();
	}

	/**
	 * Examines the Query to determine if a request to a source is needed.  If any of the 
	 * request parameters is a ResponseIn, then we don't need to make a request because it 
	 * has already been made by another call to Execution.execute()
	 */ 
    public static boolean checkIfRequestIsNeeded(ExecutionInfo info) throws ConnectorException {
    	List cols = info.getRequestedColumns();
    	boolean retVal = true;
    	Iterator paramIter = cols.iterator();
    	while(paramIter.hasNext()) {
    		ParameterDescriptor desc = (ParameterDescriptor) paramIter.next();
    		if(desc.getRole().equalsIgnoreCase(ParameterDescriptor.ROLE_COLUMN_PROPERTY_NAME_RESPONSE_IN)) {
    			retVal = false;
    			break;
    		}
    	}
    	return retVal;
    }
}