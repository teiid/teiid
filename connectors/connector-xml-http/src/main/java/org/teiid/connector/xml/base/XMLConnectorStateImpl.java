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


package org.teiid.connector.xml.base;

import java.io.InputStream;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.xml.XMLConnectorState;
import org.teiid.connector.xml.http.HTTPManagedConnectionFactory;

import com.metamatrix.connector.xml.IQueryPreprocessor;
import com.metamatrix.connector.xml.SAXFilterProvider;
import com.metamatrix.connector.xml.base.LoggingInputStreamFilter;
import com.metamatrix.core.util.ReflectionHelper;

public abstract class XMLConnectorStateImpl implements Cloneable,
        XMLConnectorState {

    private boolean m_preprocess;

    private String m_saxProviderClass;

    private String m_queryPreprocessorClass;

    private boolean m_logRequestResponse;
    
    private String m_pluggableInputStreamFilterClass;

    public static final String SAX_FILTER_PROVIDER_CLASS_DEFAULT = ""; //$NON-NLS-1$

    public static final String QUERY_PREPROCESS_CLASS_DEFAULT = ""; //$NON-NLS-1$
    
    private static final String INPUT_STREAM_FILTER_CLASS_DEFAULT = ""; //$NON-NLS-1$

    protected ConnectorLogger logger;

    private IQueryPreprocessor preprocessor;

    private SAXFilterProvider provider;

    public XMLConnectorStateImpl() {
        setPreprocess(true);
        setLogRequestResponse(false);
        setSaxProviderClass(SAX_FILTER_PROVIDER_CLASS_DEFAULT);
        setQueryPreprocessorClass(QUERY_PREPROCESS_CLASS_DEFAULT);
        setPluggableInputStreamFilterClass(INPUT_STREAM_FILTER_CLASS_DEFAULT);
    }

    public void setState(HTTPManagedConnectionFactory env) throws ConnectorException {
        if (logger == null) {
            throw new RuntimeException("Internal Exception: logger is null");
        }
        setLogRequestResponse(env.getLogRequestResponseDocs());

        String provider = env.getSaxFilterProviderClass();
        if (provider != null && !provider.equals(SAX_FILTER_PROVIDER_CLASS_DEFAULT)) {
            setSaxProviderClass(provider);
            setSAXFilterProvider(loadSAXFilter(getSaxProviderClass()));
        }
        
        String preprocessor = env.getQueryPreprocessorClass();
        if (preprocessor != null && !preprocessor.equals(QUERY_PREPROCESS_CLASS_DEFAULT)) {
            setQueryPreprocessorClass(preprocessor);
            setPreprocessor(loadPreprocessor(getQueryPreprocessorClass()));
        }
        
        String streamFilter = env.getInputStreamFilterClass();
        if(streamFilter != null && !streamFilter.equals(INPUT_STREAM_FILTER_CLASS_DEFAULT)) {
        	setPluggableInputStreamFilterClass(streamFilter);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#setLogger(com.metamatrix.data.api.ConnectorLogger)
     */
    public void setLogger(ConnectorLogger logger) {
        this.logger = logger;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getLogger()
     */
    public ConnectorLogger getLogger() {
        return logger;
    }

    protected boolean isNotNullOrEmpty(String value) {
        return (value != null && !value.equals(""));
    }

    private void setPreprocess(boolean preprocess) {
        this.m_preprocess = preprocess;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#isPreprocess()
     */
    public boolean isPreprocess() {
        return m_preprocess;
    }

    private void setLogRequestResponse(boolean logRequestResponse) {
        m_logRequestResponse = logRequestResponse;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#isLogRequestResponse()
     */
    public boolean isLogRequestResponse() {
        return m_logRequestResponse;
    }

    private void setSaxProviderClass(String m_saxProviderClass) {
        this.m_saxProviderClass = m_saxProviderClass;
    }

    private String getSaxProviderClass() {
        return m_saxProviderClass;
    }

    private void setSAXFilterProvider(SAXFilterProvider provider) {
        this.provider = provider;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getSAXFilterProvider()
     */
    public SAXFilterProvider getSAXFilterProvider() {
        return provider;
    }

    private void setQueryPreprocessorClass(String preprocessor) {
        m_queryPreprocessorClass = preprocessor;
    }

    private String getQueryPreprocessorClass() {
        return m_queryPreprocessorClass;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getPreprocessor()
     */
    public IQueryPreprocessor getPreprocessor() {
        return preprocessor;
    }

    private void setPreprocessor(IQueryPreprocessor preprocessor) {
        this.preprocessor = preprocessor;
    }

    /**
     * Class loads the Query Preprocessor Implementation class defined in the
     * connector binding.
     */
    private IQueryPreprocessor loadPreprocessor(String queryPreprocessorClass)
            throws ConnectorException {
        IQueryPreprocessor pre = null;
        try {
        	Class clazz = Thread.currentThread().getContextClassLoader().loadClass(queryPreprocessorClass);
            pre = (IQueryPreprocessor) clazz.newInstance();
        } catch (Exception e) {
            logger.logError(e.getMessage(), e);
            throw new ConnectorException(e);
        }
        return pre;
    }

    /**
     * Class loads the SAX Filter Provider Implementation class defined in the
     * connector binding.
     */
    private SAXFilterProvider loadSAXFilter(String SAXFilterClass)
            throws ConnectorException {
        SAXFilterProvider filter = null;
        try {
        	Class clazz = Thread.currentThread().getContextClassLoader().loadClass(SAXFilterClass);
            filter = (SAXFilterProvider) clazz.newInstance();
        } catch (Exception e) {
            logger.logError(e.getMessage(), e);
            throw new ConnectorException(e);
        }
        return filter;
    }
    
    public void setPluggableInputStreamFilterClass(String filterClass) {
		m_pluggableInputStreamFilterClass = filterClass;
	}
    
	public String getPluggableInputStreamFilterClass() {
		return m_pluggableInputStreamFilterClass;
	}
	
	public InputStream addStreamFilters(InputStream stream)
	throws ConnectorException {

		if (isLogRequestResponse()) {
			stream = new LoggingInputStreamFilter(stream, logger);
		}
		
		if (getPluggableInputStreamFilterClass() != null) {
			try {
				stream = (InputStream) ReflectionHelper.create(getPluggableInputStreamFilterClass(), new Object[] { stream,
					logger}, new Class[] {
					java.io.InputStream.class,
					ConnectorLogger.class }, Thread.currentThread().getContextClassLoader());
			} catch (Exception cnf) {
				throw new ConnectorException(cnf);
			}
		}
		return stream;
	}
	
}
