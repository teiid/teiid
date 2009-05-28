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

import java.util.Properties;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.connector.xml.IQueryPreprocessor;
import com.metamatrix.connector.xml.SAXFilterProvider;
import com.metamatrix.connector.xml.XMLConnectorState;

public abstract class XMLConnectorStateImpl implements Cloneable,
        XMLConnectorState {

    private int m_cacheTimeout;

    private int m_maxMemoryCacheSize;

    private int m_maxInMemoryStringSize;

    private int m_maxFileCacheSize;

    private boolean m_preprocess;

    private String m_cacheLocation;

    private String m_saxProviderClass;

    private String m_queryPreprocessorClass;

    private boolean m_logRequestResponse;
    
    private String m_pluggableInputStreamFilterClass;

    private static final int SECONDS_TO_MILLIS = 1000;

    private static final int KB_TO_BYTES = 1000;

    public static final String CACHE_TIMEOUT = "CacheTimeout"; //$NON-NLS-1$

    public static final String CACHE_ENABLED = "CacheEnabled"; //$NON-NLS-1$

    public static final String MAX_IN_MEMORY_STRING_SIZE = "MaxInMemoryStringSize"; //$NON-NLS-1$

    public static final String MAX_MEMORY_CACHE_SIZE = "MaxMemoryCacheSize"; //$NON-NLS-1$

    public static final String MAX_FILE_CACHE_SIZE = "MaxFileCacheSize"; //$NON-NLS-1$

    public static final String FILE_CACHE_LOCATION = "FileCacheLocation"; //$NON-NLS-1$

    public static final String LOG_REQUEST_RESPONSE_DOCS = "LogRequestResponseDocs"; //$NON-NLS-1$

    public static final String SAX_FILTER_PROVIDER_CLASS = "SaxFilterProviderClass"; //$NON-NLS-1$

    public static final String QUERY_PREPROCESS_CLASS = "QueryPreprocessorClass"; //$NON-NLS-1$
    
    public static final String INPUT_STREAM_FILTER_CLASS = "InputStreamFilterClass"; //$NON-NLS-1$

    public static final String SAX_FILTER_PROVIDER_CLASS_DEFAULT = "com.metamatrix.connector.xml.base.NoExtendedFilters"; //$NON-NLS-1$

    public static final String QUERY_PREPROCESS_CLASS_DEFAULT = "com.metamatrix.connector.xml.base.NoQueryPreprocessing"; //$NON-NLS-1$
    
    private static final String INPUT_STREAM_FILTER_CLASS_DEFAULT = "com.metamatrix.connector.xml.base.PluggableInputStreamFilterImpl"; //$NON-NLS-1$

    public static final String CONNECTOR_CAPABILITES = "ConnectorCapabilities";

    protected ConnectorLogger logger;

    private IQueryPreprocessor preprocessor;

    private SAXFilterProvider provider;

    private ConnectorCapabilities capabilites;

    private String capabilitiesClass;

	private Boolean caching;

    public XMLConnectorStateImpl() {
        final int defaultCacheTimeoutMillis = 60000;
        final int defaultMemoryCacheSize = 512;
        final int defaultFileCacheSize = -1; // unbounded
        setCacheTimeoutMillis(defaultCacheTimeoutMillis);
        setMaxMemoryCacheSizeKB(defaultMemoryCacheSize);
        setMaxInMemoryStringSize(128 * 1024);
        setMaxFileCacheSizeKB(defaultFileCacheSize);
        setPreprocess(true);
        setLogRequestResponse(false);
        setSaxProviderClass(SAX_FILTER_PROVIDER_CLASS_DEFAULT);
        setQueryPreprocessorClass(QUERY_PREPROCESS_CLASS_DEFAULT);
        setPluggableInputStreamFilterClass(INPUT_STREAM_FILTER_CLASS_DEFAULT);
    }

    public void setState(ConnectorEnvironment env) throws ConnectorException {
        if (logger == null) {
            throw new RuntimeException("Internal Exception: logger is null");
        }
        Properties props = env.getProperties();
        String cachingString = props.getProperty(CACHE_ENABLED);
        if (cachingString != null) {
        	Boolean caching = Boolean.parseBoolean(cachingString);
        	setCaching(caching);
        }
        
        String cache = props.getProperty(CACHE_TIMEOUT);
        if (cache != null) {
            setCacheTimeoutSeconds(Integer.parseInt(cache));
        }
        String maxMCache = props.getProperty(MAX_MEMORY_CACHE_SIZE);
        if (maxMCache != null) {
            setMaxMemoryCacheSizeKB(Integer.parseInt(maxMCache));
        }

        String maxStringSize = props.getProperty(MAX_IN_MEMORY_STRING_SIZE);
        if (maxStringSize != null) {
            setMaxInMemoryStringSize(Integer.parseInt(maxStringSize) * 1024);
        }

        String maxFCache = props.getProperty(MAX_FILE_CACHE_SIZE);
        if (maxFCache != null) {
            setMaxFileCacheSizeKB(Integer.parseInt(maxFCache));
        }

        String cacheLoc = props.getProperty(FILE_CACHE_LOCATION);
        if (cacheLoc != null) {
            setCacheLocation(cacheLoc);
        } else {
            String temp = System.getProperty("java.io.tmpdir");
            setCacheLocation(temp);
        }
        
        String logReqRes = props.getProperty(LOG_REQUEST_RESPONSE_DOCS);
        if (logReqRes != null) {
            setLogRequestResponse(Boolean.valueOf(logReqRes).booleanValue());
        }

        String capabilitesClass = props.getProperty(CONNECTOR_CAPABILITES);
        if (capabilitesClass != null && !capabilitesClass.equals("")) {
            setConnectorCapabilitiesClass(capabilitesClass);
            setCapabilites(loadConnectorCapabilities(getConnectorCapabilitiesClass()));
        } else {
            throw new ConnectorException(
                    "The Connector Capabilities Class is null or empty");
        }

        String provider = props.getProperty(SAX_FILTER_PROVIDER_CLASS);
        if (provider != null && !provider.equals("")) {
            setSaxProviderClass(provider);
            setSAXFilterProvider(loadSAXFilter(getSaxProviderClass()));
        } else {
            throw new ConnectorException(
                    "The SAX Filter Privider Class is null or empty");
        }

        String preprocessor = props.getProperty(QUERY_PREPROCESS_CLASS);
        if (preprocessor != null && !preprocessor.equals("")) {
            setQueryPreprocessorClass(preprocessor);
            setPreprocessor(loadPreprocessor(getQueryPreprocessorClass()));
        } else {
            throw new ConnectorException(
                    "The Query Preprocessor Class is null or empty");
        }
        
        String streamFilter = props.getProperty(INPUT_STREAM_FILTER_CLASS);
        if(streamFilter != null && !streamFilter.equals("")) {
        	setPluggableInputStreamFilterClass(streamFilter);
        }
    }

	private void setCaching(Boolean caching) {
		this.caching = caching;	
	}
	
	public boolean isCaching() {
		return this.caching;
	}

	private ConnectorCapabilities loadConnectorCapabilities(
            String connectorCapabilitiesClass) throws ConnectorException {
        ConnectorCapabilities caps = null;
        try {
        	Class clazz = Thread.currentThread().getContextClassLoader().loadClass(connectorCapabilitiesClass);
            caps = (ConnectorCapabilities) clazz.newInstance();
        } catch (Exception e) {
            logger.logError(e.getMessage(), e);
            throw new ConnectorException(e);
        }
        return caps;
    }

    private void setConnectorCapabilitiesClass(String capabilitesClass) {
        this.capabilitiesClass = capabilitesClass;
    }

    private String getConnectorCapabilitiesClass() {
        return capabilitiesClass;
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

    public java.util.Properties getState() {
        Properties props = new Properties();
        props.setProperty(CACHE_TIMEOUT, Integer
                .toString(getCacheTimeoutSeconds()));
        props.setProperty(MAX_MEMORY_CACHE_SIZE, Integer
                .toString(getMaxMemoryCacheSizeKB()));
        props.setProperty(MAX_IN_MEMORY_STRING_SIZE, Integer
                .toString(getMaxInMemoryStringSize()));
        props.setProperty(MAX_FILE_CACHE_SIZE, Integer
                .toString(getMaxFileCacheSizeKB()));
        props.setProperty(LOG_REQUEST_RESPONSE_DOCS, Boolean
                .toString(isLogRequestResponse()));
        String location = getCacheLocation();
        if (location == null) {
            location = "";//$NON-NLS-1$
        }
        props.setProperty(FILE_CACHE_LOCATION, location);
        props.setProperty(SAX_FILTER_PROVIDER_CLASS, getSaxProviderClass());
        props.setProperty(QUERY_PREPROCESS_CLASS, getQueryPreprocessorClass());
        return props;
    }

    protected boolean isNotNullOrEmpty(String value) {
        return (value != null && !value.equals(""));
    }

    /**
     * @param m_cacheTimeout
     *            The m_cacheTimeout to set.
     */
    private final void setCacheTimeoutSeconds(int cacheTimeoutseconds) {

        m_cacheTimeout = cacheTimeoutseconds * SECONDS_TO_MILLIS;
    }

    private final void setCacheTimeoutMillis(int cacheTimeoutmillis) {

        m_cacheTimeout = cacheTimeoutmillis;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getCacheTimeoutSeconds()
     */
    public final int getCacheTimeoutSeconds() {
        return m_cacheTimeout / SECONDS_TO_MILLIS;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getCacheTimeoutMillis()
     */
    public final int getCacheTimeoutMillis() {
        return m_cacheTimeout;
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

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getConnectorCapabilities()
     */
    public ConnectorCapabilities getConnectorCapabilities() {
        return capabilites;
    }

    private void setCapabilites(ConnectorCapabilities capabilities) {
        this.capabilites = capabilities;
    }

    private void setMaxMemoryCacheSizeKB(int maxMemoryCacheSizeKB) {
        m_maxMemoryCacheSize = maxMemoryCacheSizeKB * KB_TO_BYTES;
    }

    private void setMaxMemoryCacheSizeBytes(int maxMemoryCacheSizeByte) {
        m_maxMemoryCacheSize = maxMemoryCacheSizeByte;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getMaxMemoryCacheSizeByte()
     */
    public int getMaxMemoryCacheSizeByte() {
        return m_maxMemoryCacheSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getMaxMemoryCacheSizeKB()
     */
    public int getMaxMemoryCacheSizeKB() {
        return m_maxMemoryCacheSize / KB_TO_BYTES;
    }

    private void setMaxInMemoryStringSize(int maxInMemoryStringSize) {
        m_maxInMemoryStringSize = maxInMemoryStringSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getMaxInMemoryStringSize()
     */
    public int getMaxInMemoryStringSize() {
        return m_maxInMemoryStringSize;
    }

    private void setMaxFileCacheSizeByte(int maxFileCacheSize) {
        m_maxFileCacheSize = maxFileCacheSize;
    }

    private void setMaxFileCacheSizeKB(int maxFileCacheSize) {
        m_maxFileCacheSize = maxFileCacheSize * KB_TO_BYTES;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getMaxFileCacheSizeKB()
     */
    public int getMaxFileCacheSizeKB() {
        return m_maxFileCacheSize / KB_TO_BYTES;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getMaxFileCacheSizeByte()
     */
    public int getMaxFileCacheSizeByte() {
        return m_maxFileCacheSize;
    }

    private void setCacheLocation(String cacheLocation) {
        m_cacheLocation = cacheLocation;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.connector.xml.base.XMLConnectorState#getCacheLocation()
     */
    public String getCacheLocation() {
        return m_cacheLocation;
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
}
