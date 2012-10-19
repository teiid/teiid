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

package org.teiid.jdbc;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.net.TeiidURL;

/** 
 * @since 4.3
 */
public class JDBCURL {
    private static final String UTF_8 = "UTF-8"; //$NON-NLS-1$
    public static final String JDBC_PROTOCOL = "jdbc:teiid:"; //$NON-NLS-1$
    
    static final String URL_PATTERN = JDBC_PROTOCOL + "([\\w-\\.]+)(?:@([^;]*))?(;.*)?"; //$NON-NLS-1$
    static Pattern urlPattern = Pattern.compile(URL_PATTERN);

    public static final Set<String> EXECUTION_PROPERTIES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
            ExecutionProperties.PROP_TXN_AUTO_WRAP,
            ExecutionProperties.PROP_PARTIAL_RESULTS_MODE,
            ExecutionProperties.RESULT_SET_CACHE_MODE,
            ExecutionProperties.ANSI_QUOTED_IDENTIFIERS,
            ExecutionProperties.SQL_OPTION_SHOWPLAN,
            ExecutionProperties.NOEXEC,
            ExecutionProperties.PROP_FETCH_SIZE,
            ExecutionProperties.PROP_XML_FORMAT,
            ExecutionProperties.PROP_XML_VALIDATION,
            EmbeddedProfile.USE_CALLING_THREAD,
            ExecutionProperties.DISABLE_LOCAL_TRANSACTIONS,
            ExecutionProperties.JDBC4COLUMNNAMEANDLABELSEMANTICS)));
    
    public static final Set<String> KNOWN_PROPERTIES = getKnownProperties();
    
    private static Set<String> getKnownProperties() {
    	Set<String> props = new HashSet<String>(Arrays.asList(
    	        BaseDataSource.APP_NAME,
    	        BaseDataSource.VDB_NAME,
    	        BaseDataSource.VERSION,
    	        BaseDataSource.VDB_VERSION,
    	        BaseDataSource.USER_NAME,
    	        BaseDataSource.PASSWORD,
    	        TeiidURL.CONNECTION.AUTO_FAILOVER,
    	        TeiidURL.CONNECTION.DISCOVERY_STRATEGY,
    	        TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION,
    	        TeiidURL.CONNECTION.JAAS_NAME,
    	        TeiidURL.CONNECTION.KERBEROS_SERVICE_PRINCIPLE_NAME,
    	        TeiidURL.CONNECTION.ENCRYPT_REQUESTS));
    	props.addAll(EXECUTION_PROPERTIES);
    	return Collections.unmodifiableSet(props);
    }
    
    private String vdbName;
    private String connectionURL;
    private Properties properties = new Properties();
    
    public enum ConnectionType {
    	Embedded,
    	Socket
    }
    
    public static ConnectionType acceptsUrl(String url) {
    	Matcher m = urlPattern.matcher(url);
    	if (m.matches()) {
    		return m.group(2) != null?ConnectionType.Socket:ConnectionType.Embedded;
    	}
    	return null;
    }
    
    private String urlString;
    
    public JDBCURL(String jdbcURL) {
        parseURL(jdbcURL);
    }
    
    public JDBCURL(String vdbName, String connectionURL, Properties props) {
        if (vdbName == null || vdbName.trim().length() == 0) {
            throw new IllegalArgumentException();
        }
        this.vdbName = vdbName;
        this.connectionURL = connectionURL;
        if (props != null) {
            normalizeProperties(props, this.properties);
        }
    }
    
    public String getVDBName() {
        return vdbName;
    }
    
    public String getConnectionURL() {
        return connectionURL;
    }
    
    public Properties getProperties() {
        // Make a copy of the properties object, including any non-string values that may be contained in the map.
        Properties newProps = new Properties();
        newProps.putAll(this.properties);
        return newProps;
    }
    
    private void parseURL(String jdbcURL) {
        if (jdbcURL == null) {
            throw new IllegalArgumentException();
        }
        // Trim extra spaces
        jdbcURL = jdbcURL.trim();
        if (jdbcURL.length() == 0) {
            throw new IllegalArgumentException();
        }
        
        Matcher m = urlPattern.matcher(jdbcURL);
        if (!m.matches()) {
        	throw new IllegalArgumentException();
        }
        vdbName = m.group(1);
        connectionURL = m.group(2);
        if (connectionURL != null) {
        	connectionURL = connectionURL.trim();
        }
        String props = m.group(3);
        if (props != null) {
        	parseConnectionProperties(props, this.properties);
        }
    }
    
    public static void parseConnectionProperties(String connectionInfo, Properties p) {
        String[] connectionParts = connectionInfo.split(";"); //$NON-NLS-1$
        if (connectionParts.length != 0) {
            // The rest should be connection params
            for (int i = 0; i < connectionParts.length; i++) {
                parseConnectionProperty(connectionParts[i], p);
            }
        }
    }
    
    static void parseConnectionProperty(String connectionProperty, Properties p) {
        if (connectionProperty.length() == 0) {
            // Be tolerant of double-semicolons and dangling semicolons
            return;
        } else if(connectionProperty.length() < 3) {
            // key=value must have at least 3 characters
            throw new IllegalArgumentException();
        }
        int firstEquals = connectionProperty.indexOf('=');
        if(firstEquals < 1) {
            throw new IllegalArgumentException();
        } 
        String key = connectionProperty.substring(0, firstEquals).trim();
        String value = connectionProperty.substring(firstEquals+1).trim();        
        if(value.indexOf('=') >= 0) {
            throw new IllegalArgumentException();
        }        
        addNormalizedProperty(key, getValidValue(value), p);
    }
    
    public String getJDBCURL() {
        if (urlString == null) {
            StringBuffer buf = new StringBuffer(JDBC_PROTOCOL)
                .append(vdbName);
            	if (this.connectionURL != null) {
            		buf.append('@').append(connectionURL);
            	}
            for (Iterator i = properties.entrySet().iterator(); i.hasNext();) {
                Map.Entry entry = (Map.Entry)i.next();
                if (entry.getValue() instanceof String) {
                    // get only the string properties, because a non-string property could not have been set on the url.
                    buf.append(';')
                       .append(entry.getKey())
                       .append('=')
                       .append(entry.getValue());
                }
            }
            urlString = buf.toString();
        }
        return urlString;
    }
    
    public String getProperty(String key) {
        return properties.getProperty(key);
    }
    
    public String getUserName() {
        return properties.getProperty(BaseDataSource.USER_NAME);
    }
    
    public String getPassword() {
        return properties.getProperty(BaseDataSource.PASSWORD);
    }
    
    public String getVDBVersion() {
        if (properties.contains(BaseDataSource.VDB_VERSION)) {
        	return properties.getProperty(BaseDataSource.VDB_VERSION);
        }
        return properties.getProperty(BaseDataSource.VERSION);
    }
        
    public String getTransactionAutowrapMode() {
        return properties.getProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP);
    }
    
    public String getPartialResultsMode() {
        return properties.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE);
    }
    
    public String getResultSetCacheMode() {
        return properties.getProperty(ExecutionProperties.RESULT_SET_CACHE_MODE);
    }
    
    public String getAnsiQuotedIdentifiers() {
        return properties.getProperty(ExecutionProperties.ANSI_QUOTED_IDENTIFIERS);
    }
    
    public String getFetchSize() {
        return properties.getProperty(ExecutionProperties.PROP_FETCH_SIZE);
    }
    
    public String getXMLFormat() {
        return properties.getProperty(ExecutionProperties.PROP_XML_FORMAT);
    }
    
    public String getXMLValidation() {
        return properties.getProperty(ExecutionProperties.PROP_XML_VALIDATION);
    }
    
    public String getTransparentFailover() {
        return properties.getProperty(TeiidURL.CONNECTION.AUTO_FAILOVER);
    }
    
    public String getDisableLocalTransactions() {
        return properties.getProperty(ExecutionProperties.DISABLE_LOCAL_TRANSACTIONS);
    }
    
    public String toString() {
        return getJDBCURL();
    }
        
    private static void normalizeProperties(Properties source, Properties target) {
        for (Enumeration e = source.propertyNames(); e.hasMoreElements();) {
            String key = (String)e.nextElement();
            addNormalizedProperty(key, source.get(key), target);
        }
    }    
    
    public static void addNormalizedProperty(String key, Object value, Properties target) {
        String validKey = getValidKey(key);
         
        // now add the normalized key and value into the properties object.
        target.put(validKey, value);
    }

    public static String getValidKey(String key) {
    	for (String prop : KNOWN_PROPERTIES) {
    		if (prop.equalsIgnoreCase(key)) {
    			return prop;
    		}
        }
    	return key;
    }
    
    private static Object getValidValue(Object value) {
        if (value instanceof String) {
            try {
                // Decode the value of the property if incase they were encoded.
                return URLDecoder.decode((String)value, UTF_8);
            } catch (UnsupportedEncodingException e) {
                // use the original value
            }            
        }
        return value;
    }
    
    public static Properties normalizeProperties(Properties props) {
        normalizeProperties(props, props);
        return props;
    }

}
