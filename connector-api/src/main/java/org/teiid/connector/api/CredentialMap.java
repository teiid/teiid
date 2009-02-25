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

package org.teiid.connector.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.connector.DataPlugin;

import com.metamatrix.core.util.StringUtil;

/**
 * Allows credentials to be passed on a per user basis to a connector.  
 *
 * A CredentialsMap object is produced based on information provided in the JDBC
 * URL.  The static method parseCredentials() is used for this purpose.
 *
 * This CredentialMap serves as the session "trusted payload".
 *
 * It is the responsibility of a Connector to call
 * {@link ExecutionContext#getTrustedPayload()} to retrieve the CredentialMap.
 * 
 * The system name should be the same as the Connector Binding Name retrieved from
 * {@link ConnectorEnvironment#getConnectorName()}.
 * 
 * To get the keyword/value pairs use getSystemCredentials(systemName), this will
 * return a Map that contains the properties for the specified system.
 * 
 * Specific user and password values can be retrieved with 
 * getUser(systemName) and getPassword(systemName)
 */
public class CredentialMap implements Serializable {
	//Parsing keywords for system, user, and password.  Comparison is done
	//ignoring case.
	public final static String SYSTEM_KEYWORD = "system"; //$NON-NLS-1$
    public final static String USER_KEYWORD = "user"; //$NON-NLS-1$
    public final static String PASSWORD_KEYWORD = "password"; //$NON-NLS-1$
	public final static String ESCAPE_CHAR = "\\"; //$NON-NLS-1$
    public final static String DEFAULT_SYSTEM = "default"; //$NON-NLS-1$

	private final static String ESCAPE_SLASH = "ESCAPE_SLASH";	    	// forward slash //$NON-NLS-1$
	private final static String ESCAPE_COMMA = "ESCAPE_COMMA";			// comma //$NON-NLS-1$
	private final static String ESCAPE_EQUAL = "ESCAPE_EQUAL";			// equals //$NON-NLS-1$
//	private final static String ESCAPE_SEMI  = "ESCAPE_SEMI";			// semicolon
//	private final static String ESCAPE_CLOSE_PAREN = "ESCAPE_CLOSE_PAREN";	// closing paren


    private final static String[] escape_chars = {
    	                                      ESCAPE_CHAR + "/",    // forward slash //$NON-NLS-1$
                                              ESCAPE_CHAR + ",",	// comma //$NON-NLS-1$
                                              ESCAPE_CHAR + "="};	// equals //$NON-NLS-1$
//                                              ESCAPE_CHAR + ";",	// semicolon
//                                              ESCAPE_CHAR + ")"};	// closing paren

    private final static String[] escape_strings = {
    	                                      ESCAPE_SLASH,    	// forward slash
                                              ESCAPE_COMMA,		// comma
                                              ESCAPE_EQUAL };	    // equals
//                                              ESCAPE_SEMI,		// semicolon
//                                              ESCAPE_CLOSE_PAREN };	// closing paren

    /**
     * In this mode, the CredentialMap will ignore the default credentials
     * and only credentials set for a system will be exposed.  This is the 
     * default setting for the CredentialMap.
     */
    public static final short MODE_IGNORE_DEFAULTS = 0;
    
    /**
     * In this mode, the default credentials will be returned for any system, 
     * overlaid with any system-specific credentials.  If a system is unknown,
     * all default credentials are returned for that system. 
     */
    public static final short MODE_USE_DEFAULTS_GLOBALLY = 1;
    
    /**
     * In this mode, the default credentials will be returned for any system,
     * overlaid with any system-specific credentials.  If a system is unknown,
     * the default credentials are NOT used.  
     */
    public static final short MODE_USE_DEFAULTS_ON_EXISTING = 2;

    /**
     * The map of map of credentials (keyed by system name, upper case).  
     */
    private Map map = new HashMap();
    
    private short defaultCredentialMode = MODE_IGNORE_DEFAULTS;
    private Map defaultCredentials;


	/**
	 * Method to parse a credentials substring extracted from a JDBC URL.  The
	 * presumed command line syntax is ...;credentials=(...);...
	 * Only the substring starting and ending with the parentheses is passed to
	 * this method.  That is, the first non-blank character must be a '(' and the
	 * last non-blank character must be a ')', or an exception will be thrown.
	 *
	 * Syntax is: (credentialspec1/credentialspec2/.../credentialspecn)
	 *
	 * Any number one or greater of credential specifications may be included,
	 * separated by '/' characters.
	 *
	 * Each credentials spec will be specified in the following way:
	 * 	Keyword-specified, order-independent name-value pairs of the form
	 * 		keyword=value.  The only required keyword is "system", which must be specified
	 *  for each credentials spec, and must have a value corresponding to the name of an EIS
	 *  already known to the system.
	 *
	 * 	Ex: system=sys1,user=sys1un,pass=sys1pw, whatever=somevalue.
	 *     Each of the keywords must be unique.
	 *
	 * All blank space is ignored, except within a keyword or value.
	 *
	 * Any syntax error will cause an Exception to be thrown.
	 *
	 * @param inputStr		the string to be parsed;  first non-blank must be a '(', last non-blank must be a ')'
	 * @return             a CredentialMap containing the input
	 * @throws ConnectorException   upon any syntax error;  descriptive text included
	 */
	public static CredentialMap parseCredentials(String inputStr) throws ConnectorException {

		for (int i = 0; i < escape_chars.length; i++) {
			inputStr = StringUtil.replaceAll(inputStr, escape_chars[i], escape_strings[i]);
		}

		if (inputStr == null) {
			throw new ConnectorException(DataPlugin.Util.getString("CredentialMap.Null_input")); //$NON-NLS-1$
		}

		inputStr = inputStr.trim();

		CredentialMap credentialMap = new CredentialMap(); // map of maps keyed on system
		int strLen = inputStr.length();

		//Check that not empty

		if (strLen == 0) {
			throw new ConnectorException(DataPlugin.Util.getString("CredentialMap.Empty_input")); //$NON-NLS-1$
		}

		//Check that first non-blank char is left paren
		if (!inputStr.startsWith("(")|| !inputStr.endsWith(")")) { //$NON-NLS-1$ //$NON-NLS-2$
			throw new ConnectorException(DataPlugin.Util.getString("CredentialMap.Missing_parens")); //$NON-NLS-1$
		}

		// strip of ()'s
		inputStr = inputStr.substring(1, inputStr.length()-1);

		List credentials = StringUtil.getTokens(inputStr, "/"); //$NON-NLS-1$
		Iterator credentialIter = credentials.iterator();

		while (credentialIter.hasNext()) {
			String credential = (String) credentialIter.next();

			// Convert the escaped "/" since we already parsed on the "/"
			credential = StringUtil.replaceAll(credential, escape_strings[0], "/"); //$NON-NLS-1$

			Map newMap = getCredentialMap(credential.trim());
			String system = (String) newMap.get(SYSTEM_KEYWORD);
			if (system == null || system.length() == 0) {
				throw new ConnectorException(DataPlugin.Util.getString("CredentialMap.Missing_system_prop")); //$NON-NLS-1$
			}
			credentialMap.addSystemCredentials(system, newMap); // add to Map of Maps.
		}
		return credentialMap;
	}


	/**
	 * Takes a string containing key/value pairs.
	 *  Example "propName1=propValue1,propName2,propValue2,....."
	 * and returns a map of key/value pairs.
	 */
	private static Map getCredentialMap(String credential) {
		List propList = StringUtil.getTokens(credential, ","); //$NON-NLS-1$
		Iterator propIter = propList.iterator();
		Map map = new HashMap();
		while (propIter.hasNext()) {
			String propVal = (String) propIter.next();
			List pvList = StringUtil.getTokens(propVal, "="); //$NON-NLS-1$
			String key = null;
			String val = null;
			if (pvList.size() > 0) {
				key = (String) pvList.get(0);
				key = key.trim();
			}
			if (pvList.size() > 1) {
				val = (String) pvList.get(1);
				val = val.trim();
				// put back the escaped "," and "=" since we already parsed on these.
				val = StringUtil.replaceAll(val, escape_strings[1], ","); //$NON-NLS-1$
				val = StringUtil.replaceAll(val, escape_strings[2], "="); //$NON-NLS-1$
			}
			map.put(key,val);
		}
		return map;
	}

	public CredentialMap() {
		super();
	}

	/**
	 * Method to return an array of systems that have been added to this
	 * CredentialMap.
	 *
	 * @return  array of the systems that have been added using addSystemCredentials() - always uppercase
	 */
	public String[] getSystems() {
		Set keySet = map.keySet();
		String[] keys = new String[keySet.size()];
		Iterator it = keySet.iterator();
		for (int i = 0; it.hasNext(); i++) {
			keys[i] = (String)it.next();
		}
		return keys;
	}

	/**
	 * Method to add a user name and credentials (e.g. password) for a system
	 *
	 * @param system system name corresponding to the user and credentials
	 * @param credentials Map containing name/val pairs
	 */
	public void addSystemCredentials(String system, Map credentials) {
		map.put(system.toUpperCase(), credentials);
	}

    /**
     * Set the default credentials to use with this credential map.  See the 
     * various default credential modes to understand when and how these will
     * be returned.
     *  
     * @param defaultCredentials Map of credentials
     * @since 4.3
     */
    public void setDefaultCredentials(Map defaultCredentials) {
        this.defaultCredentials = defaultCredentials;
    }
    
    /**
     * Set the default credential mode to determine when default credentials should 
     * be returned.
     *  
     * @param mode The mode
     * @see #MODE_IGNORE_DEFAULTS
     * @see #MODE_USE_DEFAULTS_GLOBALLY
     * @see #MODE_USE_DEFAULTS_ON_EXISTING
     * @since 4.3
     */
    public void setDefaultCredentialMode(short mode) {
        this.defaultCredentialMode = mode;
    }
    
	/**
	 * Method to return the credentials map for a system
	 *
	 * @param systemName	system name
	 * @return 			Map
	 */
	public Map getSystemCredentials(String systemName) {
        Map systemCredentials = (Map)map.get(systemName.toUpperCase());
        
        // If ignoring defaults, return just as is 
        if(this.defaultCredentialMode == MODE_IGNORE_DEFAULTS) {
            return systemCredentials;
        }
        
        // Pre-load the credential set to return with the defaults if
        //  1. defaults exist  
        //  2. AND using defaults globally
        //  3.     OR (using defaults on existing AND system credentials exist) 
        Map workingMap = null;
        if(this.defaultCredentials != null && 
                        (this.defaultCredentialMode == MODE_USE_DEFAULTS_GLOBALLY || 
                                        (this.defaultCredentialMode == MODE_USE_DEFAULTS_ON_EXISTING && 
                                         systemCredentials != null))) {
            
            workingMap = new HashMap();
            workingMap.putAll(defaultCredentials);
        }
        
        // Apply system credentials over the top if they exist
        if(systemCredentials != null) {
            if(workingMap == null) {
                workingMap = new HashMap();
            }
            workingMap.putAll(systemCredentials);
        }
                
		return workingMap;
	}
    
    /**
     * Get the user property for the specified system, if it exists.  The 
     * user property is defined by the static constant {@link #USER_KEYWORD}.  
     *  
     * @param systemName The system to look up (case insensitive)
     * @return The user name for this system if the system was found and the system had a user property 
     * @since 4.3
     */
    public String getUser(String systemName) {
        Map credentials = getSystemCredentials(systemName);
        if(credentials != null) {
            return (String) credentials.get(USER_KEYWORD);
        }
        
        return null;
    }

    /**
     * Get the password property for the specified system, if it exists.  The 
     * password property is defined by the static constant {@link #PASSWORD_KEYWORD}.  
     *  
     * @param systemName The system to look up (case insensitive)
     * @return The password for this system if the system was found and the system had a password property 
     * @since 4.3
     */
    public String getPassword(String systemName) {
        Map credentials = getSystemCredentials(systemName);
        if(credentials != null) {
            return (String) credentials.get(PASSWORD_KEYWORD);
        }
        
        return null;
    }

	public String toString() {
		StringBuffer b = new StringBuffer();
		String[] systems = this.getSystems();
		for (int i=0; i < systems.length; i++) {
			String system = systems[i];
			Map map = this.getSystemCredentials(system);
			b.append("\n"); //$NON-NLS-1$
			b.append(system);
			b.append("\n"); //$NON-NLS-1$
			Iterator iter = map.keySet().iterator();
			while (iter.hasNext()) {
				Object key = iter.next();
				b.append("\t"); //$NON-NLS-1$
				b.append(key);
				b.append("="); //$NON-NLS-1$
				b.append(map.get(key));
				b.append("\n"); //$NON-NLS-1$
			}
		}
		return b.toString();
	}

}//end CredentialMap


