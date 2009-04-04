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

package com.metamatrix.toolbox.ui.widget.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

import com.metamatrix.common.properties.TextManager;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * PropertyLoader is a class for loading and working with property files, especially those containing
 * repeating branch structures.
 */
public class PropertyLoader {

    public static final String PROPERTY_PREFIX = "PROPERTY_PREFIX";
    private static final String PROPERTY_DELIM = ".";

    private Properties properties = null;
    private String propertyFile = null;

    /**
     * Constructor - specify the file (with path) for this object to load.  The file will not be
     * accessed until either getProperties() or getPropertyBranch() is called.
     *
     * @param propertiesFile property file name.  May be either in a jar or on the file system.
     * PropertyLoader attempts to find the file as a jar resource first, then on the file system.
     */
    public PropertyLoader(String propertiesFile) {
        this.propertyFile = propertiesFile;
    }

    /**
     * Constructor - set the properties object that this object should operate on.
     *
     * @param props object
     */
    public PropertyLoader(Properties props) {
        this.properties = props;
    }

    /**
     * return a Properties object containing all property pairs loaded into this object.
     * @return all properties that this object has loaded.  Will not return null.
     * @throws PropertyLoaderException if the specified file name for the properties could not be found.
     */
    public Properties getProperties() throws PropertyLoaderException {

        // Check to see if we have already read in the properties.
        // If yes then just return cached properties.
        if (properties != null) {
              return properties;
        }

        properties = new Properties();

        // first, try to load them out of a jar
        InputStream propStream = getClass().getClassLoader().getResourceAsStream(propertyFile);

        if ( propStream == null ) {
            try {
                // next, find the property file on the file system
                propStream = new FileInputStream(propertyFile);
            } catch (FileNotFoundException ex) {
                throw new PropertyLoaderException(TextManager.INSTANCE.getText(ErrorMessageKeys.CM_UTIL_ERR_0032, propertyFile));
            }
        }

        try {
            // Load props from the property file stream
            if (propStream != null) {
                properties.load(propStream);
                propStream.close();
            }
        } catch (IOException e) {
            throw new PropertyLoaderException(TextManager.INSTANCE.getText(ErrorMessageKeys.CM_UTIL_ERR_0032, propertyFile));
        }

        return properties;
    }

    /**
     * return a Properties object containing all property pairs beneath the specified prefix.
     * @return all properties beneath the specified node, plus an additional property named PROPERTY_PREFIX
     * with the value of the specified prefix.  Will not return null.
     * @throws PropertyLoaderException if the specified file name for the properties could not be found.
     */
    public Properties getPropertiesBranch(String prefix) throws PropertyLoaderException {

        // load the properties
        Properties props = getProperties();

        // create the branch Properties object and load in the prefix property and value
        Properties branch = new Properties();
        if ( prefix.endsWith(PROPERTY_DELIM) ) {
            branch.put(PROPERTY_PREFIX, prefix.substring(0,prefix.length()-1));  // strip off the '.'
        } else {
            branch.put(PROPERTY_PREFIX, prefix);
        }

        // Loop through all of the properties, loading any that begin with the prefix into a new Properties collection
        Iterator iter = props.keySet().iterator();
        while(iter.hasNext()) {
            String key = (String) iter.next();
            if (key.startsWith(prefix)) {
                Object val = props.get(key);
                if(val != null && !((String)val).equals("")) {
                    int indx = prefix.length();
                    String newKey = key.substring(indx);
                    if ( newKey.startsWith(PROPERTY_DELIM) ) {
                        newKey = newKey.substring(1);  // strip off the '.'
                    }
                    branch.put(newKey,val);
                }
            }
        }
        return branch;
    }


}
