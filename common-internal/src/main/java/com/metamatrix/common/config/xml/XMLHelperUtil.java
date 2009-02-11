/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.config.xml;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.jdom.Element;

import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.util.MetaMatrixProductNames;
import com.metamatrix.core.util.DateUtil;


/** 
 * @since 4.1
 */
public class XMLHelperUtil {
    
    /* These static variables define the constants that will be used to
    * create the header for every document that is produced using this concrete
    * utility.
    */
    static final String DEFAULT_USER_CREATED_BY = "Unknown"; //$NON-NLS-1$
    
    
    static final String APPLICATION_CREATED_BY = "ApplicationCreatedBy"; //$NON-NLS-1$
    static final String APPLICATION_VERSION_CREATED_BY = "ApplicationVersion"; //$NON-NLS-1$
    static final String USER_CREATED_BY = "UserCreatedBy"; //$NON-NLS-1$
    static final String CONFIGURATION_VERSION = "ConfigurationVersion"; //$NON-NLS-1$
    static final String METAMATRIX_SYSTEM_VERSION = "MetaMatrixSystemVersion"; //$NON-NLS-1$
    static final String TIME = "Time"; //$NON-NLS-1$

    // at 4.2 is where the configuration format changes, so anything prior
    // to this version will use the old (3.0) import/export utility
    static final String MM_CONFIG_4_2_VERSION = "4.2"; //$NON-NLS-1$
    static final String MM_CONFIG_3_0_VERSION = "3.0"; //$NON-NLS-1$
    
    static final double MM_LATEST_CONFIG_VERSION = 4.2;
    

    public static final boolean is42ConfigurationCompatible(Element root) throws InvalidConfigurationElementException{
        Element headerElement = root.getChild(XMLElementNames.Header.ELEMENT);
        if (headerElement == null) {
        // If no header element found, assume it's pre vers 4.2
            return false;
        }
        
        Properties props = getHeaderProperties(headerElement);
        
        return is42ConfigurationCompatible(props);
        
    }
    
    public static final boolean is42ConfigurationCompatible(Properties props) throws InvalidConfigurationElementException{
        
        String sVersion = props.getProperty(XMLElementNames.Header.ConfigurationVersion.ELEMENT);
        
        if (sVersion == null) {
            return false;
        }
        try {
            double sv = Double.parseDouble(sVersion);
            if (sv >= MM_LATEST_CONFIG_VERSION) {
                return true;
            } 
                return false;

        } catch (Throwable t) {
            return false;
        }

        
    }
    
    public static final Properties getHeaderProperties(Element element) throws InvalidConfigurationElementException{
        Properties props=new Properties();
        
        if (!element.getName().equals(XMLElementNames.Header.ELEMENT)) {
            throw new InvalidConfigurationElementException("This is not the header element: " + element.getName() + ".", element); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        List elements = element.getChildren();
        Iterator it = elements.iterator();
        while(it.hasNext()) {
            final Element e = (Element) it.next();
            props.setProperty(e.getName(), e.getText());
        }
        return props;
    }

    
    public static final Element addHeaderElement(Element root, Properties properties) {
        XMLHelper xmlHelper = new XMLConfig_42_HelperImpl();

        root.addContent(xmlHelper.createHeaderElement(createHeaderProperties(properties)));

        return root;
        
    }
    
    protected static Properties createHeaderProperties(Properties props) {
        Properties defaultProperties = new Properties();
        defaultProperties.setProperty(ConfigurationPropertyNames.USER_CREATED_BY, DEFAULT_USER_CREATED_BY);
        
        // the properties passed in by the user override those put in by this
        // method.
        if (props!=null) {
            defaultProperties.putAll(props);
        }
        defaultProperties.setProperty(ConfigurationPropertyNames.CONFIGURATION_VERSION, ConfigurationPropertyNames.MM_CONFIG_4_2_VERSION);        
        defaultProperties.setProperty(ConfigurationPropertyNames.METAMATRIX_SYSTEM_VERSION, MetaMatrixProductNames.VERSION_NUMBER);
        defaultProperties.setProperty(ConfigurationPropertyNames.TIME, DateUtil.getCurrentDateAsString());
       
        
        return defaultProperties;
    }
    
}
