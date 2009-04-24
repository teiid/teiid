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

package com.metamatrix.common.config.xml;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.jdom.Element;

import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.util.ApplicationInfo;
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
    
    public static final Properties getHeaderProperties(Element element) throws InvalidConfigurationElementException{
        Properties props=new Properties();
        
        if (!element.getName().equals(XMLConfig_ElementNames.Header.ELEMENT)) {
            throw new InvalidConfigurationElementException("This is not the header element: " + element.getName() + "."); //$NON-NLS-1$ //$NON-NLS-2$
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
    	XMLHelperImpl xmlHelper = new XMLHelperImpl();

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
        defaultProperties.setProperty(ConfigurationPropertyNames.CONFIGURATION_VERSION, ConfigurationPropertyNames.CONFIG_CURR_VERSION);        
        defaultProperties.setProperty(ConfigurationPropertyNames.SYSTEM_VERSION, ApplicationInfo.getInstance().getMajorReleaseNumber());
        defaultProperties.setProperty(ConfigurationPropertyNames.TIME, DateUtil.getCurrentDateAsString());
       
        
        return defaultProperties;
    }
    
}
