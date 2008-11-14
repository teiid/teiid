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

package com.metamatrix.common.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.api.exception.ComponentNotFoundException;

import com.metamatrix.common.config.api.ReleaseInfo;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.core.util.MetaMatrixProductVersion;

/**
 * Utility needed by {@link CurrentConfiguration} and by
 * the Configuration Service.  Assumes logging is available to be
 * used.
 */
public final class ProductReleaseInfoUtil {

    private static final String PLATFORM_PROJECT = "platform"; //$NON-NLS-1$
    private static final String SERVER_PROJECT = "server"; //$NON-NLS-1$
    private static final String METADATA_PROJECT = "metadata"; //$NON-NLS-1$
    private static final String CONNECTORS_PROJECT = "connector"; //$NON-NLS-1$

    //The metamatrix-complete contains the products from above
	private static final String METAMATRIX_COMPLETE = "metamatrix-server.jar"; //$NON-NLS-1$
    private static final Map NAMES = new HashMap();
    static{
        NAMES.put(PLATFORM_PROJECT, MetaMatrixProductVersion.PLATFORM_TYPE_NAME);
        NAMES.put(SERVER_PROJECT, MetaMatrixProductVersion.METAMATRIX_SERVER_TYPE_NAME);
        NAMES.put(METADATA_PROJECT, MetaMatrixProductVersion.METADATA_SERVER_TYPE_NAME);
        NAMES.put(CONNECTORS_PROJECT, MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME);
    }

    private static Collection RELEASE_INFOS;

    /**
     * Obtain the Collection of {@link com.metamatrix.common.config.api.ReleaseInfo} objects
     * which represent the installed products of the system.  Each ReleaseInfo contains
     * the name of the product, as well as release info.
     * @return Collection of {@link com.metamatrix.common.config.api.ReleaseInfo}
     * objects of licensed, installed products.
     * @throws ConfigurationException if an error occurred while loading the
     * release information for mandatory products (currently: Platform) from external files.
     * @throws IllegalStateException
     */
    public static synchronized Collection getProductReleaseInfos() throws ConfigurationException{
        if (RELEASE_INFOS == null){
            RELEASE_INFOS =  new HashSet();
//            Collection components = null;
            ApplicationInfo info = ApplicationInfo.getInstance();
            //load Platform project (mandatory)
			if (!info.isUnmodifiable()) {
	            try {
    	            info.addComponent(METAMATRIX_COMPLETE);
        	    } catch ( ComponentNotFoundException e ) {
            	    throw new ConfigurationException(e,"Could not load product release information for product " + PLATFORM_PROJECT); //$NON-NLS-1$
	            }


	            try{
    	        	info.setMainComponent(METAMATRIX_COMPLETE);
        	    } catch(ComponentNotFoundException c){
	            	throw new ConfigurationException(c.getMessage());
    	        }
	            info.markUnmodifiable();
			}

			ApplicationInfo.Component component = info.getMainComponent();
            String version      = component.getReleaseNumber();
            String date         = component.getBuildDate();
            String build        = component.getBuildNumber();

            ReleaseInfo releaseInfo = null;


            Iterator i = NAMES.keySet().iterator();
            while(i.hasNext()){
            	Object key = i.next();
            	String name = (String)NAMES.get(key);
                releaseInfo = new ReleaseInfo(name, version, date, build);
                RELEASE_INFOS.add( releaseInfo);
            }
        }
        return RELEASE_INFOS;
    }

}
