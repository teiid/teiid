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

package com.metamatrix.common.config.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductServiceConfigID;
import com.metamatrix.common.config.api.ProductTypeID;
import com.metamatrix.common.config.api.ServiceComponentDefnID;


public class BasicProductServiceConfig extends BasicComponentDefn implements ProductServiceConfig, Serializable {

	// key - ServiceComponentDefnID
	// value = Boolean indicating if the service enabled
	private HashMap serviceComponentDefnIDs;


    public BasicProductServiceConfig(ConfigurationID configurationID, ProductServiceConfigID componentID, ProductTypeID productTypeID) {
        super(configurationID, componentID, productTypeID);
        serviceComponentDefnIDs = new HashMap();

    }

    protected BasicProductServiceConfig(BasicProductServiceConfig psc) {
        super(psc);
        this.serviceComponentDefnIDs = new HashMap(psc.serviceComponentDefnIDs);
    }
    
    public boolean isServiceEnabled(ServiceComponentDefnID serviceID) {
    	if (serviceComponentDefnIDs.containsKey(serviceID)) {
    		Boolean b = (Boolean) serviceComponentDefnIDs.get(serviceID);
    		return b.booleanValue();
    	}
    	
		return false;		    	
    	    	
    }

	public void setServiceEnabled(ServiceComponentDefnID serviceID, boolean isEnabled) {
		if (serviceComponentDefnIDs.containsKey(serviceID)) {
    		serviceComponentDefnIDs.put(serviceID, Boolean.valueOf(isEnabled));
		}			
	}
	
	public boolean containsService(ServiceComponentDefnID serviceID) {
		if (serviceComponentDefnIDs.containsKey(serviceID)) {
			return true;
		}
		return false;		
	}
    
         
	
    /**
     * Returns a cloned Collection of ServiceComponentDefnID objects, which
     * represent the service component definitions that are contained
     * by this product service configuration.
     */
    public Collection getServiceComponentDefnIDs(){
        return new HashSet(this.serviceComponentDefnIDs.keySet());
    }

    /**
     * This method is used to reset the assigned services in a PSC.  
     * When the PSC is being updated with a new list, this enables
     * the map to be cleared prior to adding the ones that are
     * suppose to be in the map, saving the processing of having 
     * to compare which ones don't belong.
     */
    void resetServices() { 
        this.serviceComponentDefnIDs.clear();
    }


    /**
     * Package-level method used by {@link BasicConfigurationObjectEditor}
     * to alter this instance
     */
    void addServiceComponentDefnID(ServiceComponentDefnID serviceDefnID){
        this.serviceComponentDefnIDs.put(serviceDefnID, Boolean.TRUE);
    }

    /**
     * Package-level method used by {@link BasicConfigurationObjectEditor}
     * to alter this instance
     */
    void removeServiceComponentDefnID(ServiceComponentDefnID serviceDefnID){
        this.serviceComponentDefnIDs.remove(serviceDefnID);
    }

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     *  this method.
     *  @return the object that is the clone of this instance.
     *  @throws CloneNotSupportedException if this object cannot be cloned
     */
    public synchronized Object clone() throws CloneNotSupportedException {
    	 return new BasicProductServiceConfig(this);
    }
    
       
}

