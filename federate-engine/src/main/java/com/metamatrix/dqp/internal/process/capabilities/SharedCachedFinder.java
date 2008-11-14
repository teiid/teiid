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

package com.metamatrix.dqp.internal.process.capabilities;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;

/**
 */
public class SharedCachedFinder implements CapabilitiesFinder {

    private CapabilitiesFinder internalFinder;
    
    // Cache of SourceCapabilities by modelName
    private Map capabilityCache; //synchronized
    private Map userCache = new HashMap(); //unsynchronized
    
    /**
     * Construct a CacheFinder that wraps another finder
     * @param internalFinder Finder to wrap
     * @param sharedCache The shared cache - map of model name to SourceCapabilities
     */
    public SharedCachedFinder(CapabilitiesFinder internalFinder, Map sharedCache) {
        this.internalFinder = internalFinder;
        this.capabilityCache = sharedCache;
    }

    /**
     * Find capabilities used the cache if possible, otherwise do the lookup.
     */
    public SourceCapabilities findCapabilities(String modelName) throws MetaMatrixComponentException {
    	SourceCapabilities caps = (SourceCapabilities) userCache.get(modelName);
        if(caps != null) {
            return caps;
        }
    	caps = (SourceCapabilities) capabilityCache.get(modelName);
        if(caps == null) {
	        // Find capabilities 
	        caps = this.internalFinder.findCapabilities(modelName);
	        if(caps == null) {
	            Object[] params = new Object[] { modelName };
	            throw new MetaMatrixComponentException(DQPPlugin.Util.getString("SharedCacheFinder.Didnt_find_caps", params)); //$NON-NLS-1$
	        }
        }
        if(caps.getScope() == SourceCapabilities.Scope.SCOPE_GLOBAL) {
            capabilityCache.put(modelName, caps);
        }
        userCache.put(modelName, caps);
        return caps;
    }

}
