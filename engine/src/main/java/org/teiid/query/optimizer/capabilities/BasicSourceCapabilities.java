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

package org.teiid.query.optimizer.capabilities;

import java.io.Serializable;
import java.util.*;

/**
 */
public class BasicSourceCapabilities implements SourceCapabilities, Serializable {

	private static final long serialVersionUID = -1779069588746365579L;
	
	private Scope scope = Scope.SCOPE_GLOBAL;
    private Map<Capability, Boolean> capabilityMap = new HashMap<Capability, Boolean>();
    private Map<String, Boolean> functionMap = new TreeMap<String, Boolean>(String.CASE_INSENSITIVE_ORDER);
    private Map<Capability, Object> propertyMap = new HashMap<Capability, Object>();

    /**
     * Construct a basic capabilities object.
     */
    public BasicSourceCapabilities() {
    }

    public boolean supportsCapability(Capability capability) {
        Boolean supports = capabilityMap.get(capability);
        return (supports == null) ? false : supports.booleanValue();
    }

    public boolean supportsFunction(String functionName) {
        Boolean supports = functionMap.get(functionName);
        return (supports == null) ? false : supports.booleanValue();
    }
    
    public void setCapabilitySupport(Capability capability, boolean supports) {
    	if (supports && capability == Capability.QUERY_AGGREGATES) {
    		capabilityMap.put(Capability.QUERY_GROUP_BY, true);
    		capabilityMap.put(Capability.QUERY_HAVING, true);
    	} else {
    		capabilityMap.put(capability, supports);
    	}
    } 

    public void setFunctionSupport(String function, boolean supports) {        
        functionMap.put(function, Boolean.valueOf(supports));
    }

    public Scope getScope() {
        return this.scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }
    
    public String toString() {
        return "BasicSourceCapabilities<"+scope+", caps=" + capabilityMap + ", funcs=" + functionMap + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
    
    /**
     * This method adds the Source Property to the Property Map
     * @param propertyName
     * @param value
     * @since 4.4
     */
    public void setSourceProperty(Capability propertyName, Object value) {
        this.propertyMap.put(propertyName, value);
    }

    /** 
     * @see org.teiid.query.optimizer.capabilities.SourceCapabilities#getSourceProperty(java.lang.String)
     * @since 4.2
     */
    public Object getSourceProperty(Capability propertyName) {
        return this.propertyMap.get(propertyName);
    }
    
}
