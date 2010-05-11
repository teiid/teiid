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

package org.teiid.dqp.internal.process.multisource;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.teiid.core.TeiidComponentException;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;



/** 
 * A capabilities finder proxy that intercepts capabilities calls for multi-source models
 * and hard-codes some capabilities for certain functionality that will not be planned 
 * correctly for multi-source queries otherwise.
 * @since 4.2
 */
public class MultiSourceCapabilitiesFinder implements
                                          CapabilitiesFinder {

    private CapabilitiesFinder finder;
    private Collection multiSourceModels;
    
    public MultiSourceCapabilitiesFinder(CapabilitiesFinder finder, Collection multiSourceModels) {
        this.finder = finder;
        this.multiSourceModels = multiSourceModels;
    }
    
    public SourceCapabilities findCapabilities(String modelName) throws TeiidComponentException {
        SourceCapabilities caps = finder.findCapabilities(modelName);
        
        if(multiSourceModels.contains(modelName)) {
            caps = modifyCapabilities(caps);
        }
        
        return caps;
    }
    
    /**
     * Wrapper the existing capabilities to modify a few capabilities 
     * @param caps Real source capabilities
     * @return Modified capabilities to work correctly when planning with multi-source models
     * @since 4.2
     */
    SourceCapabilities modifyCapabilities(SourceCapabilities caps) {
        // Create a dynamic proxy that intercepts just a couple method calls
        InvocationHandler handler = new MultiSourceHandler(caps);
        return (SourceCapabilities) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] { SourceCapabilities.class }, handler);
    }
    
    private static class MultiSourceHandler implements InvocationHandler {
        
        private SourceCapabilities caps;

        public MultiSourceHandler(SourceCapabilities caps) {
            this.caps = caps;
        }
        
        private static final String CAPABILITY_METHOD = "supportsCapability"; //$NON-NLS-1$
        private static final Set<Capability> DISALLOWED_CAPABILITIES = new HashSet<Capability>(Arrays.asList(
            Capability.QUERY_UNION,
            Capability.QUERY_EXCEPT,
            Capability.QUERY_INTERSECT,
            Capability.QUERY_AGGREGATES_AVG,
            Capability.QUERY_AGGREGATES_COUNT,
            Capability.QUERY_AGGREGATES_COUNT_STAR,
            Capability.QUERY_AGGREGATES_DISTINCT,
            Capability.QUERY_AGGREGATES_MAX,
            Capability.QUERY_AGGREGATES_MIN,
            Capability.QUERY_AGGREGATES_SUM,
            Capability.QUERY_GROUP_BY,
            Capability.QUERY_HAVING,
            Capability.QUERY_ORDERBY, 
            Capability.QUERY_SELECT_DISTINCT,
            Capability.ROW_LIMIT,
            Capability.ROW_OFFSET
        ));

        /** 
         * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
         * @since 4.2
         */
        public Object invoke(Object proxy,
                             Method method,
                             Object[] args) throws Throwable {
            
            String methodName = method.getName();

            if(methodName.equals(CAPABILITY_METHOD)) {
                Capability capability = (Capability) args[0];
                if(DISALLOWED_CAPABILITIES.contains(capability)) {
                    return Boolean.FALSE;
                }
            }
            
            return method.invoke(this.caps, args);
                        
        }
    }
    
}
