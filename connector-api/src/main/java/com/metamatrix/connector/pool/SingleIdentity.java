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

/*
 */
package com.metamatrix.connector.pool;

import com.metamatrix.connector.api.ExecutionContext;

/**
 * This class represents a single ConnectorIdentity. All the connections are treated the same.
 */
public class SingleIdentity implements ConnectorIdentity {
    private ExecutionContext context;

    /**
     * Construct with a context
     * @param context The context
     */
    public SingleIdentity(ExecutionContext context){
        this.context = context;
    }
    
    /**
     * Get the original context
     * @return The original context
     */
    public ExecutionContext getSecurityContext() {
        return this.context;
    }

    /**
     * Return true for everything - all identities are identical.
     */
    public boolean equals(Object obj){
        return obj instanceof SingleIdentity;
    }
    
    public String toString(){
        if (context != null) {
            return "SingleIdentity: atomic-request="+this.context.getRequestIdentifier()+"."+this.context.getPartIdentifier()+"."+this.context.getExecutionCountIdentifier(); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        return "SingleIdentity"; //$NON-NLS-1$
    }    
    
    public int hashCode(){
        return 0; 
    }
}
