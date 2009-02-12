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

package com.metamatrix.connector.pool;

import com.metamatrix.connector.api.ExecutionContext;

/**
 * This class represents a ConnectorIdentity based on user name. Connections are 
 * pooled based on user name.
 */
public class UserIdentity implements ConnectorIdentity {
    private ExecutionContext context;
    
    /**
     * Construct with a security context
     * @param context The context
     */
    public UserIdentity(ExecutionContext context){
        this.context = context;
    }    
    
    /*
     * @see com.metamatrix.data.pool.ConnectorIdentity#getSecurityContext()
     */
    public ExecutionContext getSecurityContext() {
        return this.context;
    }

    /**
     * Implement equals based on the case-insensitive user name.
     * @param obj Other identity object
     * @return True if other object is a UserIdentity with the same user name
     */
    public boolean equals(Object obj){
        if (this == obj) {
            return true;
        }

        if (obj instanceof UserIdentity) {
            UserIdentity that = (UserIdentity)obj;
            return this.context.getUser().toUpperCase().equals(that.context.getUser().toUpperCase());
        }
        
        return false;        
    }
    
    /**
     * Get hash code, based on user name
     */
    public int hashCode(){
        return context.getUser().toUpperCase().hashCode();
    }    
    
    public String toString(){
        return "UserIdentity " + context.getUser(); //$NON-NLS-1$
    }  
}
