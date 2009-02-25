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

package org.teiid.connector.api;


/**
 * This class represents a ConnectorIdentity keyed on a username with
 * a mapped identity
 */
public class MappedUserIdentity implements ConnectorIdentity {
	private String username;
	private String mappedUser;
	private String password;
    
    /**
     * Construct with a security context
     * @param context The context
     */
    public MappedUserIdentity(String username, String mappedUser, String password){
        this.username = username;
        this.mappedUser = mappedUser;
        this.password = password;
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

        if (obj instanceof MappedUserIdentity) {
            MappedUserIdentity that = (MappedUserIdentity)obj;
            return username.equals(that.username);
        }
        
        return false;        
    }
    
    /**
     * Get hash code, based on user name
     */
    public int hashCode(){
        return username.hashCode();
    }    
    
    public String toString(){
        return "UserIdentity " + username; //$NON-NLS-1$
    }  
    
    public String getMappedUser() {
		return mappedUser;
	}
    
    public String getPassword() {
		return password;
	}
}
