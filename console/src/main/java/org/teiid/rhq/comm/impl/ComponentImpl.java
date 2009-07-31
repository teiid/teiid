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
package org.teiid.rhq.comm.impl;

import java.util.Map;
import java.util.Properties;

import org.teiid.rhq.comm.Component;
import org.teiid.rhq.comm.VMComponent;

import com.metamatrix.core.util.HashCodeUtil;


/** 
 */
public class ComponentImpl implements VMComponent {
    private String systemKey;
    private String description;
    private String name;
    private String identifier;
    private String version="1.0"; //$NON-NLS-1$
    private String port;
    int hashCode;
   
    
    private Properties compprops = new Properties();
    
    
    public void addProperty(String key, String value) {
    	compprops.put(key, value);
    }
    
    public void setProperties(Properties props) {
    	compprops.putAll(props);
    }
    
    public String getProperty(String key) {
    	return (String)compprops.get(key);
    }
    
    public Map getProperties() {
    	return compprops;
    }
    
    /** 
     * @return Returns the systemKey.
     * @since 1.0
     */
    public String getSystemKey() {
        return this.systemKey;
    }

    
    /** 
     * @param systemKey The systemKey to set.
     * @since 1.0
     */
    public void setSystemKey(String systemKey) {
        this.systemKey = systemKey;
    }

    /** 
     * @see org.teiid.rhq.comm.Component#getDescription()
     * @since 1.0
     */
    public String getDescription() {
        return description;
    }

    /** 
     * @see org.teiid.rhq.comm.Component#getIdentifier()
     * @since 1.0
     */
    public String getIdentifier() {
        return identifier;
    }

    /** 
     * @see org.teiid.rhq.comm.Component#getName()
     * @since 1.0
     */
    public String getName() {
        return name;
    }

    /** 
     * @see org.teiid.rhq.comm.Component#getVersion()
     * @since 1.0
     */
    public String getVersion() {
        return version;
    }
         
    
    /** 
     * @return Returns the port.
     */
    public String getPort() {
        return this.port;
    }


    
    /** 
     * @param port The port to set.
     */
    public void setPort(String port) {
        this.port = port;
    }


    /** 
     * @param description The description to set.
     * @since 1.0
     */
    protected void setDescription(String description) {
        this.description = description;
    }

    
    /** 
     * @param identifier The identifier to set.
     * @since 1.0
     */
    protected void setIdentifier(String identifier) {
        this.identifier = identifier;
        this.hashCode = HashCodeUtil.hashCode(0, this.identifier);
    }

    
    /** 
     * @param name The name to set.
     * @since 1.0
     */
    protected void setName(String name) {
        this.name = name;
    }

    
    /** 
     * @param version The version to set.
     * @since 1.0
     */
    protected void setVersion(String version) {
        this.version = version;
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (obj instanceof Component) {

        	
            // fail fast on different hash codes
            if (this.hashCode() != obj.hashCode()) {
                return false;
            }

            // slower comparison
            Component that = (Component)obj;
            return ( that.getSystemKey().equals(this.getSystemKey()) );
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Compares this object to another. If the specified object is an instance of
     * the same class, then this method compares the name; otherwise, it throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).  Note:  this method is consistent with <code>equals()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return 0;
        }
        if (obj == null ) {
            throw new IllegalArgumentException("Object is null, must be of type " + this.getClass().getName());
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(obj instanceof Component)) {
            throw new IllegalArgumentException(obj.getClass().getName() + " is not of type " + this.getClass().getName());
        }

        // Check if everything else is equal ...
        Component that = (Component)obj;
        int result = that.hashCode() - this.hashCode;
        if ( result != 0 ) return result;
        return this.getIdentifier().compareTo(that.getIdentifier());
    }

    /**
     * Returns a string representing this instance.
     * @return the string representation of this instance.
     */
    public String toString() {
        return this.getIdentifier();
    }

    
    
 
}
