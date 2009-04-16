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

package com.metamatrix.platform.admin.api.runtime;

import java.io.Serializable;

/**
 * This class is a container for VMRegistryBinding objects for a specific host.
 * Created and maintained by the Registry.
 */
public class ComponentData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1276023440163989298L;

	// Name that this object represents.
    private String name;

    /** indicates if component is in operational configuration */
    protected boolean deployed;

    /** indicated if component exists in registry */
    protected boolean registered;

    protected int hashCode;

    /**
     * Construct an instance for the given name
     *
     * @param Name
     */
    public ComponentData(String name, boolean deployed, boolean registered) {
        this.name = name;
        this.deployed = deployed;
        this.registered = registered;
    }

    /**
     * Return name
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    public boolean isDeployed() {
        return deployed;
    }

    public boolean isRegistered() {
        return registered;
    }

    public String toString() {
        return name;
    }

    /**
     * Returns the hash code value for this object.
     *  @return a hash code value for this object.
     */
    public final int hashCode() {
        return this.hashCode;
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {

        // Check if instances are identical ...
        if ( this == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        //if ( this.getClass().isInstance(obj) ) {
        if ( obj instanceof ComponentData ) {

        	// Do quick hash code check first
        	if( this.hashCode() != obj.hashCode() ) {
        		return false;
		    }

            // If the types aren't the same, then fail
            ComponentData that = (ComponentData) obj;
            if ( this.getClass() != that.getClass() ) {
                return false;
            }
            return this.name.equalsIgnoreCase( that.getName() );
        }

        // Otherwise not comparable ...
        return false;
    }
}

