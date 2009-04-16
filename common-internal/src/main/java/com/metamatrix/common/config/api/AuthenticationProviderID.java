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

package com.metamatrix.common.config.api;

import com.metamatrix.common.namedobject.IDVerifier;
import com.metamatrix.core.util.Assertion;


public class AuthenticationProviderID extends ComponentDefnID {

    public AuthenticationProviderID(ConfigurationID configID, String name) {
 		super(name);

    }
    protected AuthenticationProviderID(String fullName) {
        super(fullName);
    }
    
//    public AuthenticationProviderID(String name, ProductServiceConfigID pscID) {
//        super(AuthenticationProviderID.createName(Configuration.NEXT_STARTUP_ID, name, pscID));
//    } 
    
    
    /**
    * Responsible for creating the structuring id for this VM Component
    */
    
//    private static final String createName(ConfigurationID configID, String name, ProductServiceConfigID pscID) {
//		Assertion.isNotNull(configID);
//		Assertion.isNotNull(name);
//       
//        
//        StringBuffer sb = new StringBuffer();
//        sb.append(pscID.getName());
//        sb.append(IDVerifier.DELIMITER_CHARACTER);        
//        sb.append(name);
//
//        return sb.toString();
//
//  }
      


    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     * @throws CloneNotSupportedException if this object cannot be cloned (i.e., only objects in
     * {@link com.metamatrix.metadata.api.Defaults Defaults} cannot be cloned).
     */
    public Object clone() throws CloneNotSupportedException{
	      return super.clone();
    }
    /**
     * Return the full name of the parent.  This is a convenience method to return
     * the list of atomic name components that excludes this ID's last atomic name component.
     * @return the full name of the parent, or null if this ID has no parent.
     */
// Note - this is overridden so when the parent name is asked for, it still returns
// the same previous value of the parent configuration.  It should not include
// the psc name

    public String getParentFullName() {
    	return ""; //$NON-NLS-1$
    }
}


