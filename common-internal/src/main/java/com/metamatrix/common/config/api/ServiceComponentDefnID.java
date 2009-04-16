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

public class ServiceComponentDefnID extends ComponentDefnID {

    public ServiceComponentDefnID(ConfigurationID configID, String name) {
        super(name);

    }
    protected ServiceComponentDefnID(String fullName) {
        super(fullName);
    }
    
//    public ServiceComponentDefnID(ConfigurationID configID, String name) {
// 		super(ServiceComponentDefnID.createName(configID, name));
//    } 
    
    
    /**
    * Responsible for creating the structuring id for this VM Component
    */
    private static final String createName(ConfigurationID configID, String name) {
		Assertion.isNotNull(configID);
		Assertion.isNotNull(name);

        StringBuffer sb = new StringBuffer(configID.getName());
        sb.append(IDVerifier.DELIMITER_CHARACTER);        
        sb.append(name);

        return sb.toString();

    }
      


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
}


