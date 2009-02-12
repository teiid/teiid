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


abstract public class ComponentDefnID extends ComponentObjectID  {
	

    /**
     * Create an instance with the specified full name.  The full name must be one or more atomic
     * name components delimited by this class' delimeter character.
     * @param fullName the string form of the full name from which this object is to be created;
     * never null and never zero-length.
     * @throws IllegalArgumentException if the full name is null
     */

    public ComponentDefnID(ConfigurationID configID, String name) {
        this(ComponentDefnID.createName(configID, name));
//        this(name);
        
    }

    protected ComponentDefnID(String fullName) {
        super(fullName);
        
    }

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
        

}





