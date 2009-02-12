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

public class VMComponentDefnID extends ComponentDefnID {
	
    /**
     * The name of the built-in, standard process that will run basic metmatrix services
     */
    public static final String STANDARD_METAMATRIX_PROCESS = "MMProcess"; //$NON-NLS-1$


    public VMComponentDefnID(ConfigurationID configID, HostID hostID, String name) {
        super(createFullName(configID, hostID, name));
        
    }
    public VMComponentDefnID(String fullName) {
        super(fullName);
    }
    
    private static final String createFullName(ConfigurationID configID, HostID hostID, String name) {
        
            Assertion.isNotNull(configID);
            Assertion.isNotNull(name);
            Assertion.isNotNull(hostID);
 

            StringBuffer sb = new StringBuffer(configID.getName());
            sb.append(IDVerifier.DELIMITER_CHARACTER);
            sb.append(hostID.getFullName());
            sb.append(IDVerifier.DELIMITER_CHARACTER);
            sb.append(name);

            return sb.toString();
     
    }

}

