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

import java.util.ArrayList;
import java.util.List;

public class HostID extends ComponentDefnID {


    public HostID(String hostName)  {
        super(hostName);
    }

    // NOTE: The following methods override the base class to allow support for .'s in the host name.
    // The hostname is treated as an atomic name regardless if it contains periods or not.
    /**
     * @return the name for this identifier.
     */
    public String getName() {
        return this.getFullName();
    }

    public List getNameComponents() {
       this.atomicNames = new ArrayList(1);
       this.atomicNames.add(this.fullName);
       return this.atomicNames;
    }

    public String getParentFullName() {
        return null;
    }

    public final boolean hasParent() {
        return false;
    }
}

