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

package com.metamatrix.platform.admin.api.exception;

import com.metamatrix.platform.security.api.SecurityPlugin;

/**
 * Indicates a product name has been encountered that
 * is unknown, or a subsystem name for a <i>known</i> product has been
 * encountered which is unknown <i>for that product</i>.
 */
public class UnknownProductOrSubsystemException extends MetaMatrixAdminException {

    private static final String unknownProductMsg(String unknownProduct){
        return SecurityPlugin.Util.getString("UnknownProductOrSubsystemException.Product_unknown", unknownProduct); //$NON-NLS-1$
    }

    private static final String unknownSubsystemMsg(String knownProduct, String unknownSubsystem){
        Object[] params = new Object[] {unknownSubsystem, knownProduct};
        return SecurityPlugin.Util.getString("UnknownProductOrSubsystemException.Subsystem_unknown", params); //$NON-NLS-1$
    }
    
    /**
     * No-arg CTOR
     */
    public UnknownProductOrSubsystemException(  ) {
        super(  );
    }    

    /**
     * Constructs an exception indicating a product name is unknown
     * @param unknownProduct name that is not known
     */
    public UnknownProductOrSubsystemException( String unknownProduct ) {
        super( unknownProductMsg(unknownProduct));
    }

    /**
     * Constructs an exception indicating a subsystem for a known product
     * is unknown
     * @param knownProduct product name 
     * @param unknownSubsystem name that is not known for the product
     */
    public UnknownProductOrSubsystemException( String knownProduct, String unknownSubsystem ) {
        super( unknownSubsystemMsg(knownProduct, unknownSubsystem));
    }
}
