/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.id;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.id.*;


public class TransactionIDFactory extends LongIDFactory {
    public TransactionIDFactory() {
    }
    /**
     * Return the name of the protocol that this factory uses.
     * @return the protocol name
     */
    public String getProtocol() {
	    return TransactionID.PROTOCOL;
    }
    /**
     * Attempt to convert the specified string to the appropriate ObjectID instance.
     * @param value the stringified id with the protocol and ObjectID.DELIMITER already
     * removed, and which is never null or zero length
     * @return the ObjectID instance for the stringified ID if this factory is able
     * to parse the string, or null if the factory is unaware of the specified format.
     */
    public ObjectID stringToObject(String value) throws InvalidIDException {
        try {
	        return new TransactionID( Long.parseLong(value) );
        } catch ( NumberFormatException e ) {
           throw new InvalidIDException(CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0010,
            	new Object[] {value, getProtocol()}));

        }
    }
    /**
     * Return whether the specified ObjectID instance is valid.  Only ObjectID instances
     * that are for this protocol will be passed in.
     * <p>
     * This implementation only checks whether the ObjectID is an instance of a LongID.
     * @param id the ID that is to be validated, and which is never null
     * @return true if the instance is valid for this protocol, or false if
     * it is not valid.
     */
    public boolean validate(ObjectID id) {
        if ( id instanceof TransactionID ) {
            return true;
        }
        return false;
    }
    /**
     * Create a new ObjectID instance using this protocol.
     * @return the new instance
     */
    public ObjectID create(){
	    return new TransactionID(this.getNextValue());
    }
}

