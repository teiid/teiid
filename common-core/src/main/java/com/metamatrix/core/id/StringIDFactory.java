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

package com.metamatrix.core.id;

import com.metamatrix.core.CorePlugin;



public class StringIDFactory implements ObjectIDFactory {
    private long lastID = 0;
    public StringIDFactory() {
    }

    /**
     * Return the description for the type of ObjectID described by this object.
     * @return the description
     */
    public String getDescription() {
        return CorePlugin.Util.getString("StringIDFactory.Description"); //$NON-NLS-1$
    }

    protected long getNextValue() {
        return ++lastID;
    }

    /**
     * Create a new ObjectID instance using this protocol.
     * @return the new instance
     */
    public ObjectID create(){
	    return new StringID( getNextValue() );
    }
    /**
     * Return whether the specified ObjectID instance is valid.  Only ObjectID instances
     * that are for this protocol will be passed in.
     * <p>
     * This implementation only checks whether the ObjectID is an instance of a StringID.
     * @param id the ID that is to be validated, and which is never null
     * @return true if the instance is valid for this protocol, or false if
     * it is not valid.
     */
    public boolean validate(ObjectID id) {
        if ( id instanceof StringID ) {
            return true;
        }
        return false;
    }

    /**
     * Attempt to convert the specified string to the appropriate ObjectID instance.
     * This method is called by the {@link IDGenerator#stringToObject(String)} method, which
     * must process the protocol to determine the correct parser to use.  As such, it guarantees
     * that the parser that receives this call can assume that the protocol was equal to the
     * protocol returned by the parser's {@link ObjectIDDescriptor#getProtocol()}.
     * @param value the stringified id with the protocol and ObjectID.DELIMITER already
     * removed, and should never null or zero length
     * @return the ObjectID instance for the stringified ID if this factory is able
     * to parse the string, or null if the factory is unaware of the specified format.
     * @throws InvalidIDException if the parser is aware of this protocol, but it is of the wrong
     * format for this type of ObjectID.
     */
    public ObjectID stringWithoutProtocolToObject(String value) throws InvalidIDException {
        try {
            return new StringID( value );
        } catch ( NumberFormatException e ) {
            throw new InvalidIDException(CorePlugin.Util.getString("StringIDFactory.The_specified_ID_value_is_invalid",value,getProtocol())); //$NON-NLS-1$
        }
    }

    /**
     * Attempt to convert the specified string to the appropriate ObjectID instance.
     * @param value the stringified id (the result of {@link ObjectID#toString()}),
     * and should never null or zero length
     * @return the ObjectID instance for the stringified ID if this factory is able
     * to parse the string, or null if the factory is unaware of the specified format.
     * @throws InvalidIDException if the parser is aware of this protocol, but it is of the wrong
     * format for this type of ObjectID.
     */
    public ObjectID stringToObject(String value) throws InvalidIDException {
        final ParsedObjectID parsedID = ParsedObjectID.parsedStringifiedObjectID(value,LongID.PROTOCOL);
        try {
            return new StringID( parsedID.getRemainder() );
        } catch ( NumberFormatException e ) {
            throw new InvalidIDException(CorePlugin.Util.getString("StringIDFactory.The_specified_ID_value_is_invalid",value,getProtocol())); //$NON-NLS-1$
        }
    }

    /**
     * Return the name of the protocol that this factory uses.
     * @return the protocol name
     */
    public String getProtocol() {
	    return StringID.PROTOCOL;
    }
}

