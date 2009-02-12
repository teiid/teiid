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

/**
 * <p>This class is a factory for generating universally unique identifiers
 * (UUID's). </p>
 *
 * <h3>Output format for UUIDs</h3>
 * <p>UUIDs are output in the following 36-character format:
 * <pre>
 *    xxxxxxxx-yyyy-zzzz-cccc-nnnnnnnnnnnn
 * </pre>
 * where x=least significant time component, y=middle significant time component,
 * z=most significant time component multiplexed with version, c=clock sequence
 * multiplexed with variant, and n=node component (random number).
 * </p>
 *
 * <p>The generated ID's conform somewhat to the (now expired) IETF internet
 * draft standard, "UUIDs and GUIDs", DCE spec on UUIDs. </p>
 *
 * <ul>
 *   <li>
 *   <a href="http://hegel.ittc.ukans.edu/topics/internet/internet-drafts/draft-l/draft-leach-uuids-guids-01.txt">
 *      UUIDs and GUIDs, P. Leach, R. Salz, 02/05/1998</a>
 *   </li>
 *   <li>
 *   <a href="http://www.opengroup.org/onlinepubs/009629399/apdxa.htm">
 *      DCE Universal Unique Identifier</a>.
 *   </li>
 * </ul></p>
 *
 * <p>All references in this code to bit positions as "least significant" and
 * "most significant" refer to the bits moving from right to left, respectively.
 * </p>
 */
public class UUIDFactory implements ObjectIDFactory {
    
    // -------------------------------------------------------------------------
    //                           C O N S T R U C T O R
    // -------------------------------------------------------------------------
    
    public UUIDFactory() {
    }
    
    // -------------------------------------------------------------------------
    //                       P U B L I C     M E T H O D S
    // -------------------------------------------------------------------------
    
    /**
     * Return the description for the type of ObjectID described by this object.
     * @return the description
     */
    public String getDescription() {
        return CorePlugin.Util.getString("UUIDFactory.Description"); //$NON-NLS-1$
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
        return UUID.stringToObject(value);
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
        final ParsedObjectID parsedID = ParsedObjectID.parsedStringifiedObjectID(value,UUID.PROTOCOL);
        return UUID.stringToObject(parsedID.getRemainder());
    }

    /**
     * Return the name of the protocol that this factory uses.
     * @return the protocol name
     */
    public String getProtocol() {
	    return UUID.PROTOCOL;
    }

    // -------------------------------------------------------------------------
    //                   G E N E R A T I O N    M E T H O D S
    // -------------------------------------------------------------------------
    
    /**
     * <p>Create a new ObjectID instance using this protocol. </p>
     * @return Universally unique ID (UUID)
     */
    public ObjectID create() {
        return new UUID(java.util.UUID.randomUUID());
    }

}

