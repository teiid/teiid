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
import java.util.Iterator;
import java.util.Set;

import com.metamatrix.core.CorePlugin;

/**
 * ParsedObjectID
 */
public class ParsedObjectID {
    private static final int DELIMITER_LENGTH = 1;
    private final String protocol;
    private final String remainder;
    
    protected ParsedObjectID( final String protocol, final String remainder ) {
        this.protocol = protocol;
        this.remainder = remainder;
    } 
    
    public String getProtocol() {
        return protocol;
    }
    
    public String getRemainder() {
        return remainder;
    }
    
    public static ParsedObjectID parsedStringifiedObjectID( final String id, final Set protocols ) throws InvalidIDException {
        final Iterator iter = protocols.iterator();
        while (iter.hasNext()) {
            final String protocol = (String)iter.next();
            if ( id.startsWith(protocol) ) {
                return new ParsedObjectID(protocol,id.substring(protocol.length()+DELIMITER_LENGTH));
            }
        }
        throw new InvalidIDException(CorePlugin.Util.getString("ParsedObjectID.The_stringified_ObjectID_does_not_have_a_protocol")); //$NON-NLS-1$
    }

    public static ParsedObjectID parsedStringifiedObjectID( final String id ) throws InvalidIDException {
        return parsedStringifiedObjectID(id,ObjectID.DELIMITER);
    }

    public static ParsedObjectID parsedStringifiedObjectID( final String id, final char delim ) throws InvalidIDException {
        int index = -1;
        if ( id != null ) {
            // Obtain the protocol from the string
            index = id.indexOf(delim);
        }
        if ( index == -1 ) {
            throw new InvalidIDException(CorePlugin.Util.getString("ParsedObjectID.The_stringified_ObjectID_does_not_have_a_protocol")); //$NON-NLS-1$
        }
        return new ParsedObjectID(id.substring(0,index),id.substring(index+DELIMITER_LENGTH));
    }

    public static ParsedObjectID parsedStringifiedObjectID( final String id, final String expectedProtocol ) throws InvalidIDException {
        if ( expectedProtocol == null ) {
            return new ParsedObjectID("",id); //$NON-NLS-1$
        }
        if ( id.startsWith(expectedProtocol) ) {
            return new ParsedObjectID(expectedProtocol,id.substring(expectedProtocol.length()+DELIMITER_LENGTH));
        }
        throw new InvalidIDException(CorePlugin.Util.getString("ParsedObjectID.The_stringified_ObjectID_does_not_have_the_required_protocol_{0}",expectedProtocol)); //$NON-NLS-1$
    }

}
