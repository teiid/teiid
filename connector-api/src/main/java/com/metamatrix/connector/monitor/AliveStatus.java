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

package com.metamatrix.connector.monitor;

import java.io.Serializable;


/** 
 * Present the status of an object as either ALIVE, DEAD, or UNKNOWN.
 * <pre>
 * AliveStatus status = someObject.isAlive();
 * 
 * if ( status == AliveStatus.ALIVE ) {
 *     // someObject says it's Alive!
 * }
 * </pre>
 * <b>Warning</b>: If two objects are loaded by different ClassLoaders,
 * you <i>can't</i> use == to compare. They will never be equal.
 * @since 4.2
 */
public final class AliveStatus implements Serializable {
    private static final int DEAD_STATUS = 0;
    private static final int UNKNOWN_STATUS = 1;
    private static final int ALIVE_STATUS = 2;
    private int status;
    
    /** Status is ALIVE */
    public static final AliveStatus ALIVE = new AliveStatus(ALIVE_STATUS);
    /** Status is UNKNOWN */
    public static final AliveStatus UNKNOWN = new AliveStatus(UNKNOWN_STATUS);
    /** Status is DEAD */
    public static final AliveStatus DEAD = new AliveStatus(DEAD_STATUS);
    
    private AliveStatus(int status) {
        this.status = status;
    }
    
    /**
     * Implemented so that deserialization of this object
     * produces the same value as the serialized.   
     * @return A new instance of the equivalent serialized object.
     * @throws java.io.ObjectStreamException
     */
    private Object readResolve () throws java.io.ObjectStreamException {
        return new AliveStatus(this.status);
    }  

    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 4.2
     */
    public boolean equals(Object obj) {
        if ( ! (obj instanceof AliveStatus) ) {
            return false;
        }
        return ((AliveStatus)obj).status == status;
    }

    /** 
     * @see java.lang.Object#hashCode()
     * @since 4.2
     */
    public int hashCode() {
        return super.hashCode() << status;
    }

    /** 
     * @see java.lang.Object#toString()
     * @since 4.2
     */
    public String toString() {
        String statusStr = "UNKNOWN"; //$NON-NLS-1$
        switch (this.status) {
            case 0:
                statusStr = "DEAD"; //$NON-NLS-1$
                break;
            case 1:
                statusStr = "UNKNOWN"; //$NON-NLS-1$
                break;
            case 2:
                statusStr = "ALIVE"; //$NON-NLS-1$
                break;
            default:
                statusStr = "UNKNOWN"; //$NON-NLS-1$
                break;
        }
        return statusStr;
    }
}
