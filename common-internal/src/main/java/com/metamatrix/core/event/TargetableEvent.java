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

package com.metamatrix.core.event;

import java.util.EventObject;

import com.metamatrix.core.id.ObjectID;

public class TargetableEvent extends EventObject {
    private Object target;
    private ObjectID eventID;

    public TargetableEvent(Object source, Object target) {
        this(source,target,null);
    }

    public TargetableEvent(Object source, Object target, ObjectID eventID) {
        super(source);
        this.target  = target;
        this.eventID = eventID;
    }

    /**
     * Return the target of the event.
     * @return the target object, which may be null
     */
    public Object getTarget() {
        return this.target;
    }

    /**
     * Return the identifier associted with this event.
     * @return the identifier, which may be null
     */
    public ObjectID getEventID() {
        return this.eventID;
    }
}

