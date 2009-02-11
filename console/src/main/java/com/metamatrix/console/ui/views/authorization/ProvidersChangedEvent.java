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

//#############################################################################
package com.metamatrix.console.ui.views.authorization;

import java.util.EventObject;

import com.metamatrix.common.config.api.Configuration;

/**
 * The <code>ProvidersChangeEvent</code> is used to notify
 * {@link ProvidersChangeListener}s that a change in a
 * {@link Configuration} has occurred.
 * @since Golden Gate
 * @version 1.0
 * @author Dan Florian
 */
public class ProvidersChangedEvent extends EventObject {

    ///////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////

    /** Indicates a provider has been deleted. */
    public final static int DELETED = 0x001;

    /** Indicates a provider has been modified. */
    public final static int MODIFIED = 0x002;

    /** Indicates a provider has been added. */
    public final static int NEW = 0x0004;


    ///////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////

    /** The event type. */
    protected int type;

    ///////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////

    /**
     * Constructs a <code>ConfigurationChangeEvent</code> of the given type.
     * @param theType the event type
     * @param theChangedObject the object whose state has changed
     * @throws IllegalArgumentException if type is not valid, if the
     * changed object is <code>null</code>, or if the configuration
     * is <code>null</code>.
     */
    public ProvidersChangedEvent(int theType, Object theChangedObject) {

        super(theChangedObject);
        if (theChangedObject == null) {
            throw new IllegalArgumentException("Object cannot be null."); //$NON-NLS-1$
        }
        if ((theType != DELETED) && (theType != MODIFIED) && (theType != NEW) ) {
            throw new IllegalArgumentException(
                "Invalid event type <" + theType + ">."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        type = theType;
    }

    /**


    ///////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////

    /**
     * Gets the event type.
     * @return the event type
     */
    public int getType() {
        return type;
    }

    /**
     * Indicates if the changed object has been deleted.
     * @return <code>true</code> if the changed object has been deleted;
     * <code>false</code> otherwise.
     */
    public boolean isDeleted() {
        return ((type & DELETED) == DELETED);
    }

    /**
     * Indicates if the changed object has been modified.
     * @return <code>true</code> if the changed object has been modified;
     * <code>false</code> otherwise.
     */
    public boolean isModified() {
        return ((type & MODIFIED) == MODIFIED);
    }

    /**
     * Indicates if the changed object is new.
     * @return <code>true</code> if the changed object is new;
     * <code>false</code> otherwise.
     */
    public boolean isNew() {
        return ((type & NEW) == NEW);
    }


}
