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

package org.teiid.adminapi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.core.TeiidException;


/**
 * <code>AdminException</code> is the base exception for the admin package.  Many *Admin methods throw this
 * exception.  Instances will be one of the concrete subtypes:
 * {@link AdminComponentException} or {@link AdminProcessingException}</p>
 *
 * <p><code>AdminException</code>s may contain multiple child exceptions. An example
 * of this could be when performing an admin action results in multiple failures. Admin
 * clients should be aware of this and use the {@link #hasMultiple()} method to
 * determine if they need to check the child exceptions.</p>
 */
public abstract class AdminException extends TeiidException {

	private static final long serialVersionUID = -4446936145500241358L;
	// List of Admin exceptions in
    // case of multiple failure
    private List children;

    /**
     * No-arg ctor.
     *
     * @since 4.3
     */
    AdminException() {
        super();
    }

    /**
     * Construct with a message.
     * @param msg the error message.
     * @since 4.3
     */
    AdminException(String msg) {
        super(msg);
    }
    
    AdminException(Throwable cause) {
    	this(cause.getMessage(), cause);
    }

    /**
     * Construct with an optional error code and a message.
     * @param code an optional error code
     * @param msg the error message.
     * @since 4.3
     */
    AdminException(int code, String msg) {
        super(Integer.toString(code), msg);
    }
    
    AdminException(String msg, Throwable cause) {
        super(cause, msg);
    }

    AdminException(int code, String msg, Throwable cause) {
        super(cause, Integer.toString(code),msg);
    }

    /**
     * Determine whether this exception is representing
     * mutliple component failures.
     * @return <code>true</code> iff this exception contains multiple
     * component failure exceptions.
     * @since 4.3
     */
    public boolean hasMultiple() {
        return (children != null && children.size() > 0);
    }

    /**
     * Returns a non-null list of failures (<code>AdminException</code>s), one for each
     * component that failed.
     *
     * <p>The list will have members when {@link #hasMultiple()} returns <code>true</code>.</p>
     * @return The non-null list of failures.
     * @since 4.3
     */
    public List getChildren() {
        return (children != null ? children : Collections.EMPTY_LIST);
    }

    /**
     * Add a child <code>AdminException</code> for a particular failure
     * if and action resulted in multiple failures.
     *
     * @param child a specific failure
     * @since 4.3
     */
    public void addChild(AdminException child) {
        if ( children == null ) {
            children = new ArrayList();
        }
        children.add(child);
    }
}
