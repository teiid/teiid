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

package com.metamatrix.common.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to return to a client the results of a complex management
 * request.  The meaning of "result object" and "failured objects" is dependent upon the context of the method
 * from which this instance is returned.
 * <p>
 * This class uses a Map to hold those objects that were not successfully operated upon and
 * a corresponding exception; therefore, only objects which have a unique hash code for different
 * (but unequivalent) instances should be inserted as failed objects.
 */
public class MultipleRequestConfirmation implements Serializable {

    private Object result = null;
    private Map failures = new HashMap();

    /**
     * Construct an empty confirmation instance.
     */
    public MultipleRequestConfirmation() {}

    /**
     * Set the result of the method request.
     * @param obj the result object
     */
    public void setResult( Object result ) {
        this.result = result;
    }

    /**
     * Get the result of the operation.
     * @return the object that contains or is the results for the operation; while
     * this class can contain a null value for the result, the method from which this
     * object is returned should document whether null results are allowed or expected.
     */
    public Object getResult() {
        return result;
    }

    /**
     * Add the object that was not successfully operated upon.
     * @param obj the object with which the operation failed.
     * @param e an exception that describes the failure of this object; may be null
     */
    public void addFailure( Object obj, Throwable e ) {
        failures.put(obj,e);
    }

    /**
     * Get the object that were not successfully operated upon, and an exception
     * for each that may describe why the operation failed.
     * @return the map keyed upon the objects with which the operation failed; the
     * value for each object key is the exception that was thrown during the operation
     * upon that object, or null if no exception was thrown during the operation
     * but failure still occurred.
     */
    public Map getFailures() {
        return failures;
    }

    /**
     * Get the number of objects that were not successfully operated upon.
     * @return the number of objects with which the operation failed.
     */
    public int getFailuresCount() {
        return failures.size();
    }

    /*
     * Determine whether there is a result object for this confirmation.
     * @return true if this confirmation object contains a results object, or false otherwise.
     * @see getResult
     */
    public boolean hasResult() {
        return ( result != null);
    }

    /**
     * Determine whether there are any objects that were not successfully operated upon.
     * @return true if at least one operation upon an object was added as failed, or false otherwise.
     */
    public boolean hasFailures() {
        return !failures.isEmpty();
    }

}

