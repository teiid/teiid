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

package org.teiid.query.function.metadata;

import org.teiid.metadata.FunctionMethod;
import org.teiid.query.validator.ValidatorFailure;

/**
 * This is a specialized report item for reporting invalid function methods during
 * function metadata validation.  It is overrides ReportItem and adds an additional
 * attribute with the method reference for the invalid method.
 */
public class InvalidFunctionItem extends ValidatorFailure {
	
	private static final long serialVersionUID = 5679334286895174700L;

	/**
	 * Report item type
	 */
	public static final String INVALID_FUNCTION = "InvalidFunction"; //$NON-NLS-1$

	private FunctionMethod method;

    /**
     * Constructor for InvalidFunctionItem.
     */
    public InvalidFunctionItem() {
        super(INVALID_FUNCTION);
    }
    
    /** 
     * Construct with invalid function object and exception.
     * @param method Invalid function method object
     * @param message Message describing invalid function
     */
    public InvalidFunctionItem(FunctionMethod method, String message) { 
        this();
        setMessage(message);
        setMethod(method);
    }
    
    /**
     * Gets the method.
     * @return Returns a FunctionMethod
     */
    public FunctionMethod getMethod() {
        return method;
    }

    /**
     * Sets the method.
     * @param method The method to set
     */
    public void setMethod(FunctionMethod method) {
        this.method = method;
    }

}
