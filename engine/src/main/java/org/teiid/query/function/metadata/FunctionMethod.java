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

import org.teiid.metadata.FunctionParameter;


/**
 * @see FunctionMetadataValidator
 * @see FunctionCategoryConstants
 */
public class FunctionMethod extends org.teiid.metadata.FunctionMethod {
	private static final long serialVersionUID = -2380536393719646754L;

	/**
     * Construct a function method with default pushdown and null dependent attributes.
     * @param name Function name
     * @param description Function description
     * @param category Function category
     * @param invocationClass Invocation class
     * @param invocationMethod Invocation method
     * @param inputParams Input parameters
     * @param outputParam Output parameter (return parameter)
     */
    public FunctionMethod(String name, String description, String category, 
        String invocationClass, String invocationMethod, 
        FunctionParameter[] inputParams, FunctionParameter outputParam) {
        super(name, description, category, PushDown.CAN_PUSHDOWN, invocationClass, invocationMethod, inputParams, outputParam, true, Determinism.DETERMINISTIC);
    }

    /**
     * Construct a function method with all parameters assuming null dependent and non-deterministic.
     * @param name Function name
     * @param description Function description
     * @param category Function category
     * @param invocationClass Invocation class
     * @param invocationMethod Invocation method
     * @param inputParams Input parameters
     * @param outputParam Output parameter (return parameter)
     */
    public FunctionMethod(String name, String description, String category, 
        PushDown pushdown, String invocationClass, String invocationMethod, 
        FunctionParameter[] inputParams, FunctionParameter outputParam) {
        super(name, description, category, pushdown, invocationClass, invocationMethod, inputParams, outputParam, false,Determinism.NONDETERMINISTIC);
    }
    
    public FunctionMethod(String name,
            String description,
            String category,
            PushDown pushdown,
            String invocationClass,
            String invocationMethod,
            FunctionParameter[] inputParams,
            FunctionParameter outputParam,
            boolean nullDependent,
            Determinism deterministic) {
    	super(name, description, category, pushdown, invocationClass, invocationMethod, inputParams, outputParam, !nullDependent, deterministic);
    }
}
