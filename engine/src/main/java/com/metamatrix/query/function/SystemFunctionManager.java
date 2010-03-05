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
package com.metamatrix.query.function;

import java.util.Collection;
import java.util.Collections;

import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.function.metadata.FunctionMetadataValidator;
import com.metamatrix.query.function.source.SystemSource;
import com.metamatrix.query.report.ActivityReport;
import com.metamatrix.query.util.ErrorMessageKeys;

public class SystemFunctionManager {

	private static FunctionTree systemFunctionTree;
	
    static {
        // Create the system source and add it to the source list
    	SystemSource systemSource = new SystemSource();

		// Validate the system source - should never fail
        ActivityReport report = new ActivityReport("Function Validation"); //$NON-NLS-1$
       	validateSource(systemSource, report);
		if(report.hasItems()) {
		    // Should never happen as SystemSource doesn't change
		    System.err.println(QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0005, report));
		}
		
		systemFunctionTree = new FunctionTree(systemSource);
    }
	
    
    public static FunctionTree getSystemFunctions() {
    	return systemFunctionTree;
    }
    
    public static FunctionLibrary getSystemFunctionLibrary() {
    	return new FunctionLibrary(systemFunctionTree, new FunctionTree(new UDFSource(Collections.EMPTY_LIST)));
    }
    
    /**
     * Validate all function metadata in the source with the FunctionMetadataValidator.  Add
     * any problems to the specified report.
     * @param source Source of function metadata
     * @param report Report to update with any problems
     */
    private static void validateSource(FunctionMetadataSource source, ActivityReport report) {
        Collection functionMethods = source.getFunctionMethods();
    	FunctionMetadataValidator.validateFunctionMethods(functionMethods,report);
    }
}
