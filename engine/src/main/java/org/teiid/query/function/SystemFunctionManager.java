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
package org.teiid.query.function;

import java.util.Collection;

import org.teiid.core.CoreConstants;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.function.source.SystemSource;
import org.teiid.query.report.ActivityReport;


public class SystemFunctionManager {

	private FunctionTree systemFunctionTree;
	private boolean allowEnvFunction = true;
	
	public FunctionTree getSystemFunctions() {
    	if(systemFunctionTree == null) { 
	    	// Create the system source and add it to the source list
	    	SystemSource systemSource = new SystemSource(this.allowEnvFunction);
	
			// Validate the system source - should never fail
	        ActivityReport report = new ActivityReport("Function Validation"); //$NON-NLS-1$
	       	validateSource(systemSource, report);
			if(report.hasItems()) {
			    // Should never happen as SystemSourcTe doesn't change
			    System.err.println(QueryPlugin.Util.getString("ERR.015.001.0005", report)); //$NON-NLS-1$
			}
			systemFunctionTree = new FunctionTree(CoreConstants.SYSTEM_MODEL, systemSource, true);
    	}
    	return systemFunctionTree;
    }
    
    public FunctionLibrary getSystemFunctionLibrary() {
    	return new FunctionLibrary(getSystemFunctions());
    }
    
    /**
     * Validate all function metadata in the source with the FunctionMetadataValidator.  Add
     * any problems to the specified report.
     * @param source Source of function metadata
     * @param report Report to update with any problems
     */
    private void validateSource(FunctionMetadataSource source, ActivityReport report) {
        Collection functionMethods = source.getFunctionMethods();
    	FunctionMetadataValidator.validateFunctionMethods(functionMethods,report);
    }
    
    public boolean isAllowEnvFunction() {
		return allowEnvFunction;
	}

	public void setAllowEnvFunction(boolean allowEnvFunction) {
		this.allowEnvFunction = allowEnvFunction;
	}    
}
