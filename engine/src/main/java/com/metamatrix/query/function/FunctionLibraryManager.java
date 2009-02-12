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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.function.metadata.FunctionMetadataValidator;
import com.metamatrix.query.function.source.SystemSource;
import com.metamatrix.query.report.ActivityReport;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * Factory to obtain the local FunctionLibrary and register sources of function metadata.
 */
public class FunctionLibraryManager {

	// Reference to singleton FunctionLibrary
	private static FunctionLibrary LIB;

	// List of reloadable (user-defined) sources of metadata
    private static List RELOADABLE_SOURCES;

    // Static initializer
    static {
        // Create the function library instance
        LIB = new FunctionLibrary();

        // Create the system source and add it to the source list
        FunctionMetadataSource systemSource = new SystemSource();

        // Load the system source
        LIB.setSystemFunctions(systemSource);

		// Validate the system source - should never fail
        ActivityReport report = new ActivityReport("Function Validation"); //$NON-NLS-1$
       	validateSource(systemSource, report);
		if(report.hasItems()) {
		    // Should never happen as SystemSource doesn't change
		    System.err.println(QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0005, report));
		}

        // Initialize the reloadable sources
        RELOADABLE_SOURCES = new ArrayList();

    }

    /**
     * Can't construct - just a factory
     */
	private FunctionLibraryManager() {}

    /**
     * Factory method to obtain a reference to a function.
     * @return Function library to find and invoke functions
     */
	public static FunctionLibrary getFunctionLibrary() {
		return LIB;
	}

    /**
     * Register a new source of function metadata.  The method
     * {@link #reloadSources} will be called as a result.
     * @param source A new source of function metadata
     * @return Report of any invalid FunctionMethod objects
     */
    public static synchronized ActivityReport registerSource(FunctionMetadataSource source) {
        // Add the reloadable source
        RELOADABLE_SOURCES.add(source);

        // Reload
        return reloadSources();
    }

    /**
     * Register a new source of function metadata.  The method
     * {@link #reloadSources} will be called as a result.
     * @param source A new source of function metadata
     * @return Report of any invalid FunctionMethod objects
     */
    public static synchronized ActivityReport deregisterSource(FunctionMetadataSource source) {
        // Add the reloadable source
        RELOADABLE_SOURCES.remove(source);

        // Reload
        return reloadSources();
    }
    
    /**
     * Reload all sources.  All valid functions in the registered function sources
     * are reloaded.  Any invalid functions are noted in the report.
     * @return Report of any invalid FunctionMethod objects.
     */
    public static synchronized ActivityReport reloadSources() {
        // Create activity report
        ActivityReport report = new ActivityReport("Function Validation"); //$NON-NLS-1$

        // Reload and re-validate all metadata sources
        Iterator iter = RELOADABLE_SOURCES.iterator();
        while(iter.hasNext()) {
        	FunctionMetadataSource source = (FunctionMetadataSource) iter.next();

        	// Reload - even though it is reload it is never called
        	// other than the register methods, so it may of no use.
        	// if need it we will get this back.
        	//source.reloadMethods();

        	// Validate
        	validateSource(source, report);
        }

        // Upload all new reloadable functions into function library in bulk
        LIB.replaceReloadableFunctions(RELOADABLE_SOURCES);

        // Return report of failures during reload
        return report;
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
