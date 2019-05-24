/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.query.function;

import java.util.Collection;
import java.util.Map;

import org.teiid.core.CoreConstants;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.FunctionMethod;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.function.source.SystemSource;
import org.teiid.query.validator.ValidatorReport;


public class SystemFunctionManager {

    private FunctionTree systemFunctionTree;
    private Map<String, Datatype> types;

    public SystemFunctionManager(Map<String, Datatype> typeMap) {
        this.types = typeMap;
        // Create the system source and add it to the source list
        SystemSource systemSource = new SystemSource();
        // Validate the system source - should never fail
        ValidatorReport report = new ValidatorReport("Function Validation"); //$NON-NLS-1$
        Collection<FunctionMethod> functionMethods = systemSource.getFunctionMethods();
        FunctionMetadataValidator.validateFunctionMethods(functionMethods,report, types);
        if(report.hasItems()) {
            // Should never happen as SystemSourcTe doesn't change
            System.err.println(QueryPlugin.Util.getString("ERR.015.001.0005", report)); //$NON-NLS-1$
        }
        systemFunctionTree = new FunctionTree(CoreConstants.SYSTEM_MODEL, systemSource, true);
    }

    public FunctionTree getSystemFunctions() {
        return systemFunctionTree;
    }

    public FunctionLibrary getSystemFunctionLibrary() {
        return new FunctionLibrary(getSystemFunctions());
    }

}
