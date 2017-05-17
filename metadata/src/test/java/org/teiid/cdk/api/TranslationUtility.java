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

package org.teiid.cdk.api;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.teiid.cdk.CommandBuilder;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.Command;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.BasicQueryMetadataWrapper;
import org.teiid.query.metadata.QueryMetadataInterface;


/**
 * <p>This translation utility can be used to translate sql strings into 
 * Connector API language interfaces for testing purposes.  The utility
 * requires a metadata .vdb file in order to resolve references in 
 * the SQL.</p>  
 * 
 * <p>This utility class can also be used to obtain a RuntimeMetadata
 * implementation based on the VDB file, which is sometimes handy when writing 
 * unit tests.</p>
 */
public class TranslationUtility {
    
    private QueryMetadataInterface metadata;
    private FunctionLibrary functionLibrary;

    /**
     * Construct a utility instance with a given vdb file.  
     * @param vdbFile The .vdb file name representing metadata for the connector
     */
    public TranslationUtility(String vdbFile) {
        initWrapper(VDBMetadataFactory.getVDBMetadata(vdbFile));
    }

	private void initWrapper(QueryMetadataInterface acutalMetadata) {
		functionLibrary = acutalMetadata.getFunctionLibrary();
		this.functions.addAll(Arrays.asList(this.functionLibrary.getUserFunctions()));
		metadata = new BasicQueryMetadataWrapper(acutalMetadata) {
        	@Override
        	public FunctionLibrary getFunctionLibrary() {
        		return functionLibrary;
        	}
        };
	}
    
    public TranslationUtility(String vdbName, URL url) {
        try {
    		initWrapper(VDBMetadataFactory.getVDBMetadata(vdbName, url, null));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}     
    }
    
    public TranslationUtility(QueryMetadataInterface metadata) {
    	initWrapper(metadata);
    }
    
    public Command parseCommand(String sql, boolean generateAliases, boolean supportsGroupAliases) {
        CommandBuilder commandBuilder = new CommandBuilder(metadata);
        return commandBuilder.getCommand(sql, generateAliases, supportsGroupAliases);
    }
    
    /**
     * Parse a SQL command and return an ICommand object.
     * @param sql
     * @return Command using the language interfaces
     */
    public Command parseCommand(String sql) {
        CommandBuilder commandBuilder = new CommandBuilder(metadata);
        return commandBuilder.getCommand(sql);
    }
    
    /**
     * Create a RuntimeMetadata that can be used for testing a connector.
     * This RuntimeMetadata instance will be backed by the metadata from the vdbFile
     * this translation utility instance was created with.
     * @return RuntimeMetadata for testing
     */
    public RuntimeMetadata createRuntimeMetadata() {
        return new RuntimeMetadataImpl(metadata);
    }
    
    private List<FunctionTree> functions = new ArrayList<FunctionTree>();
    
    public void addUDF(String schema, Collection<FunctionMethod> methods) {
    	if (methods == null || methods.isEmpty()) {
    		return;
    	}
    	this.functions.add(new FunctionTree(schema, new UDFSource(methods)));
		SystemFunctionManager sfm = new SystemFunctionManager();
		functionLibrary = new FunctionLibrary(sfm.getSystemFunctions(), this.functions.toArray(new FunctionTree[this.functions.size()]));
    }

}
