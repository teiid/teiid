/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
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
import org.teiid.query.metadata.SystemMetadata;


/**
 * <p>This translation utility can be used to translate sql strings into
 * Connector API language interfaces for testing purposes.  The utility
 * requires a metadata .vdb file in order to resolve references in
 * the SQL.
 *
 * <p>This utility class can also be used to obtain a RuntimeMetadata
 * implementation based on the VDB file, which is sometimes handy when writing
 * unit tests.
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
            initWrapper(VDBMetadataFactory.getVDBMetadata(vdbName, url));
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
        SystemFunctionManager sfm = SystemMetadata.getInstance().getSystemFunctionManager();
        functionLibrary = new FunctionLibrary(sfm.getSystemFunctions(), this.functions.toArray(new FunctionTree[this.functions.size()]));
    }

}
