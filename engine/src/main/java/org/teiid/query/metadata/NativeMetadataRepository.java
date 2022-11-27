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
package org.teiid.query.metadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.AccessibleByteArrayOutputStream;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.query.QueryPlugin;
import org.teiid.resource.api.WrappedConnection;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class NativeMetadataRepository implements MetadataRepository {

    public static final String IMPORT_PUSHDOWN_FUNCTIONS = "importer.importPushdownFunctions"; //$NON-NLS-1$

    @Override
    public void loadMetadata(MetadataFactory factory, ExecutionFactory executionFactory, Object connectionFactory) throws TranslatorException {

        if (executionFactory == null ) {
            throw new TranslatorException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30591, factory.getName()));
        }

        if (connectionFactory == null && executionFactory.isSourceRequiredForMetadata()) {
            throw new TranslatorException(QueryPlugin.Event.TEIID31097, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31097));
        }
        ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(executionFactory.getClass().getClassLoader());
            getMetadata(factory, executionFactory, connectionFactory);
        } finally {
            Thread.currentThread().setContextClassLoader(originalCL);
        }
    }

    private void getMetadata(MetadataFactory factory, ExecutionFactory executionFactory, Object connectionFactory)
            throws TranslatorException {
        Object connection = null;
        try {
            connection = executionFactory.getConnection(connectionFactory, null);
        } catch (Throwable e) {
            // if security pass through is enabled the connection creation may fail at the startup
            if (executionFactory.isSourceRequiredForMetadata()) {
                throw new TranslatorException(QueryPlugin.Event.TEIID31178, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31178, factory.getName()));
            }
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception getting connection for metadata, but no connection is required"); //$NON-NLS-1$
        }

        Object unwrapped = null;

        if (connection instanceof WrappedConnection) {
            try {
                unwrapped = ((WrappedConnection)connection).unwrap();
            } catch (Exception e) {
                if (executionFactory.isSourceRequiredForMetadata()) {
                    throw new TranslatorException(QueryPlugin.Event.TEIID30477, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30477, factory.getName()));
                }
                connection = null;
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Could not unwrap exception to get metadata, but no connection is required"); //$NON-NLS-1$
            }
        }

        try {
            executionFactory.getMetadata(factory, (unwrapped == null) ? connection:unwrapped);
        } finally {
            executionFactory.closeConnection(connection, connectionFactory);
        }

        if (factory.isImportPushdownFunctions()) {
            List<FunctionMethod> functions = executionFactory.getPushDownFunctions();
            //create a copy and add to the schema
            if (!functions.isEmpty()) {
                try {
                    AccessibleByteArrayOutputStream baos = new AccessibleByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeObject(functions);
                    oos.close();
                    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.getBuffer(), 0, baos.getCount()));
                    functions = (List<FunctionMethod>) ois.readObject();
                    for (FunctionMethod functionMethod : functions) {
                        factory.addFunction(functionMethod);
                        functionMethod.setProperty(FunctionMethod.SYSTEM_NAME, functionMethod.getName());
                    }
                } catch (IOException e) {
                    throw new TeiidRuntimeException(e);
                } catch (ClassNotFoundException e) {
                    throw new TeiidRuntimeException(e);
                }
            }
        }
    }

}
