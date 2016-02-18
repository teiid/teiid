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
package org.teiid.query.metadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import javax.resource.ResourceException;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.AccessibleByteArrayOutputStream;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.query.QueryPlugin;
import org.teiid.resource.spi.WrappedConnection;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class NativeMetadataRepository extends MetadataRepository {

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
                throw new TranslatorException(QueryPlugin.Event.TEIID30477, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30477));
            } 
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Could not get exception to get metadata, but no connection is required"); //$NON-NLS-1$
        }
        
		Object unwrapped = null;
		
		if (connection instanceof WrappedConnection) {
			try {
				unwrapped = ((WrappedConnection)connection).unwrap();
			} catch (ResourceException e) {
				if (executionFactory.isSourceRequiredForMetadata()) {
					throw new TranslatorException(QueryPlugin.Event.TEIID30477, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30477));
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
		
		if (PropertiesUtils.getBooleanProperty(factory.getModelProperties(), IMPORT_PUSHDOWN_FUNCTIONS, false)) { //$NON-NLS-1$
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
