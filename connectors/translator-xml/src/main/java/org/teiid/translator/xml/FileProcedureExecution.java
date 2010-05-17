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

package org.teiid.translator.xml;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.language.Call;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.FileConnection;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;


/** 
 * Execution class for File based XML Source.
 */
public class FileProcedureExecution implements ProcedureExecution {
    
    private Call procedure;
    private XMLExecutionFactory executionFactory;
    private boolean returnedResult;
    private SQLXML returnValue;
    private FileConnection connection; 
    
    /** 
     * @param env
     * @param conn
     * @param context
     * @param metadata
     */
    public FileProcedureExecution(Call proc, XMLExecutionFactory executionFactory, FileConnection connection) {
        this.procedure = proc;
        this.executionFactory = executionFactory;
        this.connection = connection; 
    }
    
    /** 
     * @see org.teiid.connector.api.ProcedureExecution#execute(org.teiid.connector.language.Call, int)
     */
    @Override
    public void execute() throws TranslatorException {
        
        // look for the name of the file to return in the metadata, "Name in Source" property
    	AbstractMetadataRecord metaObject = procedure.getMetadataObject();
        String fileName = metaObject.getNameInSource();
                
        // if the source procedure name is not supplied then throw an exception
        if (fileName == null || fileName.length() == 0) {
            throw new TranslatorException(XMLPlugin.Util.getString("source_name_not_supplied", new Object[] {procedure.getProcedureName()})); //$NON-NLS-1$            
        }
    
        final File[] files = this.connection.getFiles(fileName); 
        if (files.length == 0 || !files[0].exists()) {
        	throw new TranslatorException(XMLPlugin.Util.getString("file_not_supplied", new Object[] {fileName, procedure.getProcedureName()})); //$NON-NLS-1$
        }
        
        String encoding = this.executionFactory.getCharacterEncodingScheme();
        
        returnValue = new SQLXMLImpl(new InputStreamFactory(encoding) {
			
			@Override
			public InputStream getInputStream() throws IOException {
				return new BufferedInputStream(new FileInputStream(files[0]));
			}
		});
        
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, XMLPlugin.Util.getString("executing_procedure", new Object[] {procedure.getProcedureName()})); //$NON-NLS-1$
    }
    
    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
    	if (!returnedResult) {
    		returnedResult = true;
    		return Arrays.asList(returnValue);
    	}
    	return null;
    }  
    
    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        throw new TranslatorException(XMLPlugin.Util.getString("No_outputs_allowed")); //$NON-NLS-1$
    }

    public void close() throws TranslatorException {
        // no-op
    }

    public void cancel() throws TranslatorException {
        // no-op
    }    
}
