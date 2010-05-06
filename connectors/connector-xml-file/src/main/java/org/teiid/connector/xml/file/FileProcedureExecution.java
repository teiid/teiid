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

package org.teiid.connector.xml.file;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.Call;
import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.common.types.InputStreamFactory;
import com.metamatrix.common.types.SQLXMLImpl;


/** 
 * Execution class for File based XML Source.
 */
public class FileProcedureExecution extends BasicExecution implements ProcedureExecution {
    
    RuntimeMetadata metadata = null;
    ExecutionContext context;
    File rootFolder;
    private Call procedure;
    private FileManagedConnectionFactory config;
    private boolean returnedResult;
    private SQLXML returnValue;
    
    /** 
     * @param env
     * @param conn
     * @param context
     * @param metadata
     */
    public FileProcedureExecution(Call proc, FileManagedConnectionFactory env, RuntimeMetadata metadata, ExecutionContext context) {
        this.metadata = metadata;
        this.context = context;
        this.procedure = proc;
        this.config = env;
        this.rootFolder = new File(this.config.getDirectoryLocation());
    }
    
    /** 
     * @see org.teiid.connector.api.ProcedureExecution#execute(org.teiid.connector.language.Call, int)
     */
    @Override
    public void execute() throws ConnectorException {
        
        // look for the name of the file to return in the metadata, "Name in Source" property
    	AbstractMetadataRecord metaObject = procedure.getMetadataObject();
        String fileName = metaObject.getNameInSource();
                
        // if the source procedure name is not supplied then throw an exception
        if (fileName == null || fileName.length() == 0) {
            String msg = XMLSourcePlugin.Util.getString("source_name_not_supplied", new Object[] {procedure.getProcedureName()}); //$NON-NLS-1$
            throw new ConnectorException(msg);            
        }
        
        // try to open the file and read.        
        final File xmlFile = new File(this.rootFolder, fileName);
        if (!xmlFile.exists()) {
            throw new ConnectorException(XMLSourcePlugin.Util.getString("XML_file_not_found", new Object[] {fileName, this.rootFolder.getAbsolutePath()})); //$NON-NLS-1$            
        }        
        
        String encoding = this.config.getCharacterEncodingScheme();
        
        returnValue = new SQLXMLImpl(new InputStreamFactory(encoding) {
			
			@Override
			public InputStream getInputStream() throws IOException {
				return new BufferedInputStream(new FileInputStream(xmlFile));
			}
		});
        
        this.config.getLogger().logDetail(XMLSourcePlugin.Util.getString("executing_procedure", new Object[] {procedure.getProcedureName()})); //$NON-NLS-1$
    }
    
    @Override
    public List<?> next() throws ConnectorException, DataNotAvailableException {
    	if (!returnedResult) {
    		returnedResult = true;
    		return Arrays.asList(returnValue);
    	}
    	return null;
    }  
    
    @Override
    public List<?> getOutputParameterValues() throws ConnectorException {
        throw new ConnectorException(XMLSourcePlugin.Util.getString("No_outputs_allowed")); //$NON-NLS-1$
    }

    public void close() throws ConnectorException {
        // no-op
    }

    public void cancel() throws ConnectorException {
        // no-op
    }    
    
}
