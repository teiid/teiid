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

import java.io.File;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.basic.BasicManagedConnectionFactory;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.language.Select;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xml.IQueryPreprocessor;



/** 
 * This is a connection object for File based XML File.
 */
public class FileConnection extends BasicConnection {
    
    boolean connected = false;
    private FileManagedConnectionFactory config;
    
    public FileConnection(FileManagedConnectionFactory env) 
        throws ConnectorException {
        this.config = env;
        connect();
    }
    
    @Override
    public ProcedureExecution createProcedureExecution(Call command,ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
        return new FileProcedureExecution(command, this.config, metadata, executionContext);
    }

    @Override
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
    	IQueryPreprocessor preProcessor = getQueryPreProcessor();
    	if (preProcessor != null) {
    		command = preProcessor.preprocessQuery((Select)command, metadata, executionContext, this.config);
    		this.config.getLogger().logTrace("XML Connector Framework: executing command: " + command);
    	}
		return new FileResultSetExecution((Select)command, this.config, metadata, executionContext);
	}

    @Override
	public void close() {
        disconnect();
    }
    
    /**
     * Connect to the source 
     */
    void connect() throws ConnectorException {
        String dirPath = this.config.getDirectoryLocation(); 
        
        if (dirPath == null) {
            throw new ConnectorException(XMLSourcePlugin.Util.getString("XML_dir_property_missing")); //$NON-NLS-1$            
        }
        
        // try to open the file and read.
        File rootDirectory = new File(dirPath);
        if (!rootDirectory.exists()) {
            throw new ConnectorException(XMLSourcePlugin.Util.getString("XML_dir_property_wrong", new Object[] {dirPath})); //$NON-NLS-1$            
        }
        this.connected = true;        
        this.config.getLogger().logDetail(XMLSourcePlugin.Util.getString("file_connection_open", new Object[] {rootDirectory.getAbsolutePath()})); //$NON-NLS-1$
    }

    /**
     * Close the resources and disconnect 
     */
    void disconnect() {
        this.connected = false;
        this.config.getLogger().logDetail(XMLSourcePlugin.Util.getString("file_connection_closed")); //$NON-NLS-1$
    }

	public boolean isConnected() {
        return this.connected;
    }
	
	private IQueryPreprocessor getQueryPreProcessor() throws ConnectorException {
		String className = this.config.getQueryPreprocessorClass();
		if (className == null) {
			return null;
		}
		return BasicManagedConnectionFactory.getInstance(IQueryPreprocessor.class, className, null, null);
	}
}
