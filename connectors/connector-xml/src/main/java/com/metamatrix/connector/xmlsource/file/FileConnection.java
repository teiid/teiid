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

package com.metamatrix.connector.xmlsource.file;

import java.io.File;
import java.util.Properties;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xmlsource.XMLSourceConnection;
import com.metamatrix.connector.xmlsource.XMLSourcePlugin;


/** 
 * This is a connection object for File based XML File.
 */
public class FileConnection extends XMLSourceConnection {
    
    private static final String DIRECTORY_LOCATION = "DirectoryLocation"; //$NON-NLS-1$
    boolean connected = false;
    File rootDirectory = null; // root directory where the XML files are stored 
    
    /**
     * ctor 
     * @param env - Connector environment
     * @throws ConnectorException
     */
    public FileConnection(ConnectorEnvironment env) 
        throws ConnectorException {
        super(env);
        connect();
    }

    @Override
    public ProcedureExecution createProcedureExecution(IProcedure command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
        return new FileExecution(command, this.env, metadata, executionContext, getXMLDirectory());
    }

    /** 
     * @see com.metamatrix.connector.xmlsource.XMLSourceConnection#release()
     */
    @Override
	public void close() {
        disconnect();
        super.close();        
    }
    
    /**
     * Connect to the source 
     */
    void connect() throws ConnectorException {
        Properties props = this.env.getProperties();
        String dirPath = props.getProperty(DIRECTORY_LOCATION); 
        
        if (dirPath == null) {
            throw new ConnectorException(XMLSourcePlugin.Util.getString("XML_dir_property_missing")); //$NON-NLS-1$            
        }
        
        // try to open the file and read.
        rootDirectory = new File(dirPath);
        if (!rootDirectory.exists()) {
            throw new ConnectorException(XMLSourcePlugin.Util.getString("XML_dir_property_wrong", new Object[] {dirPath})); //$NON-NLS-1$            
        }
        this.connected = true;        
        XMLSourcePlugin.logDetail(this.env.getLogger(), "file_connection_open", new Object[] {this.rootDirectory.getAbsolutePath()}); //$NON-NLS-1$
    }

    /**
     * Close the resources and disconnect 
     */
    void disconnect() {
        this.connected = false;
        XMLSourcePlugin.logDetail(this.env.getLogger(), "file_connection_closed", new Object[] {this.rootDirectory.getAbsolutePath()}); //$NON-NLS-1$
        this.rootDirectory = null;
    }

    /** 
     * @see com.metamatrix.connector.xmlsource.XMLSourceConnection#isConnected()
     */
    @Override
	public boolean isConnected() {
        return this.connected;
    }
    
    File getXMLDirectory() {
        return this.rootDirectory;
    }
}
