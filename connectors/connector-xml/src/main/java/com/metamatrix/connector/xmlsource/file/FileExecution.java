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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.metadata.runtime.MetadataObject;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xmlsource.XMLSourceExecution;
import com.metamatrix.connector.xmlsource.XMLSourcePlugin;


/** 
 * Execution class for File based XML Source.
 */
public class FileExecution extends XMLSourceExecution {
    private static final String ENCODING = "CharacterEncodingScheme"; //$NON-NLS-1$
    private static final String ISO8859 = "ISO-8859-1"; //$NON-NLS-1$
    
    RuntimeMetadata metadata = null;
    Source returnValue = null;
    ExecutionContext context;
    File rootFolder;
    private IProcedure procedure;
    
    /** 
     * @param env
     * @param conn
     * @param context
     * @param metadata
     */
    public FileExecution(IProcedure proc, ConnectorEnvironment env, RuntimeMetadata metadata, ExecutionContext context, File rootFolder) 
        throws ConnectorException{
        super(env);
        this.metadata = metadata;
        this.context = context;
        this.rootFolder = rootFolder;
        this.procedure = proc;
    }
    
    /** 
     * @see org.teiid.connector.api.ProcedureExecution#execute(org.teiid.connector.language.IProcedure, int)
     */
    @Override
    public void execute() throws ConnectorException {
        
        // look for the name of the file to return in the metadata, "Name in Source" property
        MetadataObject metaObject = procedure.getMetadataObject();
        String fileName = metaObject.getNameInSource();
                
        // if the source procedure name is not supplied then throw an exception
        if (fileName == null || fileName.length() == 0) {
            String msg = XMLSourcePlugin.Util.getString("source_name_not_supplied", new Object[] {procedure.getProcedureName()}); //$NON-NLS-1$
            XMLSourcePlugin.logError(this.env.getLogger(), msg); 
            throw new ConnectorException(msg);            
        }
        
        // try to open the file and read.        
        File xmlFile = new File(this.rootFolder, fileName);
        if (!xmlFile.exists()) {
            throw new ConnectorException(XMLSourcePlugin.Util.getString("XML_file_not_found", new Object[] {fileName, this.rootFolder.getAbsolutePath()})); //$NON-NLS-1$            
        }        
        
        Properties props = this.env.getProperties();
        String encoding = props.getProperty(ENCODING, ISO8859);
        
        try {
			this.returnValue = new StreamSource(new InputStreamReader(new FileInputStream(xmlFile), encoding));
		} catch (IOException e) {
			throw new ConnectorException(e);
		} 
		
        XMLSourcePlugin.logDetail(this.env.getLogger(), "executing_procedure", new Object[] {procedure.getProcedureName()}); //$NON-NLS-1$
    }
    
    @Override
	public Source getReturnValue() {
		return returnValue;
	}

}
