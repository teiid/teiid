/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import com.metamatrix.connector.xmlsource.XMLSourceExecution;
import com.metamatrix.connector.xmlsource.XMLSourcePlugin;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.MetadataObject;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;


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
    
    /** 
     * @param env
     * @param conn
     * @param context
     * @param metadata
     */
    public FileExecution(ConnectorEnvironment env, RuntimeMetadata metadata, ExecutionContext context, File rootFolder) 
        throws ConnectorException{
        super(env);
        this.metadata = metadata;
        this.context = context;
        this.rootFolder = rootFolder;
    }
    
    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#execute(com.metamatrix.data.language.IProcedure, int)
     */
    public void execute(IProcedure procedure, int maxBatchSize) throws ConnectorException {
        
        // look for the name of the file to return in the metadata, "Name in Source" property
        MetadataObject metaObject = this.metadata.getObject(procedure.getMetadataID());
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
		
        this.context.keepExecutionAlive(true);
        
        XMLSourcePlugin.logDetail(this.env.getLogger(), "executing_procedure", new Object[] {procedure.getProcedureName()}); //$NON-NLS-1$
    }

    /** 
     * @see com.metamatrix.connector.xmlsource.XMLSourceExecution#nextBatch()
     */
    public Batch nextBatch() throws ConnectorException {
        Batch b = new BasicBatch();
        List row = new ArrayList();
        if (this.returnValue != null) {
        	this.context.keepExecutionAlive(true);
            row.add(convertToXMLType(this.returnValue));   
        }
        else {
            row.add(this.returnValue);
        }
        b.addRow(row);
        b.setLast();
        return b;
    }  
    
    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#getOutputValue(com.metamatrix.data.language.IParameter)
     */
    public Object getOutputValue(IParameter parameter) throws ConnectorException {
        throw new ConnectorException(XMLSourcePlugin.Util.getString("No_outputs_allowed")); //$NON-NLS-1$
    }

    /** 
     * @see com.metamatrix.data.api.Execution#close()
     */
    public void close() throws ConnectorException {
        // no-op
    }

    /** 
     * @see com.metamatrix.data.api.Execution#cancel()
     */
    public void cancel() throws ConnectorException {
        // no-op
    }
}
