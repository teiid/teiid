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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.teiid.core.util.FileUtils;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.VDBResource;
import org.teiid.query.QueryPlugin;
import org.teiid.query.QueryPlugin.Event;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("rawtypes")
public class DDLFileMetadataRepository extends MetadataRepository {
	
	public static final String SEPARATION = ",";
	public static final String SUFFIX = ".tmp";
	public static final boolean APPEND = true;
	
	@Override
	public void loadMetadata(MetadataFactory factory, ExecutionFactory executionFactory, Object connectionFactory) throws TranslatorException {
		String ddlFileString = factory.getModelProperties().getProperty("ddl-file");
		
		if (ddlFileString == null) {
			ddlFileString = factory.getRawMetadata();
		}
		
		String[] ddlFiles =  ddlFileString.split(SEPARATION);
		
		File tempDir = new File(FileUtils.TEMP_DIRECTORY);
	    
		File tempFile = null;
		try {
			tempFile = File.createTempFile(factory.getVdbName() +"_"+ factory.getVdbVersion() , SUFFIX, tempDir);
		} catch (IOException ioException) {
			throw new MetadataException(Event.TEIID31137, QueryPlugin.Util.gs(Event.TEIID31137, factory.getVdbName() +"_"+ factory.getVdbVersion()+SUFFIX));
		}
	    
	    /* if old file already exist , delete content of old file before writing new content */
	    if (tempFile != null && tempFile.exists()) {
	    	tempFile.delete();
        }
		
		/* merge multiple ddl file into temp file */
		for(String filePath : ddlFiles){
			
			VDBResource resource = factory.getVDBResources().get(filePath);
			
			if (resource == null) {	
				throw new MetadataException(Event.TEIID31137, QueryPlugin.Util.gs(Event.TEIID31137, filePath));	
			}
			
			try {
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resource.openStream(), "UTF-8"));
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tempFile, APPEND));
				
				String line = "";
				
				while ( (line = bufferedReader.readLine()) != null) {
					bufferedWriter.write(line);
					bufferedWriter.newLine();
				}
				
				if(bufferedReader != null)
					bufferedReader.close();
				if(bufferedWriter != null){
					bufferedWriter.flush();
					bufferedWriter.close();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				throw new MetadataException(Event.TEIID31137, QueryPlugin.Util.gs(Event.TEIID31137, "closing stream"));
			}
		}
		
		
		
		if (tempFile != null && tempFile.exists() ) {
			
			InputStream is;
			try {
				is = new FileInputStream(tempFile.getAbsolutePath());
			} catch (IOException e1) {
				throw new MetadataException(e1);
			}
			try {
				//TODO: could allow for a property driven encoding
				factory.parse(new InputStreamReader(is, Charset.forName("UTF-8"))); //$NON-NLS-1$
			} finally {
				try {
					is.close();
					
					if(tempFile != null && tempFile.exists())	
						if(!tempFile.delete())
							tempFile.deleteOnExit();
					
				} catch (IOException e) {
				}
			}
		}
	}

}
