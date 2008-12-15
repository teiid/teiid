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

package com.metamatrix.common.vdb.api;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.util.TempDirectory;
import com.metamatrix.core.util.ZipFileUtil;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.core.vdb.VdbConstants;
import com.metamatrix.vdb.materialization.ScriptType;
import com.metamatrix.vdb.runtime.BasicModelInfo;
import com.metamatrix.vdb.runtime.BasicVDBDefn;

/**
 * Latest incarnation of the VDBContext, specifically for weeding out 
 * dependencies on the vdb.edit; So do put in Modeler dependencies here.
 */
public class VDBArchive {
    private static final Random RANDOM = new Random(System.currentTimeMillis());
	
	private TempDirectory tempDirectory;
	boolean open = false;
	
	// configuration def contents
	BasicVDBDefn def;
	
	// data roles contents
	char[] dataRoles;
	
	short status = VDBStatus.INCOMPLETE;
	
	Manifest manifest;
	
	boolean wsdlAvailable = false;
	
	// this in the underlying VDB file holding the archive
	File vdbFile;
	
	HashSet<String> pathsInArchive = new HashSet<String>();
	
	/**
	 * Build the VDBArchive from a supplied file. Note that any updates on the archive object will reflect
	 * in the supplied file.
	 * @param vdbFile
	 * @throws IOException
	 */
	public VDBArchive(File vdbFile) throws IOException {
		load(vdbFile);
	}
	
	/**
	 * Build VDB archive from given stream. Note that any updates on the archive object will reflect
	 * in the temporary file that this object manipulates. To get the modified archive use the getVDBStream
	 * to stream and save.
	 * 
	 * @param vdbStream
	 * @return
	 * @throws IOException
	 */
	public VDBArchive(InputStream vdbStream) throws IOException {
		
		if (vdbStream == null) {
			throw new IllegalArgumentException();
		}
		
		// Since VDB is a ZIP file we need to write disk before we can open it.
		// so throw this at some temp location
		open();		
		File dummyName = new File(this.tempDirectory.getPath(), Math.abs(RANDOM.nextLong())+".vdb"); //$NON-NLS-1$
		FileUtils.write(vdbStream, dummyName);
		
		load(dummyName);
	}
	
	/**
	 * Load a VDB archive file, loads the DEF, Data Roles and Manifest file.
	 * @param vdbFile - archive with contained DEF file
	 * @return BasicVDBDefn
	 * @throws IOException
	 */
	private void load(File vdbFile) throws IOException{
		
		if (vdbFile == null || !vdbFile.exists()) {
			throw new IllegalArgumentException();
		}
		
		// we need unzip this archive file in the temp location
		open();
		ZipFile archive = new ZipFile(vdbFile);
		
		try {
			
			this.pathsInArchive = getListOfEntries(archive);
			
			// check if manifest file is available then load it.
			InputStream manifestStream = getStream(VdbConstants.MANIFEST_MODEL_NAME, archive);
			if (manifestStream != null) {
				this.manifest = new Manifest();
				this.manifest.load(manifestStream);
			}			
			
			// get DEF file from zip and load it
			InputStream defStream = getStream(VdbConstants.DEF_FILE_NAME, archive);
			if (defStream != null) {
				this.def = readFromDef(defStream);
				defStream.close();
			}
			
			// check if the data roles file is available, if found load it.
			InputStream rolesStream = getStream(VdbConstants.DATA_ROLES_FILE, archive);
			if (rolesStream != null) {
				File rolesFile = new File(this.tempDirectory.getPath(), Math.abs(RANDOM.nextLong())+"_roles.xml"); //$NON-NLS-1$
                FileUtils.write(rolesStream, rolesFile);
                rolesStream.close();
                this.dataRoles = FileUtil.read(new FileReader(rolesFile)).toCharArray();
			}
			
			// check if WSDL is defined
			InputStream wsdlStream = getStream(VdbConstants.WSDL_FILENAME, archive);
			if (wsdlStream != null) {
				this.wsdlAvailable = true;
				wsdlStream.close();
			}
			
			// set the VDBStream correctly
			this.vdbFile = vdbFile;
			
			// Right now all the Modelinfo is not in the DEF file; they are
			// scrapped from manifest and def file together. 
			if (this.def == null) {
				this.def = manifest.getVDB();
			} else {
				appendManifest(this.def, manifest);
			}
			
		} finally {
			archive.close();
		}
	}
	
	/**
	 * Since DEF file does not know if the model is PHYSICAL or not
	 * we need to scrape that information from the Manifest file.
	 * @param mydef
	 * @param manifest
	 */
	private void appendManifest(BasicVDBDefn mydef, Manifest manifest) {
		if (mydef == null || manifest == null) {
			return;
		}
		
		Collection<BasicModelInfo> models = mydef.getModels();
		
		BasicVDBDefn manifestVdb = manifest.getVDB();
		
		// if models defined n the def file; add them the manifest
		if (models == null || models.isEmpty()) {
			mydef.setModelInfos(manifestVdb.getModels());
		}

		// if they are defined, but incomplete them add that info.
		for(BasicModelInfo defModel:models) {
			ModelInfo manifestModel = manifestVdb.getModel(defModel.getName());
			if (manifestModel != null) {
				defModel.setModelType(manifestModel.getModelType());
				defModel.setPath(manifestModel.getPath());
				defModel.setUuid(manifestModel.getUUID());
				defModel.setModelURI(manifestModel.getModelURI());
			}
		}
	}
	
	private InputStream getStream(String wantedFile, ZipFile archive) throws IOException {
        Enumeration entries = archive.entries();
        while(entries.hasMoreElements()) {
            ZipEntry entry = (ZipEntry)entries.nextElement();
            if (entry != null && entry.getName().equalsIgnoreCase(wantedFile)) {
                return archive.getInputStream(entry);
            }
        }		
        return null;
	}
	
	private HashSet<String> getListOfEntries(ZipFile archive) throws IOException {
		HashSet<String> files = new HashSet<String>();
        Enumeration entries = archive.entries();
        while(entries.hasMoreElements()) {
            ZipEntry entry = (ZipEntry)entries.nextElement();
            files.add(entry.getName());
        }		
        return files;
	}
	
		
	private File getTempName(String ext) {
		File parent = new File(this.tempDirectory.getPath()+File.separator+"workspace");//$NON-NLS-1$
		parent.mkdirs();
		return new File(parent, Math.abs(RANDOM.nextLong())+"."+ext); //$NON-NLS-1$
	}
	
	private void open() {
		if (!open) {
			this.tempDirectory = new TempDirectory(FileUtils.TEMP_DIRECTORY+File.separator+"federate", System.currentTimeMillis(), RANDOM.nextLong()); //$NON-NLS-1$
			this.tempDirectory.create();
			open = true;
		}
	}	
	
	private void checkOpen() {
		if(!open) {
			throw new IllegalStateException("Archive already closed"); //$NON-NLS-1$
		}
	}
	
	/**
	 * Load the VDB from DEF file. The name of the VDB Archive is taken from the DEF
	 * contents. If the VDB file exists it will be loaded.
	 * 
	 * @param defStream - DEF file Stream; 
	 * @return VDBDefn
	 * @throws IOException 
	 */
	public static BasicVDBDefn readFromDef(InputStream defStream) throws IOException {
		DEFReaderWriter reader = new DEFReaderWriter();
		BasicVDBDefn vdbDefn = reader.read(defStream);
		return vdbDefn;
	}	
	
	/**
	 * Update the Configuration.def file, with supplied DEF object.
	 * @param vdbDef
	 * @throws IOException
	 */
	public void updateConfigurationDef(BasicVDBDefn vdbDef) throws IOException {
		if (vdbDef == null) {
			return;
		}
		
		File f = getTempName("xml"); //$NON-NLS-1$
		DEFReaderWriter writer = new DEFReaderWriter();
		
		FileOutputStream fos = new FileOutputStream(f);
		writer.write(fos, vdbDef, new Properties());
		fos.close();
		
		InputStream in = null;
		try {
			in = new FileInputStream(f);
			ZipFileUtil.replace(this.vdbFile, VdbConstants.DEF_FILE_NAME, in);
		} finally {
			in.close();
			f.delete();
		}
		
		// update the local copies.
		this.def = vdbDef;
		appendManifest(this.def, manifest);
	}	
	
	/**
	 * Update/Add the data roles file in the archive.
	 * @param roles - contents of the roles file
	 * @throws IOException
	 */
	public void updateRoles(char[] roles) throws IOException {
		if (roles != null && roles.length > 0) {
			checkOpen();
			
			File f = getTempName("bin"); //$NON-NLS-1$
			FileWriter fw = new FileWriter(f);	
			fw.write(roles);
			fw.close();
			
			InputStream in = null;
			try {
				in = new FileInputStream(f);		
				ZipFileUtil.replace(this.vdbFile, VdbConstants.DATA_ROLES_FILE, in);
			} finally {
				in.close();
				f.delete();
			}	
		}
	}

	
	/**
	 * Close the VDBArchive and do the cleanup. Once this call is made archive is no longer usable.
	 */
	public void close() {
		if (open) {
			File dir = new File(this.tempDirectory.getPath());
            FileUtils.removeChildrenRecursively(dir);
            dir.delete();
            open = false;
		}
	}	
	
	/**
	 * Get the parsed object for the Configuration.def file. 
	 * @return null if the ConfigurationInfo.def file not found in the archive
	 */
	public BasicVDBDefn getConfigurationDef() {
		checkOpen();
		return this.def;
	}

	
	/**
	 * Get the Data roles file if one is defined
	 * @return null is no data roles file is found in the archive.
	 */
	public char[] getDataRoles() {
		checkOpen();
		return dataRoles;
	}
	
	
	/**
	 * Get the stream object for the VDB. This object is only valid when the archive is
	 * still open. Once closed this stream becomes invalid.
	 * @return stream for the VDB contents.
	 */
	public InputStream getInputStream() throws IOException {
		checkOpen();
		return new FileInputStream(this.vdbFile);
	}
	
	/**
	 * Returns errors, otherwise null for no validity errors. 
	 * @return
	 */
	public String[] getVDBValidityErrors() {
		return this.manifest.getValidityErrors();
	}
	
	/**
	 * Write the VDBArchive to the given output stream
	 * @param out
	 */
	public void write(OutputStream out) throws IOException {
		checkOpen();
		FileUtils.write(this.vdbFile, out);
	}
	
	public String getName() {
		checkOpen();
		return this.def.getName();
	}
	
	public void setName(String name) {
		checkOpen();
		// TODO:all vdb specific information 
		// need to move into archive
		if (this.def != null) {
			this.def.setName(name);
		}
	}
	
	public String getVersion() {
		checkOpen();
		return this.def.getVersion();
	}
	
	public short getStatus() {
		checkOpen();
		return this.status;
	}
	
	public void setStatus(short status) {
		checkOpen();
		this.status = status;
	}	
	
	public Set<String> getEntries(){
		return new HashSet<String>(this.pathsInArchive);
	}
	
	public boolean isVisible(String pathInVdb) {
		
		// make sure this is one of ours
		if (this.pathsInArchive.contains(pathInVdb)) {
		
	        String entry = StringUtil.getLastToken(pathInVdb, "/"); //$NON-NLS-1$
	        // index files should not be visible
			if( entry.endsWith(VdbConstants.INDEX_EXT) || entry.endsWith(VdbConstants.SEARCH_INDEX_EXT)) {
				return true;
			}
	
			// manifest file should not be visible
	        if(entry.equalsIgnoreCase(VdbConstants.MANIFEST_MODEL_NAME)) {
	            return false;
	        }
	        
	        // materialization models should not be visible
	        if(entry.startsWith(VdbConstants.MATERIALIZATION_MODEL_NAME) && entry.endsWith(VdbConstants.MODEL_EXT)) {
	            return false;
	        }
	        
	        // ddl files for materialization should not be visible
	        if(ScriptType.isDDLScript(entry)) {
	            return false;
	        }
	        
	        // wldl file should be visible
	        if(entry.equalsIgnoreCase(VdbConstants.WSDL_FILENAME)) {
	            return true;
	        }     
	        // any other file should be visible
	        return true;
		}
		return false;
	}
	
    public static byte[] writeToByteArray(VDBArchive vdb) throws MetaMatrixComponentException {
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			vdb.write(bos);
			byte[] contents = bos.toByteArray();
			bos.close();
			return contents;
		} catch (Exception e) {
			throw new MetaMatrixComponentException(e);
		}
	}	
	
	public String toString() {
		return getName()+"_"+getVersion(); //$NON-NLS-1$
	}
}
