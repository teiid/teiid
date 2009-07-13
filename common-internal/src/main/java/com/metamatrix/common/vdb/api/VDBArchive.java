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

package com.metamatrix.common.vdb.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.util.TempDirectory;
import com.metamatrix.core.util.ZipFileUtil;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.core.vdb.VdbConstants;
import com.metamatrix.metadata.runtime.api.MetadataSource;
import com.metamatrix.vdb.materialization.ScriptType;
import com.metamatrix.vdb.runtime.BasicModelInfo;
import com.metamatrix.vdb.runtime.BasicVDBDefn;

/**
 * Latest incarnation of the VDBContext, specifically for weeding out 
 * dependencies on the vdb.edit; So do put in Modeler dependencies here.
 */
public class VDBArchive implements MetadataSource {
	
	public static String USE_CONNECTOR_METADATA = "UseConnectorMetadata"; //$NON-NLS-1$
	public static String CACHED = "CACHED"; //$NON-NLS-1$
	
	// configuration def contents
	private BasicVDBDefn def;
	
	// data roles contents
	private char[] dataRoles;
	
	private Manifest manifest;
	
	private boolean wsdlAvailable;
	
	// this in the underlying VDB file holding the archive, only set in temp mode
	private File vdbFile;
	private ZipFile archive;
	private boolean tempVDB;
	
	// the full deploy directory for the extracted archive
	private File deployDirectory;
	private TempDirectory tempDir;
	
	private Set<String> pathsInArchive = new HashSet<String>();
	private boolean open;
		
	public static VDBArchive loadVDB(URL vdbURL, File deployDirectory) throws IOException {
		boolean loadedFromDef = false;
    	BasicVDBDefn def = null;
    	String vdblocation = vdbURL.toString().toLowerCase();
    	boolean defOnly = false;
        if (vdblocation.endsWith(VdbConstants.DEF)) {
        	loadedFromDef = true;
        	def = VDBArchive.readFromDef(vdbURL.openStream());
        	
            String vdbName = def.getFileName();
            
            if (vdbName == null) {
            	defOnly = true;
            } else {
            	vdbURL = URLHelper.buildURL(vdbURL, vdbName);
            }
        } 
        
        File tempArchive = null;

        if (!defOnly) {
        	tempArchive = createTempVDB(vdbURL.openStream());
            ZipFile vdb = new ZipFile(tempArchive);
            if (def == null) {
            	InputStream defStream = getStream(VdbConstants.DEF_FILE_NAME, vdb);
            	if (defStream != null) {
            		def = readFromDef(defStream);
                	defStream.close();
            	}
            }
            vdb.close();
        } 

        if (def == null) {
    		throw new IllegalArgumentException("No ConfigurationInfo.def file associated with vdb " + vdbURL); //$NON-NLS-1$
        }
        
    	deployDirectory = new File(deployDirectory, def.getName().toLowerCase() + "/" + def.getVersion().toLowerCase()); //$NON-NLS-1$
        
        boolean firstExtraction = false;
        if (!deployDirectory.exists()) {
        	deployDirectory.mkdirs();
        	firstExtraction = true;
        	if (tempArchive != null) {
        		ZipFileUtil.extract(tempArchive.getAbsolutePath(), deployDirectory.getAbsolutePath());
        	}
        } 
		
        if (tempArchive != null) {
        	tempArchive.delete();
        }
        
        VDBArchive result = new VDBArchive();
        result.deployDirectory = deployDirectory;
        if (loadedFromDef && firstExtraction) {
        	result.updateConfigurationDef(def);
        }
        result.load();
        return result;
	}
	
	private static File createTempVDB(InputStream is) throws IOException {
		File tempArchive = File.createTempFile("teiid", ".vdb"); //$NON-NLS-1$ //$NON-NLS-2$
        tempArchive.deleteOnExit();
        if (is != null) {
        	FileUtils.write(is, tempArchive);
        }
		return tempArchive;
	}
	
	private VDBArchive() {
	}
	
	/**
	 * Build VDB archive from given stream. Note that any updates on the archive object will reflect
	 * in the temporary file that this object manipulates. 
	 * 
	 * @param vdbStream
	 * @return
	 * @throws IOException
	 */
	public VDBArchive(InputStream vdbStream) throws IOException {
		this.tempVDB = true;
		loadFromFile(createTempVDB(vdbStream));
	}
		
	/**
	 * Build a VDB archive from the given zip file.
	 * @param vdb
	 * @throws IOException
	 */
	public VDBArchive(File vdb) throws IOException {
		loadFromFile(vdb);
	}

	private void loadFromFile(File vdb) throws ZipException, IOException {
		this.vdbFile = vdb;
		this.archive = new ZipFile(this.vdbFile);
		this.tempDir = TempDirectory.getTempDirectory(null);
		this.deployDirectory = new File(this.tempDir.getPath());
		ZipFileUtil.extract(vdbFile.getAbsolutePath(), this.deployDirectory.getAbsolutePath());
		try {
			load();
		} finally {
			this.archive.close();
		}
	}
	
	/**
	 * Load a VDB archive file, loads the DEF, Data Roles and Manifest file.
	 * @param vdbFile - archive with contained DEF file
	 * @return BasicVDBDefn
	 * @throws IOException
	 */
	private void load() throws IOException{
		this.pathsInArchive = Collections.unmodifiableSet(getListOfEntries());
		
		// check if manifest file is available then load it.
		InputStream manifestStream = getStream(VdbConstants.MANIFEST_MODEL_NAME);
		if (manifestStream != null) {
			this.manifest = new Manifest();
			this.manifest.load(manifestStream);
		}			
		
		// get DEF file from zip and load it
		InputStream defFile = getStream(VdbConstants.DEF_FILE_NAME);
		if (defFile != null) {
			try {
				this.def = readFromDef(defFile);
			} finally {
				defFile.close();
			}
		}
		
		// check if the data roles file is available, if found load it.
		InputStream rolesFile = getStream(VdbConstants.DATA_ROLES_FILE);
		if (rolesFile != null) {
			try {
				this.dataRoles = FileUtil.read(new InputStreamReader(rolesFile)).toCharArray();
			} finally {
				rolesFile.close();
			}
		}
		
		// check if WSDL is defined
		InputStream wsdlStream = getStream(VdbConstants.WSDL_FILENAME);
		if (wsdlStream != null) {
			this.wsdlAvailable = true;
			wsdlStream.close();
		}
		
		// Right now all the Modelinfo is not in the DEF file; they are
		// scrapped from manifest and def file together. 
		if (this.def == null) {
			if (this.manifest != null) {
				this.def = manifest.getVDB();
			}
		} else {
			appendManifest(this.def);
		}
		if (this.def != null) {
			this.def.setHasWSDLDefined(this.wsdlAvailable);
		}
		open = true;
	}
	
	@Override
	public Set<String> getConnectorMetadataModelNames() {
		if (this.def.getInfoProperties() != null && 
				(cacheConnectorMetadata() || PropertiesUtils.getBooleanProperty(this.def.getInfoProperties(), USE_CONNECTOR_METADATA, false))) {
			return new HashSet<String>(this.def.getModelNames());
		}
		return Collections.emptySet();
	}
	
	public boolean cacheConnectorMetadata() {
		if (this.def.getInfoProperties() == null) {
			return false;
		}
		return CACHED.equalsIgnoreCase(this.def.getInfoProperties().getProperty(USE_CONNECTOR_METADATA));
	}
	
	@Override
	public void saveFile(InputStream is, String path) throws IOException {
		FileUtils.write(is, new File(this.deployDirectory, path));
	}
	
	private InputStream getStream(String path) throws IOException {
		File f = new File(this.deployDirectory, path);
		if (!f.exists()) {
			return null;
		}
		return new FileInputStream(f);
	}
	
	public File getDeployDirectory() {
		return deployDirectory;
	}
	
	/**
	 * Since DEF file does not know if the model is PHYSICAL or not
	 * we need to scrape that information from the Manifest file.
	 * @param mydef
	 * @param manifest
	 */
	private void appendManifest(BasicVDBDefn mydef) {
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
	
	private static InputStream getStream(String wantedFile, ZipFile archive) throws IOException {
        Enumeration entries = archive.entries();
        while(entries.hasMoreElements()) {
            ZipEntry entry = (ZipEntry)entries.nextElement();
            if (entry != null && entry.getName().equalsIgnoreCase(wantedFile)) {
                return archive.getInputStream(entry);
            }
        }		
        return null;
	}
	
	private HashSet<String> getListOfEntries() {
		HashSet<String> files = new HashSet<String>();
		File[] allFiles = FileUtils.findAllFilesInDirectoryRecursively(deployDirectory.getAbsolutePath());
		int length = deployDirectory.getAbsolutePath().length();
		for (File file : allFiles) {
			files.add(file.getAbsolutePath().substring(length));
		}
		return files;
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
		
		DEFReaderWriter writer = new DEFReaderWriter();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		writer.write(baos, vdbDef, new Properties());
		baos.close();

		if (this.vdbFile != null) {
			ZipFileUtil.replace(this.vdbFile, VdbConstants.DEF_FILE_NAME, new ByteArrayInputStream(baos.toByteArray()));
		} 
		FileUtils.write(baos.toByteArray(), new File(this.deployDirectory, VdbConstants.DEF_FILE_NAME));	
		
		// update the local copies.
		this.def = vdbDef;
		appendManifest(this.def);
	}	
	
	/**
	 * Update/Add the data roles file in the archive.
	 * @param roles - contents of the roles file
	 * @throws IOException
	 */
	public void updateRoles(char[] roles) throws IOException {
		if (roles != null && roles.length > 0) {
			checkOpen();
			
			if (this.vdbFile != null) {
				ZipFileUtil.replace(this.vdbFile, VdbConstants.DATA_ROLES_FILE, ObjectConverterUtil.convertToInputStream(roles));
			} 
			FileUtils.write(ObjectConverterUtil.convertToInputStream(roles), new File(this.deployDirectory, VdbConstants.DATA_ROLES_FILE));	
		}
	}

	
	/**
	 * Close the VDBArchive and do the cleanup. Once this call is made archive is no longer usable.
	 */
	public void close() {
		if (open) {
			if (this.tempVDB && this.vdbFile != null) {
				this.vdbFile.delete();
			}
			if (this.tempDir != null) {
				this.tempDir.remove();
			}
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
	 * Returns errors, otherwise null for no validity errors. 
	 * @return
	 */
	public String[] getVDBValidityErrors() {
		if (this.manifest != null) {
			return this.manifest.getValidityErrors();
		}
		return null;
	}
	
	/**
	 * Write the VDBArchive to the given output stream
	 * @param out
	 */
	public void write(OutputStream out) throws IOException {
		checkOpen();
		if (this.vdbFile != null) {
			FileUtils.write(this.vdbFile, out);
		} else {
			File vdb = createTempVDB(null);
			ZipFileUtil.addAll(vdb, this.deployDirectory.getAbsolutePath(), ""); //$NON-NLS-1$
			FileUtils.write(vdb, out);
		}
	}
	
	public String getName() {
		checkOpen();
		return this.def.getName();
	}
	
	public void setName(String name) {
		checkOpen();
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
		if (getVDBValidityErrors() != null && getVDBValidityErrors().length > 0) {
			return VDBStatus.INCOMPLETE;
		}
		return this.def.getStatus();
	}
	
	public void setStatus(short status) {
		checkOpen();
		this.def.setStatus(status);
	}	
	
	public Set<String> getEntries(){
		return this.pathsInArchive;
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

	@Override
	public File getFile(String path) {
		File f = new File(this.deployDirectory, path);
		if (f.exists()) {
			return f;
		}
		return null;
	}
	
	@Override
	public ModelInfo getModelInfo(String name) {
		if (this.def == null) {
			return null;
		}
		return this.def.getModel(name);
	}

}
