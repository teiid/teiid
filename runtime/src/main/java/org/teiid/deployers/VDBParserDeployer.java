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
package org.teiid.deployers;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.deployer.managed.ManagedObjectCreator;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.jboss.deployers.vfs.spi.structure.VFSDeploymentUnit;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.factory.ManagedObjectFactory;
import org.jboss.virtual.VirtualFile;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.VdbConstants;
import org.teiid.metadata.index.IndexConstants;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.runtime.RuntimePlugin;
import org.xml.sax.SAXException;


/**
 * This file loads the "vdb.xml" file inside a ".vdb" file, along with all the metadata in the .INDEX files
 */
public class VDBParserDeployer extends BaseMultipleVFSParsingDeployer<VDBMetaData> implements ManagedObjectCreator {
	private ObjectSerializer serializer;
	private VDBRepository vdbRepository;
	 
	public VDBParserDeployer() {
		super(VDBMetaData.class, getCustomMappings(), IndexConstants.NAME_DELIM_CHAR+IndexConstants.INDEX_EXT, IndexMetadataFactory.class, VdbConstants.MODEL_EXT, UDFMetaData.class);
		setAllowMultipleFiles(true);
	}

	private static Map<String, Class<?>> getCustomMappings() {
		Map<String, Class<?>> mappings = new HashMap<String, Class<?>>();
		mappings.put(VdbConstants.DEPLOYMENT_FILE, VDBMetaData.class);
		// this not required but the to make the framework with extended classes 
		// this required otherwise different version of parse is invoked.
		mappings.put("undefined", UDFMetaData.class); //$NON-NLS-1$
		return mappings;
	}
	
	@Override
	protected <U> U parse(VFSDeploymentUnit unit, Class<U> expectedType, VirtualFile file, Object root) throws Exception {
		if (expectedType.equals(VDBMetaData.class)) {
			Unmarshaller un = getUnMarsheller();
			VDBMetaData def = (VDBMetaData)un.unmarshal(file.openStream());
			
			return expectedType.cast(def);
		}
		else if (expectedType.equals(UDFMetaData.class)) {
			if (root == null) {
				root = unit.getAttachment(UDFMetaData.class);
				if (root == null) {
					root = new UDFMetaData();
					unit.addAttachment(UDFMetaData.class, UDFMetaData.class.cast(root));
				}
			}
			UDFMetaData udf = UDFMetaData.class.cast(root);		
			udf.addModelFile(file);
			
			return expectedType.cast(udf);
		}		
		else if (expectedType.equals(IndexMetadataFactory.class)) {
			if (root == null) {
				root = unit.getAttachment(IndexMetadataFactory.class);
				if (root == null) {
					root = new IndexMetadataFactory();
				}
			}
			IndexMetadataFactory imf = IndexMetadataFactory.class.cast(root);
			imf.addIndexFile(file);
			unit.addAttachment(IndexMetadataFactory.class, imf);
			return expectedType.cast(imf);
		}
		else {
			throw new IllegalArgumentException("Cannot match arguments: expectedClass=" + expectedType ); //$NON-NLS-1$
		}		
	}

	static Unmarshaller getUnMarsheller() throws JAXBException, SAXException {
		JAXBContext jc = JAXBContext.newInstance(new Class<?>[] {VDBMetaData.class});
		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Schema schema = schemaFactory.newSchema(VDBMetaData.class.getResource("/vdb-deployer.xsd")); //$NON-NLS-1$
		Unmarshaller un = jc.createUnmarshaller();
		un.setSchema(schema);
		return un;
	}
	
	@Override
	protected VDBMetaData mergeMetaData(VFSDeploymentUnit unit, Map<Class<?>, List<Object>> metadata) throws Exception {
		VDBMetaData vdb = getInstance(metadata, VDBMetaData.class);
		UDFMetaData udf = getInstance(metadata, UDFMetaData.class);
		IndexMetadataFactory imf = getInstance(metadata, IndexMetadataFactory.class);
		
		if (vdb == null) {
			LogManager.logError(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("invlaid_vdb_file",unit.getRoot().getName())); //$NON-NLS-1$
			return null;
		}
		
		vdb.setUrl(unit.getRoot().toURL());		
		
		// build the metadata store
		if (imf != null) {
			imf.addEntriesPlusVisibilities(unit.getRoot(), vdb);
			unit.addAttachment(IndexMetadataFactory.class, imf);
							
			// add the cached store.
			File cacheFile = VDBDeployer.buildCachedVDBFileName(this.serializer, unit, vdb);
			// check to see if the vdb has been modified when server is down; if it is then clear the old files
			if (this.serializer.isStale(cacheFile, unit.getRoot().getLastModified())) {
				this.serializer.removeAttachments(unit);
				LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB", unit.getRoot().getName(), "old cached metadata has been removed"); //$NON-NLS-1$ //$NON-NLS-2$				
			}
			MetadataStoreGroup stores = this.serializer.loadSafe(cacheFile, MetadataStoreGroup.class);
			if (stores == null) {				
				// start to build the new metadata 
				stores = new MetadataStoreGroup();
				stores.addStore(imf.getMetadataStore(vdbRepository.getSystemStore().getDatatypes()));
			}
			else {
				LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB", unit.getRoot().getName(), "was loaded from cached metadata"); //$NON-NLS-1$ //$NON-NLS-2$
			}
			unit.addAttachment(MetadataStoreGroup.class, stores);				
		}
		
		if (udf != null) {
			// load the UDF
			for(Model model:vdb.getModels()) {
				if (model.getModelType().equals(Model.Type.FUNCTION)) {
					String path = ((ModelMetaData)model).getPath();
					if (path == null) {
						throw new DeploymentException(RuntimePlugin.Util.getString("invalid_udf_file", model.getName())); //$NON-NLS-1$
					}
					udf.buildFunctionModelFile(path);
				}
			}		
			
			// If the UDF file is enclosed then attach it to the deployment artifact
			unit.addAttachment(UDFMetaData.class, udf);
		}
				
		LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB", unit.getRoot().getName(), "has been parsed."); //$NON-NLS-1$ //$NON-NLS-2$
		return vdb;
	}
	
	public void setVdbRepository(VDBRepository vdbRepository) {
		this.vdbRepository = vdbRepository;
	}
	
	public void setObjectSerializer(ObjectSerializer serializer) {
		this.serializer = serializer;
	}		
	
	private ManagedObjectFactory mof;
	
	@Override
	public void build(DeploymentUnit unit, Set<String> attachmentNames, Map<String, ManagedObject> managedObjects)
		throws DeploymentException {
	          
		ManagedObject vdbMO = managedObjects.get(VDBMetaData.class.getName());
		if (vdbMO != null) {
			VDBMetaData vdb = (VDBMetaData) vdbMO.getAttachment();
			for (Model m : vdb.getModels()) {
				ManagedObject mo = this.mof.initManagedObject(m, ModelMetaData.class, m.getName(),m.getName());
				if (mo == null) {
					throw new DeploymentException("could not create managed object"); //$NON-NLS-1$
				}
				managedObjects.put(mo.getName(), mo);
			}
			
			for (Translator t: vdb.getOverrideTranslators()) {
				ManagedObject mo = this.mof.initManagedObject(t, VDBTranslatorMetaData.class, t.getName(), t.getName());
				if (mo == null) {
					throw new DeploymentException("could not create managed object"); //$NON-NLS-1$
				}
				managedObjects.put(mo.getName(), mo);				
			}
		}
	}	
	
	public void setManagedObjectFactory(ManagedObjectFactory mof) {
		this.mof = mof;
	}
	
}
