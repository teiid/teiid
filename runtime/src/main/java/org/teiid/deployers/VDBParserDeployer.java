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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.jboss.deployers.vfs.spi.structure.VFSDeploymentUnit;
import org.jboss.logging.Logger;
import org.jboss.virtual.VirtualFile;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.metadata.CompositeMetadataStore;
import org.teiid.metadata.index.IndexConstants;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.xml.sax.SAXException;

import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.vdb.VdbConstants;
import com.metamatrix.query.function.metadata.FunctionMetadataReader;

/**
 * This file loads the "vdb.xml" file inside a ".vdb" file, along with all the metadata in the .INDEX files
 */
public class VDBParserDeployer extends BaseMultipleVFSParsingDeployer<VDBMetaData> {
	protected Logger log = Logger.getLogger(getClass());
	private ObjectSerializer serializer;
	 
	public VDBParserDeployer() {
		super(VDBMetaData.class, getCustomMappings(), IndexConstants.NAME_DELIM_CHAR+IndexConstants.INDEX_EXT, IndexMetadataFactory.class);
		setAllowMultipleFiles(true);
	}

	private static Map<String, Class<?>> getCustomMappings() {
		Map<String, Class<?>> mappings = new HashMap<String, Class<?>>();
		mappings.put(VdbConstants.DEPLOYMENT_FILE, VDBMetaData.class);
		mappings.put(VdbConstants.UDF_FILE_NAME, UDFMetaData.class);
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
			UDFMetaData udf = new UDFMetaData(FunctionMetadataReader.loadFunctionMethods(file.openStream()));
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
			throw new IllegalArgumentException("Cannot match arguments: expectedClass=" + expectedType );
		}		
	}

	static Unmarshaller getUnMarsheller() throws JAXBException, SAXException {
		JAXBContext jc = JAXBContext.newInstance(new Class<?>[] {VDBMetaData.class});
		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Schema schema = schemaFactory.newSchema(VDBMetaData.class.getResource("/vdb-deployer.xsd")); 		
		Unmarshaller un = jc.createUnmarshaller();
		un.setSchema(schema);
		return un;
	}
	
	@Override
	protected VDBMetaData mergeMetaData(VFSDeploymentUnit unit, Map<Class<?>, List<Object>> metadata) throws Exception {
		VDBMetaData def = getInstance(metadata, VDBMetaData.class);
		UDFMetaData udf = getInstance(metadata, UDFMetaData.class);
		
		if (def == null) {
			log.error("Invalid VDB file deployment failed ="+unit.getRoot().getName());
			return null;
		}
		
		def.setUrl(unit.getRoot().toURL().toExternalForm());		
		
		// add the entries and determine their visibility
		VirtualFile root = unit.getRoot();
		List<VirtualFile> children = root.getChildrenRecursively();
		Map<VirtualFile, Boolean> visibilityMap = new LinkedHashMap<VirtualFile, Boolean>();
		for(VirtualFile f: children) {
			visibilityMap.put(f, isFileVisible(f.getPathName(), def));
		}
		
		// build the metadata store
		List<Object> indexFiles = metadata.get(IndexMetadataFactory.class);
		if (indexFiles != null && !indexFiles.isEmpty()) {
			IndexMetadataFactory imf = (IndexMetadataFactory)indexFiles.get(0);
			if (imf != null) {
				imf.addEntriesPlusVisibilities(visibilityMap);
				unit.addAttachment(IndexMetadataFactory.class, imf);
				
				// add the cached store.
				CompositeMetadataStore store = null;
				File cacheFileName = this.serializer.getAttachmentPath(unit, def.getName()+"_"+def.getVersion());
				if (cacheFileName.exists()) {
					store = this.serializer.loadAttachment(cacheFileName, CompositeMetadataStore.class);
				}
				else {
					store = new CompositeMetadataStore(imf.getMetadataStore());
				}
				unit.addAttachment(CompositeMetadataStore.class, store);				
			}
		}
		
		// If the UDF file is enclosed then attach it to the deployment artifact
		if (udf != null) {
			unit.addAttachment(UDFMetaData.class, udf);
		}
		
		log.debug("VDB "+unit.getRoot().getName()+" has been parsed.");
		return def;
	}

	private final static boolean isSystemModelWithSystemTableType(String modelName) {
        return CoreConstants.SYSTEM_MODEL.equalsIgnoreCase(modelName);
    }
	
	private boolean isFileVisible(String pathInVDB, VDBMetaData vdb) {

		String modelName = StringUtil.getFirstToken(StringUtil.getLastToken(pathInVDB, "/"), "."); //$NON-NLS-1$ //$NON-NLS-2$

		// If this is any of the Public System Models, like JDBC,ODBC system
		// models
		if (isSystemModelWithSystemTableType(modelName)) {
			return true;
		}

		ModelMetaData model = vdb.getModel(modelName);
		if (model != null) {
			return model.isVisible();
		}

        String entry = StringUtil.getLastToken(pathInVDB, "/"); //$NON-NLS-1$
        
        // index files should not be visible
		if( entry.endsWith(VdbConstants.INDEX_EXT) || entry.endsWith(VdbConstants.SEARCH_INDEX_EXT)) {
			return false;
		}

		// deployment file should not be visible
        if(entry.equalsIgnoreCase(VdbConstants.DEPLOYMENT_FILE)) {
            return false;
        }
        
        // any other file should be visible
        return true;		
	}	
	
	public void setObjectSerializer(ObjectSerializer serializer) {
		this.serializer = serializer;
	}		
}
