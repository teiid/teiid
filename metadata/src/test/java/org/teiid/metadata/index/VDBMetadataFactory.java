/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.teiid.metadata.index;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;

import javax.xml.stream.XMLStreamException;

import org.jboss.vfs.VirtualFile;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.FileUtils;
import org.teiid.core.util.LRUCache;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.function.UDFSource;
import org.teiid.query.function.metadata.FunctionMetadataReader;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.PureZipFileSystem;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.VDBResources;
import org.teiid.query.metadata.VDBResources.Resource;


public class VDBMetadataFactory {
	
	public static LRUCache<URL, TransformationMetadata> VDB_CACHE = new LRUCache<URL, TransformationMetadata>(10);
	
	public static class IndexVDB {
		public MetadataStore store;
		public VDBResources resources;
	}
	
	public static TransformationMetadata getVDBMetadata(String vdbFile) {
		try {
			File f = new File(vdbFile);
			return getVDBMetadata(f.getName(), f.toURI().toURL(), null);
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		}
    }
	
	public static TransformationMetadata getVDBMetadata(String vdbName, URL vdbURL, URL udfFile) throws IOException {
		TransformationMetadata vdbmetadata = VDB_CACHE.get(vdbURL);
		if (vdbmetadata != null) {
			return vdbmetadata;
		}

		try {
			IndexVDB imf = loadMetadata(vdbName, vdbURL);
			Resource r = imf.resources.getEntriesPlusVisibilities().get("/META-INF/vdb.xml");
			VDBMetaData vdb = null;
			if (r != null) {
				vdb = VDBMetadataParser.unmarshell(r.openStream());
			}
			Collection <FunctionMethod> methods = null;
			Collection<FunctionTree> trees = null;
			if (udfFile != null) {
				String schema = FileUtils.getFilenameWithoutExtension(udfFile.getPath());
				methods = FunctionMetadataReader.loadFunctionMethods(udfFile.openStream());
				trees = Arrays.asList(new FunctionTree(schema, new UDFSource(methods), true));
			}
			SystemFunctionManager sfm = new SystemFunctionManager();
			vdbmetadata = new TransformationMetadata(vdb, new CompositeMetadataStore(Arrays.asList(SystemMetadata.getInstance().getSystemStore(), imf.store)), imf.resources.getEntriesPlusVisibilities(), sfm.getSystemFunctions(), trees); 
			VDB_CACHE.put(vdbURL, vdbmetadata);
			return vdbmetadata;
		} catch (XMLStreamException e) {
			throw new IOException(e);
		}
    }

	public static IndexVDB loadMetadata(String vdbName, URL url) throws IOException, MalformedURLException {
		VirtualFile root;
		try {
			root = PureZipFileSystem.mount(url);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
    	IndexVDB result = new IndexVDB();
    	result.resources = new VDBResources(root, null);
    	IndexMetadataRepository store =  new IndexMetadataRepository();
    	result.store = store.load(SystemMetadata.getInstance().getDataTypes(), result.resources);
    	return result;
	}
}
