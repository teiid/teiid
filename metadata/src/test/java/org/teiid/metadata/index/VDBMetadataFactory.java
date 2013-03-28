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

package org.teiid.metadata.index;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.xml.stream.XMLStreamException;

import org.jboss.vfs.TempFileProvider;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
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
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.VDBResources;


@SuppressWarnings("nls")
public class VDBMetadataFactory {
	
	public static LRUCache<URL, TransformationMetadata> VDB_CACHE = new LRUCache<URL, TransformationMetadata>(10);
	private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
	
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
			
			Collection <FunctionMethod> methods = null;
			Collection<FunctionTree> trees = null;
			if (udfFile != null) {
				String schema = FileUtils.getFilenameWithoutExtension(udfFile.getPath());
				methods = FunctionMetadataReader.loadFunctionMethods(udfFile.openStream());
				trees = Arrays.asList(new FunctionTree(schema, new UDFSource(methods), true));
			}
			SystemFunctionManager sfm = new SystemFunctionManager();
			vdbmetadata = new TransformationMetadata(null, new CompositeMetadataStore(Arrays.asList(SystemMetadata.getInstance().getSystemStore(), imf.store)), imf.resources.getEntriesPlusVisibilities(), sfm.getSystemFunctions(), trees); 
			VDB_CACHE.put(vdbURL, vdbmetadata);
			return vdbmetadata;
		} catch (XMLStreamException e) {
			throw new IOException(e);
		}
    }

	public static IndexVDB loadMetadata(String vdbName, URL url) throws IOException, MalformedURLException {
		String fileName = String.valueOf(vdbName + "-" + url.toExternalForm().hashCode());
		VirtualFile root = VFS.getChild(fileName);
    	if (!root.exists()) {
    		VFS.mountZip(url.openStream(), fileName, root, TempFileProvider.create("vdbs", executor));
    		// once done this mount should be closed, since this class is only used testing
    		// it is hard to event when the test is done, otherwise we need to elevate the VFS to top
    	}
    	IndexVDB result = new IndexVDB();
    	result.resources = new VDBResources(root, null);
    	IndexMetadataRepository store =  new IndexMetadataRepository();
    	result.store = store.load(SystemMetadata.getInstance().getDataTypes(), result.resources);
    	return result;
	}
}
