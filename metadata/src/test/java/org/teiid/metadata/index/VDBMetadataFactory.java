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
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executors;

import javax.xml.stream.XMLStreamException;

import org.jboss.vfs.TempFileProvider;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.FileUtils;
import org.teiid.core.util.LRUCache;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.function.UDFSource;
import org.teiid.query.function.metadata.FunctionMetadataReader;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;


@SuppressWarnings("nls")
public class VDBMetadataFactory {
	
	public static LRUCache<URL, TransformationMetadata> VDB_CACHE = new LRUCache<URL, TransformationMetadata>(10);
	private static MetadataStore system;
	
	public static TransformationMetadata getVDBMetadata(String vdbFile) {
		try {
			File f = new File(vdbFile);
			return getVDBMetadata(f.getName(), f.toURI().toURL(), null);
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		}
    }
	
	public static MetadataStore getSystem() {
		try {
			if (system == null) {
				system=  loadMetadata( CoreConstants.SYSTEM_VDB, Thread.currentThread().getContextClassLoader().getResource(CoreConstants.SYSTEM_VDB));
			}
			return system;
		} catch (Exception e) {
			throw new TeiidRuntimeException("System VDB not found");
		}
    }
	
	public static TransformationMetadata getVDBMetadata(String vdbName, URL vdbURL, URL udfFile) throws IOException {
		TransformationMetadata vdbmetadata = VDB_CACHE.get(vdbURL);
		if (vdbmetadata != null) {
			return vdbmetadata;
		}

		try {
			IndexMetadataStore imf = loadMetadata(vdbName, vdbURL, getSystem().getDatatypes().values());
			
			Collection <FunctionMethod> methods = null;
			Collection<FunctionTree> trees = null;
			if (udfFile != null) {
				String schema = FileUtils.getFilenameWithoutExtension(udfFile.getPath());
				methods = FunctionMetadataReader.loadFunctionMethods(udfFile.openStream());
				trees = Arrays.asList(new FunctionTree(schema, new UDFSource(methods), true));
			}
			SystemFunctionManager sfm = new SystemFunctionManager();
			vdbmetadata = new TransformationMetadata(null, new CompositeMetadataStore(Arrays.asList(getSystem(), imf)), imf.getEntriesPlusVisibilities(), sfm.getSystemFunctions(), trees); 
			VDB_CACHE.put(vdbURL, vdbmetadata);
			return vdbmetadata;
		} catch (URISyntaxException e) {
			throw new IOException(e);
		} catch (XMLStreamException e) {
			throw new IOException(e);
		}
    }

	public static IndexMetadataStore loadMetadata(String vdbName, URL url, Collection<Datatype> dataTypes) throws IOException, MalformedURLException, URISyntaxException {
    	VirtualFile root = VFS.getChild(vdbName);
    	if (!root.exists()) {
    		VFS.mountZip(url.openStream(), vdbName, root, TempFileProvider.create("vdbs", Executors.newScheduledThreadPool(2)));
    		// once done this mount should be closed, since this class is only used testing
    		// it is hard to event when the test is done, otherwise we need to elevate the VFS to top
    	}
    	IndexMetadataStore store =  new IndexMetadataStore(root);
    	store.load(dataTypes);
    	return store;

	}	
	
	public static IndexMetadataStore loadMetadata(String vdbName, URL url) throws IOException, MalformedURLException, URISyntaxException {
		return loadMetadata(vdbName, url, null);
	}
}
