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
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.jboss.virtual.VFS;
import org.jboss.virtual.VirtualFile;
import org.jboss.virtual.VirtualFileFilter;
import org.jboss.virtual.plugins.context.zip.ZipEntryContext;
import org.teiid.metadata.CompositeMetadataStore;
import org.teiid.metadata.TransformationMetadata;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.query.function.metadata.FunctionMetadataReader;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.metadata.QueryMetadataInterface;

public class VDBMetadataFactory {
	
	public static LRUCache<URL, QueryMetadataInterface> VDB_CACHE = new LRUCache<URL, QueryMetadataInterface>(10);
	
	public static QueryMetadataInterface getVDBMetadata(String vdbFile) {
		try {
			return getVDBMetadata(new File(vdbFile).toURI().toURL(), null);
		} catch (IOException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }
	
	public static QueryMetadataInterface getVDBMetadata(URL vdbURL, URL udfFile) throws IOException {
		QueryMetadataInterface vdbmetadata = VDB_CACHE.get(vdbURL);
		if (vdbmetadata != null) {
			return vdbmetadata;
		}

		try {
			VFS.init();
			VDBContext vdbContext = new VDBContext(vdbURL);
			VirtualFile vdbFile = new VirtualFile(vdbContext.getRoot());
			
			List<VirtualFile> children = vdbFile.getChildrenRecursively(new VirtualFileFilter() {
				@Override
				public boolean accepts(VirtualFile file) {
					return file.getName().endsWith(IndexConstants.NAME_DELIM_CHAR+IndexConstants.INDEX_EXT);
				}
			});
			
			IndexMetadataFactory imf = new IndexMetadataFactory();
			for (VirtualFile f: children) {
				imf.addIndexFile(f);
			}
			
			Collection <FunctionMethod> methods = null;
			if (udfFile != null) {
				methods = FunctionMetadataReader.loadFunctionMethods(udfFile.openStream());
			}
			
			vdbmetadata = new TransformationMetadata(null, new CompositeMetadataStore(Arrays.asList(imf.getMetadataStore())), null, methods);
			VDB_CACHE.put(vdbURL, vdbmetadata);
			return vdbmetadata;
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
    }	
	

	private static class VDBContext extends ZipEntryContext{
		private static final long serialVersionUID = -6504988258841073415L;

		protected VDBContext(URL url) throws IOException, URISyntaxException {
			super(url,false);
		}
	}
}
