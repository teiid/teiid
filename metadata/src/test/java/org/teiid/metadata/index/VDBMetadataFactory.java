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
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;

import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.metadata.CompositeMetadataStore;
import org.teiid.metadata.TransformationMetadata;

import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.query.function.metadata.FunctionMetadataReader;
import com.metamatrix.query.function.metadata.FunctionMethod;

@SuppressWarnings("nls")
public class VDBMetadataFactory {
	
	public static LRUCache<URL, TransformationMetadata> VDB_CACHE = new LRUCache<URL, TransformationMetadata>(10);
	
	public static TransformationMetadata getVDBMetadata(String vdbFile) {
		try {
			return getVDBMetadata(new File(vdbFile).toURI().toURL(), null);
		} catch (IOException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }
	
	public static MetadataStore getSystemVDBMetadataStore() {
		try {
			IndexMetadataFactory imf = loadMetadata(Thread.currentThread().getContextClassLoader().getResource(CoreConstants.SYSTEM_VDB));
			return imf.getMetadataStore();
		} catch (Exception e) {
			throw new MetaMatrixRuntimeException("System VDB not found");
		}
    }
	
	public static TransformationMetadata getVDBMetadata(URL vdbURL, URL udfFile) throws IOException {
		TransformationMetadata vdbmetadata = VDB_CACHE.get(vdbURL);
		if (vdbmetadata != null) {
			return vdbmetadata;
		}

		try {
			IndexMetadataFactory imf = loadMetadata(vdbURL);
			
			Collection <FunctionMethod> methods = null;
			if (udfFile != null) {
				methods = FunctionMetadataReader.loadFunctionMethods(udfFile.openStream());
			}
			MetadataStore system = loadMetadata(Thread.currentThread().getContextClassLoader().getResource(CoreConstants.SYSTEM_VDB)).getMetadataStore();
			vdbmetadata = new TransformationMetadata(null, new CompositeMetadataStore(Arrays.asList(system, imf.getMetadataStore())), imf.getEntriesPlusVisibilities(), methods); 
			VDB_CACHE.put(vdbURL, vdbmetadata);
			return vdbmetadata;
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
    }

	public static IndexMetadataFactory loadMetadata(URL vdbURL)
			throws IOException, MalformedURLException, URISyntaxException {
		//vfs has a problem with vdbs embedded in jars in the classpath, so we'll create a temp version
		if (vdbURL.getProtocol().equals("jar")) {
			InputStream is = vdbURL.openStream();
			File temp = File.createTempFile("temp", ".vdb", new File(UnitTestUtil.getTestScratchPath()));
			temp.deleteOnExit();
			FileUtils.write(is, temp);
			vdbURL = temp.toURI().toURL();
		}
		return new IndexMetadataFactory(vdbURL);
	}

}
