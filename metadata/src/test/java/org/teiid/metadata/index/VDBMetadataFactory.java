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

import javax.xml.bind.JAXBException;

import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.FileUtils;
import org.teiid.core.util.LRUCache;
import org.teiid.core.util.UnitTestUtil;
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
			return getVDBMetadata(new File(vdbFile).toURI().toURL(), null);
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		}
    }
	
	public static MetadataStore getSystem() {
		try {
			if (system == null) {
				system = loadMetadata(Thread.currentThread().getContextClassLoader().getResource(CoreConstants.SYSTEM_VDB)).getMetadataStore(null);
			}
			return system;
		} catch (Exception e) {
			throw new TeiidRuntimeException("System VDB not found");
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
			Collection<FunctionTree> trees = null;
			if (udfFile != null) {
				String schema = FileUtils.getFilenameWithoutExtension(udfFile.getPath());
				methods = FunctionMetadataReader.loadFunctionMethods(udfFile.openStream());
				trees = Arrays.asList(new FunctionTree(schema, new UDFSource(methods), true));
			}
			SystemFunctionManager sfm = new SystemFunctionManager();
			vdbmetadata = new TransformationMetadata(null, new CompositeMetadataStore(Arrays.asList(getSystem(), imf.getMetadataStore(getSystem().getDatatypes()))), imf.getEntriesPlusVisibilities(), sfm.getSystemFunctions(), trees); 
			VDB_CACHE.put(vdbURL, vdbmetadata);
			return vdbmetadata;
		} catch (URISyntaxException e) {
			throw new IOException(e);
		} catch (JAXBException e) {
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
