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
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.metadata.CompositeMetadataStore;
import org.teiid.metadata.TransformationMetadata;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.metadata.runtime.api.MetadataSource;
import com.metamatrix.query.metadata.QueryMetadataInterface;

public class VDBMetadataFactory {
	
	public static LRUCache<URL, QueryMetadataInterface> VDB_CACHE = new LRUCache<URL, QueryMetadataInterface>(10);
	
	public static QueryMetadataInterface getVDBMetadata(String vdbFile) {
		try {
			return getVDBMetadata(new File(vdbFile).toURI().toURL());
		} catch (IOException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }
	
	public static QueryMetadataInterface getVDBMetadata(URL vdbURL) {
		QueryMetadataInterface vdb = VDB_CACHE.get(vdbURL);
		if (vdb != null) {
			return vdb;
		}
		try {
			MetadataSource source = new VDBArchive(vdbURL.openStream());
			IndexMetadataFactory selector = new IndexMetadataFactory(source);
	        vdb = new TransformationMetadata(new CompositeMetadataStore(Arrays.asList(selector.getMetadataStore()), source));
	        VDB_CACHE.put(vdbURL, vdb);
	        return vdb;
		} catch (IOException e) {
			throw new MetaMatrixRuntimeException(e);
		} catch (MetaMatrixComponentException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }	
	
	public static QueryMetadataInterface getVDBMetadata(String[] vdbFile) {
		
        List<MetadataStore> selectors = new ArrayList<MetadataStore>();
        MetadataSource source = null;
        for (int i = 0; i < vdbFile.length; i++){
        	try {
	        	MetadataSource tempSource = new VDBArchive(new File(vdbFile[i]));
	        	if (i == 0) {
	        		source = tempSource;
	        	}
				selectors.add(new IndexMetadataFactory(tempSource).getMetadataStore());
			} catch (IOException e) {
				throw new MetaMatrixRuntimeException(e);
			} catch (MetaMatrixComponentException e) {
				throw new MetaMatrixRuntimeException(e);
			}        
        }
        
        return new TransformationMetadata(new CompositeMetadataStore(selectors, source));
    }	
}
