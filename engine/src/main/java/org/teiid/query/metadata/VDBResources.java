/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;

import org.jboss.vfs.VirtualFile;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.metadata.VDBResource;

public class VDBResources {
	
	public static final String DEPLOYMENT_FILE = "vdb.xml"; // !!! DO NOT CHANGE VALUE as this would cause problems with existing VDBs having DEF files !!! //$NON-NLS-1$
	public static final String VDB_ARCHIVE_EXTENSION = ".vdb"; //$NON-NLS-1$
	public final static String INDEX_EXT        = ".INDEX";     //$NON-NLS-1$
	public final static String SEARCH_INDEX_EXT = ".SEARCH_INDEX";     //$NON-NLS-1$
	public final static String MODEL_EXT = ".xmi";     //$NON-NLS-1$
	
	public static class Resource implements VDBResource {
		public Resource(VirtualFile file) {
			this.file = file;
		}
		VirtualFile file;
		boolean visible;
		@Override
		public InputStream openStream() throws IOException {
			return file.openStream();
		}
		@Override
		public long getSize() {
			return file.getSize();
		}
		@Override
		public String getName() {
			return file.getName();
		}
		public VirtualFile getFile() {
			return file;
		}
	}

	private LinkedHashMap<String, VDBResources.Resource> vdbEntries;
	
	public VDBResources(VirtualFile root, VDBMetaData vdb) throws IOException {
		LinkedHashMap<String, VDBResources.Resource> visibilityMap = new LinkedHashMap<String, VDBResources.Resource>();
		for(VirtualFile f: root.getChildrenRecursively()) {
			if (f.isFile()) {
				// remove the leading vdb name from the entry
				String path = f.getPathName().substring(root.getPathName().length());
				if (!path.startsWith("/")) { //$NON-NLS-1$
					path = "/" + path; //$NON-NLS-1$
				}
				visibilityMap.put(path, new VDBResources.Resource(f)); 
			}
		}
		this.vdbEntries = visibilityMap;
	}
	
	public LinkedHashMap<String, VDBResources.Resource> getEntriesPlusVisibilities(){
		return this.vdbEntries;
	}

}
