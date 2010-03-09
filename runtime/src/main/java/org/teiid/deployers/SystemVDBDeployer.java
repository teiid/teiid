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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.jboss.virtual.VirtualFile;
import org.jboss.virtual.VirtualFileFilter;
import org.jboss.virtual.plugins.context.zip.ZipEntryContext;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.metadata.index.IndexConstants;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.runtime.RuntimePlugin;

import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;

public class SystemVDBDeployer {
	private VDBRepository vdbRepository;
	

	public void start() {
		ModelMetaData model = new ModelMetaData();
		model.setName(CoreConstants.SYSTEM_MODEL);
		model.setVisible(true);
		
		VDBMetaData deployment = new VDBMetaData();
		deployment.setName(CoreConstants.SYSTEM_VDB);
		
		deployment.addModel(model);		

		try {
			URL url = Thread.currentThread().getContextClassLoader().getResource(CoreConstants.SYSTEM_VDB);
			if (url == null) {
				throw new MetaMatrixRuntimeException(RuntimePlugin.Util.getString("system_vdb_not_found")); //$NON-NLS-1$
			}
			SystemVDBContext systemVDB = new SystemVDBContext(url);
			VirtualFile vdb = new VirtualFile(systemVDB.getRoot());
			List<VirtualFile> children = vdb.getChildrenRecursively(new VirtualFileFilter() {
				@Override
				public boolean accepts(VirtualFile file) {
					return file.getName().endsWith(IndexConstants.NAME_DELIM_CHAR+IndexConstants.INDEX_EXT);
				}
			});
			
			IndexMetadataFactory imf = new IndexMetadataFactory();
			for (VirtualFile f: children) {
				imf.addIndexFile(f);
			}
			this.vdbRepository.addMetadataStore(deployment, imf.getMetadataStore());
		} catch (URISyntaxException e) {
			throw new MetaMatrixRuntimeException(e, RuntimePlugin.Util.getString("failed_to_deployed", CoreConstants.SYSTEM_VDB)); //$NON-NLS-1$
		} catch (IOException e) {
			throw new MetaMatrixRuntimeException(e, RuntimePlugin.Util.getString("failed_to_deployed", CoreConstants.SYSTEM_VDB)); //$NON-NLS-1$
		}
	}
	
	public void stop() {
		this.vdbRepository.removeVDB(CoreConstants.SYSTEM_VDB, 1);
	}
	
	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}	
	
	private static class SystemVDBContext extends ZipEntryContext{
		private static final long serialVersionUID = -6504988258841073415L;

		protected SystemVDBContext(URL url) throws IOException, URISyntaxException {
			super(url,true);
		}
	}
}
