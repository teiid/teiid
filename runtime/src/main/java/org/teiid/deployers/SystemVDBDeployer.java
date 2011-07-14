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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import org.jboss.as.server.deployment.module.TempFileProviderService;
import org.jboss.modules.Module;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.metadata.index.RuntimeMetadataPlugin;
import org.teiid.runtime.RuntimePlugin;


public class SystemVDBDeployer {
	private VDBRepository vdbRepository;
	private Closeable file;

	public void start() {
		try {
			VirtualFile mountPoint = VFS.getChild("content/" + CoreConstants.SYSTEM_VDB); //$NON-NLS-1$
			if (!mountPoint.exists()) {
				InputStream contents = Module.getCallerModule().getClassLoader().findResourceAsStream(CoreConstants.SYSTEM_VDB, false);
				if (contents == null) {
					throw new TeiidRuntimeException(RuntimeMetadataPlugin.Util.getString("system_vdb_not_found")); //$NON-NLS-1$
				}
				this.file = VFS.mountZip(contents, CoreConstants.SYSTEM_VDB, mountPoint, TempFileProviderService.provider());
			}
			
			// uri conversion is only to remove the spaces in URL, note this only with above kind situation  
			this.vdbRepository.setSystemStore(new IndexMetadataFactory(mountPoint).getMetadataStore(null));
		} catch (URISyntaxException e) {
			throw new TeiidRuntimeException(e, RuntimePlugin.Util.getString("system_vdb_load_error")); //$NON-NLS-1$
		} catch (IOException e) {
			throw new TeiidRuntimeException(e, RuntimePlugin.Util.getString("system_vdb_load_error")); //$NON-NLS-1$
		}
	}

	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}	
	
	public void stop() {
		try {
			if (file != null) {
				file.close();
			}
		} catch (IOException e) {
			//ignore
		}
	}
}
