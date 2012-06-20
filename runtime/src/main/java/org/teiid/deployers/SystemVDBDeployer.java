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
import java.util.concurrent.Executors;

import org.jboss.vfs.TempFileProvider;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.metadata.index.IndexMetadataStore;
import org.teiid.metadata.index.RuntimeMetadataPlugin;
import org.teiid.runtime.RuntimePlugin;


public class SystemVDBDeployer {
	private VDBRepository vdbRepository;
	private Closeable file;
    
	private static final TempFileProvider PROVIDER;
    static {
       try {
          PROVIDER = TempFileProvider.create("teiid-deployment", Executors.newScheduledThreadPool(2)); //$NON-NLS-1$
       }
       catch (final IOException ioe) {
          throw new RuntimeException("Failed to create temp file provider");//$NON-NLS-1$
       }
    }	

	public void start() {
		try {
			VirtualFile mountPoint = VFS.getChild("content/" + CoreConstants.SYSTEM_VDB); //$NON-NLS-1$
			if (!mountPoint.exists()) {
				InputStream contents = Thread.currentThread().getContextClassLoader().getResourceAsStream(CoreConstants.SYSTEM_VDB);
				if (contents == null) {
					 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40021, RuntimeMetadataPlugin.Util.gs(RuntimePlugin.Event.TEIID40021));
				}
				this.file = VFS.mountZip(contents, CoreConstants.SYSTEM_VDB, mountPoint, PROVIDER);
			}
			
			IndexMetadataStore idxStore = new IndexMetadataStore(mountPoint);
			idxStore.load(null, null);
			
			// uri conversion is only to remove the spaces in URL, note this only with above kind situation  
			this.vdbRepository.setSystemStore(idxStore);
		} catch (URISyntaxException e) {
			 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40022, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40022));
		} catch (IOException e) {
			 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40022, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40022));
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
