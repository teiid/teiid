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

import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.metadata.index.RuntimeMetadataPlugin;
import org.teiid.runtime.RuntimePlugin;


public class SystemVDBDeployer {
	private VDBRepository vdbRepository;
	

	public void start() {
		try {
			URL url = Thread.currentThread().getContextClassLoader().getResource(CoreConstants.SYSTEM_VDB);
			if (url == null) {
				throw new TeiidRuntimeException(RuntimeMetadataPlugin.Util.getString("system_vdb_not_found")); //$NON-NLS-1$
			}
			this.vdbRepository.setSystemStore(new IndexMetadataFactory(url).getMetadataStore());
		} catch (URISyntaxException e) {
			throw new TeiidRuntimeException(e, RuntimePlugin.Util.getString("failed_to_deployed", CoreConstants.SYSTEM_VDB)); //$NON-NLS-1$
		} catch (IOException e) {
			throw new TeiidRuntimeException(e, RuntimePlugin.Util.getString("failed_to_deployed", CoreConstants.SYSTEM_VDB)); //$NON-NLS-1$
		}
	}

	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}	
	
}
