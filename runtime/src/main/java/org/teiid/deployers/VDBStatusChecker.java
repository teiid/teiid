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

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.teiid.adminapi.Model;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.runtime.RuntimePlugin;

import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;

public class VDBStatusChecker {
	private VDBRepository vdbRepository;
	
	public void connectorAdded(String connectorName) {
		for (VDBMetaData vdb:this.vdbRepository.getVDBs()) {
			if (vdb.getStatus() == VDB.Status.ACTIVE || vdb.isPreview()) {
				continue;
			}
			
			for (Model m:vdb.getModels()) {
				ModelMetaData model = (ModelMetaData)m;
				if (model.getErrors().isEmpty()) {
					continue;
				}

				boolean inUse = false;
				for (String sourceName:model.getSourceNames()) {
					if (connectorName.equals(model.getSourceJndiName(sourceName))) {
						inUse = true;
					}
				}
					
				if (inUse) {
					model.clearErrors();
					for (String sourceName:model.getSourceNames()) {
						if (!connectorName.equals(model.getSourceJndiName(sourceName))) {
							try {
								InitialContext ic = new InitialContext();
								ic.lookup(model.getSourceJndiName(sourceName));
							} catch (NamingException e) {
								String msg = RuntimePlugin.Util.getString("jndi_not_found", vdb.getName(), vdb.getVersion(), model.getSourceJndiName(sourceName), sourceName); //$NON-NLS-1$
								model.addError(ModelMetaData.ValidationError.Severity.ERROR.name(), msg);
								LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);
							}								
						}
					}						
				}
			}

			boolean valid = true;
			for (Model m:vdb.getModels()) {
				ModelMetaData model = (ModelMetaData)m;
				if (!model.getErrors().isEmpty()) {
					valid = false;
					break;
				}
			}
			
			if (valid) {
				vdb.setStatus(VDB.Status.ACTIVE);
				LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_activated",vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
			}
			
		}
	}

	public void connectorRemoved(String connectorName) {
		for (VDBMetaData vdb:this.vdbRepository.getVDBs()) {
			if (vdb.isPreview()) {
				continue;
			}
			for (Model m:vdb.getModels()) {
				ModelMetaData model = (ModelMetaData)m;
				for (String sourceName:model.getSourceNames()) {
					if (connectorName.equals(model.getSourceJndiName(sourceName))) {
						vdb.setStatus(VDB.Status.INACTIVE);
						String msg = RuntimePlugin.Util.getString("jndi_not_found", vdb.getName(), vdb.getVersion(), model.getSourceJndiName(sourceName), sourceName); //$NON-NLS-1$
						model.addError(ModelMetaData.ValidationError.Severity.ERROR.name(), msg);
						LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);					
						LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_inactivated",vdb.getName(), vdb.getVersion())); //$NON-NLS-1$							
					}
				}
			}
		}
	}
	
	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}	
}
