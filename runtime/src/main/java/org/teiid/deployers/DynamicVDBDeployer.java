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

import javax.xml.bind.Unmarshaller;

import org.jboss.deployers.vfs.spi.deployer.AbstractVFSParsingDeployer;
import org.jboss.deployers.vfs.spi.structure.VFSDeploymentUnit;
import org.jboss.virtual.VirtualFile;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;

import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.CoreConstants;

public class DynamicVDBDeployer extends AbstractVFSParsingDeployer<VDBMetaData> {
	
	public DynamicVDBDeployer() {
		super(VDBMetaData.class);
		setSuffix("-vdb.xml"); //$NON-NLS-1$
	}

	@Override
	protected VDBMetaData parse(VFSDeploymentUnit unit, VirtualFile file, VDBMetaData root) throws Exception {
		Unmarshaller un = VDBParserDeployer.getUnMarsheller();
		VDBMetaData vdb = (VDBMetaData)un.unmarshal(file.openStream());
		
		vdb.setUrl(unit.getRoot().toURL().toExternalForm());
		vdb.setDynamic(true);
		
		// Add system model to the deployed VDB
		ModelMetaData system = new ModelMetaData();
		system.setName(CoreConstants.SYSTEM_MODEL);
		system.setVisible(true);
		system.setModelType(Model.Type.PHYSICAL);
		system.addSourceMapping(CoreConstants.SYSTEM_MODEL, CoreConstants.SYSTEM_MODEL); 
		system.setSupportsMultiSourceBindings(false);
		vdb.addModel(system);		
		
		LogManager.logDetail(LogConstants.CTX_RUNTIME,"VDB "+unit.getRoot().getName()+" has been parsed.");  //$NON-NLS-1$ //$NON-NLS-2$
		
		// The loading of metadata from data sources will be done during the real deploy
		// as the resources are guaranteed to be available by that time.
		
		return vdb;
	}
}
