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
import java.util.ArrayList;
import java.util.List;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.vfs.plugins.structure.AbstractVFSStructureDeployer;
import org.jboss.deployers.vfs.spi.structure.StructureContext;
import org.jboss.virtual.VirtualFile;
import org.jboss.virtual.plugins.context.jar.JarUtils;
import org.teiid.metadata.VdbConstants;



public class VDBStructure  extends AbstractVFSStructureDeployer{
	
   public VDBStructure(){
      setRelativeOrder(1000);
      JarUtils.addJarSuffix(".vdb"); //$NON-NLS-1$
   }	
   
	@Override
	public boolean determineStructure(StructureContext structureContext) throws DeploymentException {
		VirtualFile file = structureContext.getFile();
		try {
			if (isLeaf(file) == false) {
				if (file.getName().endsWith(".vdb")) { //$NON-NLS-1$
					
					VirtualFile metainf = file.getChild("META-INF"); //$NON-NLS-1$
					if (metainf == null) {
						return false;
					}
					
					if (metainf.getChild(VdbConstants.DEPLOYMENT_FILE) == null) {
						return false;
					}
					
					List<String> scanDirs = new ArrayList<String>();
					scanDirs.add("/"); //$NON-NLS-1$
					
					List<VirtualFile> children = file.getChildren();
					for (VirtualFile child:children) {
						addAllDirs(child, scanDirs, null);
					}
					createContext(structureContext, scanDirs.toArray(new String[scanDirs.size()]));	
					return true;
				}
			}
		} catch (IOException e) {
			throw DeploymentException.rethrowAsDeploymentException("Error determining structure: " + file.getName(), e); //$NON-NLS-1$
		}
		return false;
	}
	
	private void addAllDirs(VirtualFile file, List<String> scanDirs, String parentName) throws IOException {
		if (!file.isLeaf()) {
			if (parentName != null) {
				scanDirs.add(parentName + "/" + file.getName()); //$NON-NLS-1$
			}
			else {
				scanDirs.add(file.getName());
			}
			List<VirtualFile> children = file.getChildren();
			for (VirtualFile child:children) {
				if (parentName == null) {
					addAllDirs(child, scanDirs, file.getName());
				}
				else {
					addAllDirs(child, scanDirs, parentName + "/" +file.getName()); //$NON-NLS-1$
				}
			}
		}
	}

}
