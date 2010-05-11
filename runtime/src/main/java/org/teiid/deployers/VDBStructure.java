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

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.vfs.plugins.structure.AbstractVFSStructureDeployer;
import org.jboss.deployers.vfs.spi.structure.StructureContext;
import org.jboss.virtual.VirtualFile;
import org.jboss.virtual.plugins.context.jar.JarUtils;
import org.teiid.metadata.VdbConstants;



public class VDBStructure  extends AbstractVFSStructureDeployer{
	
   public VDBStructure(){
      setRelativeOrder(1000);
      JarUtils.addJarSuffix(".vdb");
   }	
   
	@Override
	public boolean determineStructure(StructureContext structureContext) throws DeploymentException {
		VirtualFile file = structureContext.getFile();
		try {
			if (isLeaf(file) == false) {
				if (file.getName().endsWith(".vdb")) {
					
					VirtualFile metainf = file.getChild("META-INF");
					if (metainf == null) {
						return false;
					}
					
					if (metainf.getChild(VdbConstants.DEPLOYMENT_FILE) == null) {
						return false;
					}
					createContext(structureContext, new String[] {"/", "META-INF", "runtime-inf"});	
					return true;
				}
			}
		} catch (IOException e) {
			throw DeploymentException.rethrowAsDeploymentException("Error determining structure: " + file.getName(), e);
		}
		return false;
	}

}
