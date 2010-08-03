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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.jboss.deployers.vfs.spi.deployer.MultipleVFSParsingDeployer;
import org.jboss.deployers.vfs.spi.structure.VFSDeploymentUnit;
import org.jboss.virtual.VirtualFile;
import org.jboss.virtual.VirtualFileFilter;

/**
 * Overriding the base MultipleVFSParsingDeployer so that the parse method is supplied with VFSDeploymentUnit.
 * @param <T>
 */
public abstract class BaseMultipleVFSParsingDeployer<T> extends	MultipleVFSParsingDeployer<T> {
	
	private String suffix2;
	private Class<?> suffixClass2;

	public BaseMultipleVFSParsingDeployer(Class<T> output,Map<String, Class<?>> mappings, String suffix, Class<?> suffixClass, String suffix2, Class<?>suffixClass2) {
		super(output, mappings, suffix, suffixClass);
		this.suffix2 = suffix2;
		this.suffixClass2 = suffixClass2;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected T mergeFiles(VFSDeploymentUnit unit, T root,List<VirtualFile> files, Set<String> missingFiles) throws Exception {
		
		List<VirtualFile> matched = unit.getMetaDataFiles(new VirtualFileFilter() {
			@Override
			public boolean accepts(VirtualFile file) {
				return file.getName().endsWith(suffix2);
			}
		});
		
		files.addAll(matched);
		Map<Class<?>, List<Object>> metadata = new HashMap<Class<?>, List<Object>>();
		for (VirtualFile file : files) {
			Class<?> clazz = matchFileToClass(unit, file);
			List<Object> instances = metadata.get(clazz);
			if (instances == null) {
				instances = new ArrayList<Object>();
				metadata.put(clazz, instances);
			}
			Object instance = parse(unit, clazz, file, root);
			boolean found = false;
			for (Object obj:instances) {
				if (obj == instance) {
					found = true;
				}
			}
			if (!found) {
				instances.add(instance);
			}
		}
		return mergeMetaData(unit, root, metadata, missingFiles);
	}
	
	@Override
	protected Class<?> matchFileToClass(DeploymentUnit unit, VirtualFile file){
		if (file.getName().endsWith(this.suffix2)) {
			return this.suffixClass2;
		}
		return super.matchFileToClass(unit, file);
	}
	
	@Override
	protected <U> U parse(Class<U> expectedType, VirtualFile file, Object root) throws Exception{
		throw new UnsupportedOperationException("This will be never invoked"); //$NON-NLS-1$
	}
	protected abstract <U> U parse(VFSDeploymentUnit unit, Class<U> expectedType, VirtualFile file, Object root) throws Exception;	
}
