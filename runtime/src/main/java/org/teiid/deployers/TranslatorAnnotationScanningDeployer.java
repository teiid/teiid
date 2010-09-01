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

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.annotations.AnnotationEnvironment;
import org.jboss.deployers.spi.deployer.helpers.AbstractAnnotationDeployer;
import org.jboss.deployers.spi.deployer.helpers.AbstractAnnotationProcessor;
import org.jboss.deployers.spi.deployer.managed.ManagedObjectCreator;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.factory.ManagedObjectFactory;
import org.teiid.adminapi.impl.TranslatorMetaData;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.Translator;

/**
 * This translator looks for classes with {@link Translator} annotation, if it finds it tries to build metadata based on that
 * annotation. 
 * TODO: the logic in the translator deployers need to be consolidated.
 */
public class TranslatorAnnotationScanningDeployer extends AbstractAnnotationDeployer implements ManagedObjectCreator {

	private TranslatorRepository translatorRepository;
	private VDBStatusChecker vdbChecker;
	private ManagedObjectFactory mof;
	
	public TranslatorAnnotationScanningDeployer() {
		super(new TranslatorAnnotationProcessor());
	}

	private static class TranslatorAnnotationProcessor extends AbstractAnnotationProcessor<Translator, TranslatorMetaData> {
		public Class<Translator> getAnnotation() {
			return Translator.class;
		}

		public Class<TranslatorMetaData> getOutput() {
			return TranslatorMetaData.class;
		}

		// this is called with-in the deploy 
		protected TranslatorMetaData createMetaDataFromClass(Class<?> clazz, Translator bean) {
			String name = bean.name();
			if (name == null) {
				throw new IllegalArgumentException("Null Translator name: " + clazz); //$NON-NLS-1$
			}
			
			TranslatorMetaData data = new TranslatorMetaData();
			data.setName(bean.name());
			data.setExecutionFactoryClass(clazz);
			data.setDescription(bean.description()); 

			return data;
		}
	}
	
	public void deploy(DeploymentUnit unit, AnnotationEnvironment deployment) throws DeploymentException {
		super.deploy(unit, deployment);

		Collection<Object> group = unit.getAttachments().values();

		for (Object anObj : group) {
			if (anObj instanceof TranslatorMetaData) {
				TranslatorMetaData data = (TranslatorMetaData)anObj;
				String translatorName = data.getName();
				if (translatorName == null) {
					throw new DeploymentException(RuntimePlugin.Util.getString("name_not_found", unit.getName())); //$NON-NLS-1$
				}
				
				// fill with default properties for the tooling to see the properties
				Properties props = TranslatorUtil.getTranslatorPropertiesAsProperties(data.getExecutionFactoryClass());
				data.setProperties(props);
				data.addProperty(TranslatorMetaData.EXECUTION_FACTORY_CLASS, data.getExecutionFactoryClass().getName());
				
	            this.translatorRepository.addTranslatorMetadata(translatorName, data);
	            LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("translator_added", translatorName)); //$NON-NLS-1$
	            this.vdbChecker.translatorAdded(translatorName);
			}
		}
	}

	@Override
	public void undeploy(DeploymentUnit unit, AnnotationEnvironment deployment) {
		super.undeploy(unit, deployment);
		
		Collection<Object> group = unit.getAttachments().values();

		for (Object anObj : group) {
			if (anObj instanceof TranslatorMetaData) {
				TranslatorMetaData data = (TranslatorMetaData)anObj;
				String translatorName = data.getName();
				if (this.translatorRepository != null) {
					this.translatorRepository.removeTranslatorMetadata(translatorName);
					LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("translator_removed", translatorName)); //$NON-NLS-1$
					this.vdbChecker.translatorRemoved(translatorName);
				}
			}
		}		
	}
	
	@Override
	public void build(DeploymentUnit unit, Set<String> attachmentNames, Map<String, ManagedObject> managedObjects) throws DeploymentException {
		Collection<Object> group = unit.getAttachments().values();

		for (Object anObj : group) {
			if (anObj instanceof TranslatorMetaData) {
				TranslatorMetaData data = (TranslatorMetaData)anObj;
				
				ManagedObject mo = this.mof.initManagedObject(data, TranslatorMetaData.class, data.getName(),data.getName());
				if (mo == null) {
					throw new DeploymentException("could not create managed object"); //$NON-NLS-1$
				}
				managedObjects.put(mo.getName(), mo);
				
			}
		}
	}
	
	public void setTranslatorRepository(TranslatorRepository repo) {
		this.translatorRepository = repo;
	}	
	
	public void setVDBStatusChecker(VDBStatusChecker checker) {
		this.vdbChecker = checker;
	}
	
	public void setManagedObjectFactory(ManagedObjectFactory mof) {
		this.mof = mof;
	}
}