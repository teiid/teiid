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

import java.util.Set;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.deployer.helpers.AbstractSimpleRealDeployer;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.TranslatorMetaData;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;


/**
 * Deployer for the Translator
 */
public class TranslatorDeployer extends AbstractSimpleRealDeployer<TranslatorMetaDataGroup> {
	
	private TranslatorRepository translatorRepository;
	private VDBStatusChecker vdbChecker;
	
	public TranslatorDeployer() {
		super(TranslatorMetaDataGroup.class);
	}

	@Override
	public void deploy(DeploymentUnit unit, TranslatorMetaDataGroup group) throws DeploymentException {

		for (TranslatorMetaData data:group.getTranslators()) {
			String translatorName = data.getName();
			if (translatorName == null) {
				throw new DeploymentException(RuntimePlugin.Util.getString("name_not_found", unit.getName())); //$NON-NLS-1$
			}
			
			String type = data.getType();
			Translator parent = this.translatorRepository.getTranslatorMetaData(type);
			if ( parent == null) {
				throw new DeploymentException(RuntimePlugin.Util.getString("translator_type_not_found", unit.getName())); //$NON-NLS-1$
			}
			
			// fill with default properties ignoring the overridden ones.
			Set<String> keys = parent.getProperties().stringPropertyNames();
			for (String key:keys) {
				if (data.getPropertyValue(key) == null && parent.getPropertyValue(key) != null) {
					data.addProperty(key, parent.getPropertyValue(key));
				}
			}
			
            this.translatorRepository.addTranslatorMetadata(translatorName, data);
            LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("translator_added", translatorName)); //$NON-NLS-1$
            this.vdbChecker.translatorAdded(translatorName);
		}
	}
	
	@Override
	public void undeploy(DeploymentUnit unit, TranslatorMetaDataGroup group) {
		super.undeploy(unit, group);

		for (TranslatorMetaData data:group.getTranslators()) {
		
			String translatorName = data.getName();
			if (this.translatorRepository != null) {
				this.translatorRepository.removeTranslatorMetadata(translatorName);
				LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("translator_removed", translatorName)); //$NON-NLS-1$
				this.vdbChecker.translatorRemoved(translatorName);
			}
		}
	}	

	public void setTranslatorRepository(TranslatorRepository repo) {
		this.translatorRepository = repo;
	}	
	
	public void setVDBStatusChecker(VDBStatusChecker checker) {
		this.vdbChecker = checker;
	}
}
