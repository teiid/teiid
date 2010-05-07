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
package org.teiid.templates;

import java.io.File;
import java.io.FileWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.jboss.deployers.spi.management.DeploymentTemplate;
import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.api.factory.ManagedObjectFactory;
import org.jboss.virtual.VFS;
import org.jboss.virtual.VirtualFile;
import org.teiid.adminapi.impl.TranslatorMetaData;
import org.teiid.deployers.TranslatorParserDeployer;

/**
 * Translator template writer and deployer 
 */
public class TranslatorDeploymentTemplate implements DeploymentTemplate {

	private DeploymentTemplateInfo info;
	private ManagedObjectFactory mof;
    
	public String getDeploymentName(String deploymentBaseName) {
		if (deploymentBaseName == null)
			throw new IllegalArgumentException("Null base name.");//$NON-NLS-1$	
		
	    if(deploymentBaseName.endsWith(TranslatorParserDeployer.TRANSLATOR_SUFFIX) == false)
	        deploymentBaseName = deploymentBaseName + TranslatorParserDeployer.TRANSLATOR_SUFFIX;
	    
		return deploymentBaseName;
	}

	public VirtualFile applyTemplate(DeploymentTemplateInfo sourceInfo) throws Exception {
		File dsXml = File.createTempFile(getClass().getSimpleName(),TranslatorParserDeployer.TRANSLATOR_SUFFIX);
		writeTemplate(dsXml, sourceInfo);
		return VFS.getRoot(dsXml.toURI());
	}
	
	private void writeTemplate(File dsXml, DeploymentTemplateInfo values) throws Exception {

		// The management framework, will update the attachment the managed property value changes.
		// that way the translator is configured.
		TranslatorMetaData translator = new TranslatorMetaData();
        this.mof.setInstanceClassFactory(TranslatorMetaData.class, new TranslatorMetadataICF(this.mof));
        ManagedObject mo = mof.initManagedObject(translator, "teiid", "translator"); //$NON-NLS-1$ //$NON-NLS-2$		

		for (ManagedProperty mp : values.getProperties().values()) {
			ManagedProperty dsProp = mo.getProperty(mp.getName());
			if (dsProp != null) {
				if (mp.getValue() != null) {
					dsProp.setValue(mp.getValue());
				}
			}
		}      
        
		// Now use JAXB and write the file.
		Class[] classes = { TranslatorMetaData.class };
		JAXBContext context = JAXBContext.newInstance(classes);
		Marshaller marshaller = context.createMarshaller();
		marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,new Boolean(true));

		FileWriter fw = null;
		try {
			fw = new FileWriter(dsXml);
			marshaller.marshal(translator, fw);
		} finally {
			if (fw != null) {
				fw.close();
			}
		}
	}

	@Override
	public DeploymentTemplateInfo getInfo() {
		return info;
	}

	public void setInfo(DeploymentTemplateInfo info) {
		this.info = info;
	}
	
	public void setManagedObjectFactory(ManagedObjectFactory mof) {
		this.mof = mof;
	}	
}
