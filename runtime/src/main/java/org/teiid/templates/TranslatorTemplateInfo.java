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

import java.util.Map;

import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.BasicDeploymentTemplateInfo;
import org.jboss.metatype.api.types.MapCompositeMetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.teiid.deployers.ManagedPropertyUtil;
import org.teiid.translator.TranslatorProperty;

/**
 * This class defines the template for all the translator classes. Each Translator's class
 * defines its properties through {@link TranslatorProperty} annotation. This class need to 
 * present them as template properties that can be managed. 
 */
public class TranslatorTemplateInfo extends BasicDeploymentTemplateInfo {
	
	private static final long serialVersionUID = 9066758787789280783L;
	static final String TEMPLATE_NAME = "template-name"; //$NON-NLS-1$
	static final String EXECUTION_FACTORY_NAME = "execution-factory-class"; //$NON-NLS-1$
	static final String NAME = "name"; //$NON-NLS-1$
	static final String TRANSLATOR_PROPERTY = "translator-property"; //$NON-NLS-1$
	
	
	public TranslatorTemplateInfo(String name, String description, Map<String, ManagedProperty> properties) {
		super(name, description, properties);
	}

	public void start() {
		populate();
	}

	@Override
	public TranslatorTemplateInfo copy() {
		TranslatorTemplateInfo copy = new TranslatorTemplateInfo(getName(), getDescription(), getProperties());
		super.copy(copy);
		copy.populate();
		
		return copy;
	}
	
	private void populate() {
		addProperty(buildTemplateProperty(getName()));
		
		addProperty(ManagedPropertyUtil.createProperty(EXECUTION_FACTORY_NAME,SimpleMetaType.STRING, "Execution Factory Class name", "The translator's execution factory name", true, false, null));//$NON-NLS-1$ //$NON-NLS-2$
		addProperty(ManagedPropertyUtil.createProperty(NAME,SimpleMetaType.STRING, "name", "Name of the Translator", true, false, null));//$NON-NLS-1$ //$NON-NLS-2$
		addProperty(ManagedPropertyUtil.createProperty(TRANSLATOR_PROPERTY, new MapCompositeMetaType(SimpleMetaType.STRING), TRANSLATOR_PROPERTY, "Additional Translator properties", false, false, null)); //$NON-NLS-1$
	}
	
	static ManagedProperty buildTemplateProperty(String name) {
		return ManagedPropertyUtil.createProperty(TEMPLATE_NAME,SimpleMetaType.STRING,
						"Template Name", "The Name of the Teiid Connector Template", true, true, name);//$NON-NLS-1$ //$NON-NLS-2$
	}		
}
