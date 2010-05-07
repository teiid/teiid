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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedProperty;
import org.teiid.deployers.ManagedPropertyUtil;
import org.teiid.deployers.TranslatorPropertyUtil;
import org.teiid.resource.cci.TranslatorProperty;

public class TranslatorTemplateInfoFactory {

	/**
	 * Create a DeploymentTemplateInfo by scanning the metadata attachment class
	 * for ManagementProperty annotations.
	 * 
	 * @param infoClass - the DeploymentTemplateInfo implementation to use. Must have a ctor with sig (String,String,Map).
	 * @param attachmentClass - the metadata class to scan for ManagementProperty annotations
	 * @param name - the template name
	 * @param description - the template description
	 * @return the DeploymentTemplateInfo instance
	 * @throws Exception on failure to create the DeploymentTemplateInfo
	 */
	public DeploymentTemplateInfo createTemplateInfo(Class<? extends DeploymentTemplateInfo> infoClass, Class<?> attachmentClass, String name, String description) throws Exception {

		Map<Method, TranslatorProperty> props = TranslatorPropertyUtil.getTranslatorProperties(attachmentClass);
		
		Map<String, ManagedProperty> infoProps = new HashMap<String, ManagedProperty>();
		
		Collection<TranslatorProperty> propertyInfos = props.values();
		if (propertyInfos != null && !propertyInfos.isEmpty()) {
			for (TranslatorProperty propertyInfo : propertyInfos) {
				ManagedProperty mp = ManagedPropertyUtil.convert(propertyInfo);
				infoProps.put(mp.getName(), mp);
			}
		}
		Class<?>[] parameterTypes = { String.class, String.class, Map.class };
		Constructor<? extends DeploymentTemplateInfo> ctor = infoClass.getConstructor(parameterTypes);
		DeploymentTemplateInfo info = ctor.newInstance(name, description,infoProps);
		return info;
	}
}
