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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.teiid.resource.cci.TranslatorProperty;

public class TranslatorPropertyUtil {
	
	public static Map<Method, TranslatorProperty> getTranslatorProperties(Class<?> attachmentClass) {
		Map<Method, TranslatorProperty> props = new HashMap<Method,  TranslatorProperty>();
		buildTranslatorProperties(attachmentClass, props);
		return props;
	}
	
	private static void buildTranslatorProperties(Class<?> attachmentClass, Map<Method, TranslatorProperty> props){
		Method[] methods = attachmentClass.getMethods();
		for (Method m:methods) {
			TranslatorProperty tp = m.getAnnotation(TranslatorProperty.class);
			if (tp != null) {
				props.put(m, tp);
			}
		}
		// Now look at the base interfaces
		Class[] baseInterfaces = attachmentClass.getInterfaces();
		for (Class clazz:baseInterfaces) {
			buildTranslatorProperties(clazz, props);
		}
		Class superClass = attachmentClass.getSuperclass();
		if (superClass != null) {
			buildTranslatorProperties(superClass, props);
		}
	}	
}
