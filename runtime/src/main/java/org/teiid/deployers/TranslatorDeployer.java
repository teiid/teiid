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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.deployer.helpers.AbstractSimpleRealDeployer;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.teiid.adminapi.impl.TranslatorMetaData;
import org.teiid.core.TeiidException;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.datamgr.impl.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorProperty;


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
			
			String executionFactoryClass = data.getExecutionFactoryClass();
			if (executionFactoryClass != null) {
				ExecutionFactory connector = buildTranslator(executionFactoryClass, data);
	            this.translatorRepository.addTranslator(translatorName, connector);
	            LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("translator_started", translatorName)); //$NON-NLS-1$
	            this.vdbChecker.translatorAdded(translatorName);
			}	
			else {
				throw new DeploymentException(RuntimePlugin.Util.getString("executionfactory_not_found", unit.getName())); //$NON-NLS-1$
			}
		}
	}
	
	@Override
	public void undeploy(DeploymentUnit unit, TranslatorMetaDataGroup group) {
		super.undeploy(unit, group);

		for (TranslatorMetaData data:group.getTranslators()) {
		
			String translatorName = data.getName();
			if (this.translatorRepository != null) {
				this.translatorRepository.removeTranslator(translatorName);
				LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("translator_stopped", translatorName)); //$NON-NLS-1$
				this.vdbChecker.translatorRemoved(translatorName);
			}
		}
	}	

	ExecutionFactory buildTranslator(String executionFactoryClass, TranslatorMetaData data) throws DeploymentException {
		ExecutionFactory executionFactory;
		try {
			Object o = ReflectionHelper.create(executionFactoryClass, null, Thread.currentThread().getContextClassLoader());
			if(!(o instanceof ExecutionFactory)) {
				throw new DeploymentException(RuntimePlugin.Util.getString("invalid_class", executionFactoryClass));//$NON-NLS-1$	
			}
			
			executionFactory = (ExecutionFactory)o;
			injectProperties(executionFactory, data);
			executionFactory.start();
			return executionFactory;
			
		} catch (TeiidException e) {
			throw new DeploymentException(e);
		} catch (InvocationTargetException e) {
			throw new DeploymentException(e);
		} catch (IllegalAccessException e) {
			throw new DeploymentException(e);
		}
	}
	
	public static String getPropertyName(Method method) {
		String result = method.getName();
		if (result.startsWith("get") || result.startsWith("set")) { //$NON-NLS-1$ //$NON-NLS-2$
			return result.substring(3);
		}
		else if (result.startsWith("is")) { //$NON-NLS-1$
			return result.substring(2);
		}
		return result;
	}
	
	public static Method getSetter(Class<?> clazz, Method method) throws SecurityException, DeploymentException {
		String setter = method.getName();
		if (method.getName().startsWith("get")) { //$NON-NLS-1$
			setter = "set"+setter.substring(3);//$NON-NLS-1$
		}
		else if (method.getName().startsWith("is")) { //$NON-NLS-1$
			setter = "set"+setter.substring(2); //$NON-NLS-1$
		}
		try {
			return clazz.getMethod(setter, method.getReturnType());
		} catch (NoSuchMethodException e) {
			throw new DeploymentException(RuntimePlugin.Util.getString("no_set_method", setter, method.getName())); //$NON-NLS-1$
		}
	}
	
	private void injectProperties(ExecutionFactory ef, final TranslatorMetaData data) throws InvocationTargetException, IllegalAccessException, DeploymentException{
		Map<Method, TranslatorProperty> props = TranslatorPropertyUtil.getTranslatorProperties(ef.getClass());
		
		for (Method method:props.keySet()) {
			TranslatorProperty tp = props.get(method);
			if (tp.managed()) {
				continue;
			}
			String propertyName = getPropertyName(method);
			Object value = data.getPropertyValue(propertyName);
			
			if (value != null) {
				Method setterMethod = getSetter(ef.getClass(), method);
				setterMethod.invoke(ef, convert(value, method.getReturnType()));
			} else if (tp.required()) {
				throw new DeploymentException(RuntimePlugin.Util.getString("required_property_not_exists", tp.display())); //$NON-NLS-1$
			}
		}
		
		ef.setExceptionOnMaxRows(data.isExceptionOnMaxRows());
		ef.setImmutable(data.isImmutable());
		ef.setMaxResultRows(data.getMaxResultRows());
		ef.setXaCapable(data.isXaCapable());
	}

	public void setTranslatorRepository(TranslatorRepository repo) {
		this.translatorRepository = repo;
	}	
	
	public void setVDBStatusChecker(VDBStatusChecker checker) {
		this.vdbChecker = checker;
	}
	
	Object convert(Object value, Class<?> type) {
		if(value.getClass() == type) {
			return value;
		}
		
		if (value instanceof String) {
			String str = (String)value;
			return StringUtil.valueOf(str, type);
		}
		return value;
	}
}
