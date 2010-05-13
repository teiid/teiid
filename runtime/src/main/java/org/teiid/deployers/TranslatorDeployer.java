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
import org.jboss.managed.api.annotation.ManagementProperty;
import org.teiid.adminapi.impl.TranslatorMetaData;
import org.teiid.core.TeiidException;
import org.teiid.core.util.ReflectionHelper;
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
		} catch (NoSuchMethodException e) {
			throw new DeploymentException(e);
		}
	}
    
	private void injectProperties(ExecutionFactory ef, final TranslatorMetaData data) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, DeploymentException{
		Map<Method, TranslatorProperty> props = TranslatorPropertyUtil.getTranslatorProperties(ef.getClass());
		
		for (Method method:props.keySet()) {
			TranslatorProperty tp = props.get(method);
			Object value = data.getPropertyValue(tp.name());
			if (value == null) {
				Method[] sourceMethods = data.getClass().getMethods();
				for (Method sm:sourceMethods) {
					ManagementProperty mp = sm.getAnnotation(ManagementProperty.class);
					if (mp != null && mp.name().equals(tp.name())) {
						value = sm.invoke(data);
						break;
					}
				}
			}
			
			if (value != null) {
				String setter = method.getName();
				if (method.getName().startsWith("get")) { //$NON-NLS-1$
					setter = "set"+method.getName().substring(3);//$NON-NLS-1$
				}
				else if (method.getName().startsWith("is")) { //$NON-NLS-1$
					setter = "set"+method.getName().substring(2); //$NON-NLS-1$
				}
				Method setterMethod = ef.getClass().getMethod(setter, method.getReturnType());
				if (setterMethod == null) {
					throw new DeploymentException(RuntimePlugin.Util.getString("no_set_method", setter, tp.name())); //$NON-NLS-1$
				}
				setterMethod.invoke(ef, convert(value, method.getReturnType()));
			}
		}
	}

	public void setTranslatorRepository(TranslatorRepository repo) {
		this.translatorRepository = repo;
	}	
	
	public void setVDBStatusChecker(VDBStatusChecker checker) {
		this.vdbChecker = checker;
	}
	
	Object convert(Object value, Class type) {
		if(value.getClass() == type) {
			return value;
		}
		
		if (value instanceof String) {
			String str = (String)value;
			if (type == int.class || type == Integer.class) {
				return Integer.parseInt(str);
			}
			else if (type == boolean.class || type == Boolean.class) {
				return Boolean.parseBoolean(str);
			}
			else if (type == long.class || type == Long.class) {
				return Long.parseLong(str);
			}
			else if (type == byte.class || type == Byte.class) {
				return Byte.parseByte(str);
			}
			else if (type == short.class || type == Short.class) {
				return Short.parseShort(str);
			}
			else if (type == float.class || type == Float.class) {
				return Float.parseFloat(str);
			}
		}
		return value;
	}
}
