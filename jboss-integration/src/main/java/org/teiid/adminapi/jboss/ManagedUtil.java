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
package org.teiid.adminapi.jboss;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jboss.deployers.spi.management.deploy.DeploymentManager;
import org.jboss.deployers.spi.management.deploy.DeploymentProgress;
import org.jboss.deployers.spi.management.deploy.DeploymentStatus;
import org.jboss.managed.api.ManagedCommon;
import org.jboss.managed.api.ManagedOperation;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.metatype.api.types.EnumMetaType;
import org.jboss.metatype.api.types.MapCompositeMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.CollectionValue;
import org.jboss.metatype.api.values.EnumValue;
import org.jboss.metatype.api.values.EnumValueSupport;
import org.jboss.metatype.api.values.MapCompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.PropertiesMetaValue;
import org.jboss.metatype.api.values.SimpleValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.profileservice.spi.DeploymentOption;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.jboss.IntegrationPlugin;


public class ManagedUtil {
	
	public static boolean sameValue(MetaValue v1, String v2) {
		if (v1 == null || v2 == null) {
			return false;
		}
		
		MetaType type = v1.getMetaType();
		if (v1 instanceof SimpleValue && type instanceof SimpleMetaType) {
			SimpleMetaType st = (SimpleMetaType)type;
			SimpleValue sv = wrap(st, v2);
			return sv.compareTo((SimpleValue)v1) == 0;
		}
		return false;
	}
	
	public static boolean sameValue(MetaValue v1, MetaValue v2) {
		if (v1 == null || v2 == null) {
			return false;
		}
		
		if (v1 instanceof SimpleValue && v2 instanceof SimpleValue) {
			return ((SimpleValue)v1).compareTo((SimpleValue)v2) == 0;
		}
		return false;
	}

	public static MapCompositeValueSupport compositeValueMap(Map<String, String> map) {
		MapCompositeValueSupport metaValue = new MapCompositeValueSupport(SimpleMetaType.STRING);
		for (String key : map.keySet()) {
			MetaValue value = SimpleValueSupport.wrap(map.get(key));
			metaValue.put(key, value);
		}
		return metaValue;
	}	
	
	public static String stringValue(MetaValue v1) {
		if (v1 != null) {
			MetaType type = v1.getMetaType();
			if (type instanceof SimpleMetaType) {
				SimpleValue simple = (SimpleValue)v1;
				return simple.getValue().toString();
			}
			throw new TeiidRuntimeException("Failed to convert value to string value"); //$NON-NLS-1$
		}
		return null;
	}	
	
	public static <T> T getSimpleValue(ManagedCommon mc, String prop, Class<T> expectedType) {
		 ManagedProperty mp = mc.getProperty(prop);
		 if (mp != null) {
			 MetaType metaType = mp.getMetaType();
			 if (metaType.isSimple()) {
		            SimpleValue simpleValue = (SimpleValue)mp.getValue();
		            return expectedType.cast((simpleValue != null) ? simpleValue.getValue() : null);
			 }
			 else if (metaType.isEnum()) {
				 EnumValue enumValue = (EnumValue)mp.getValue();
				 return expectedType.cast((enumValue != null) ? enumValue.getValue() : null);
			 }
			 throw new IllegalArgumentException(prop+ " is not a simple type"); //$NON-NLS-1$
		 }
		 return null;
	}	
	
	public static Properties getPropertiesValue(ManagedCommon mc, String prop) {
		 ManagedProperty mp = mc.getProperty(prop);
		 if (mp != null) {
			 MetaType metaType = mp.getMetaType();
			 if (metaType.isProperties()) {
				 return (PropertiesMetaValue)mp.getValue();
			 }
			 else if (metaType.isComposite()) {
				Properties props = new Properties();
				MapCompositeValueSupport map = (MapCompositeValueSupport) mp.getValue();
				MapCompositeMetaType type = map.getMetaType();
				for (String key : type.keySet()) {
					MetaValue value = map.get(key);
					props.setProperty(key, stringValue(value));
				}
				return props;
			 }
			 throw new IllegalArgumentException(prop+ " is not a properties type"); //$NON-NLS-1$
		 }
		 return null;
	}	
	
	public static <T> void getCollectionValue(ManagedCommon mc, String prop, Collection<T> list, Class<T> expectedType) {
		 ManagedProperty mp = mc.getProperty(prop);
		 if (mp != null) {
			 MetaType metaType = mp.getMetaType();
			 if (metaType.isCollection()) {
				 CollectionValue collectionValue = (CollectionValue)mp.getValue();
				 for(MetaValue value:collectionValue.getElements()) {
					 if (value.getMetaType().isSimple()) {
						 SimpleValue simpleValue = (SimpleValue)value;
						 list.add(expectedType.cast(simpleValue.getValue()));
					 }
					 else {
						 throw new IllegalArgumentException(prop+ " is not a simple type"); //$NON-NLS-1$
					 }
				 }
			 }
		 }
	}
	
	public static EnumValue wrap(EnumMetaType type, String value) {
		return new EnumValueSupport(type, value);
	}
	
	public static SimpleValue wrap(MetaType type, String value) {
		if (type instanceof SimpleMetaType) {
			SimpleMetaType st = (SimpleMetaType)type;
			
			if (SimpleMetaType.BIGDECIMAL.equals(st)) {
				return new SimpleValueSupport(st, new BigDecimal(value));
			} else if (SimpleMetaType.BIGINTEGER.equals(st)) {
				return new SimpleValueSupport(st, new BigInteger(value));
			} else if (SimpleMetaType.BOOLEAN.equals(st)) {
				return new SimpleValueSupport(st, Boolean.valueOf(value));
			} else if (SimpleMetaType.BOOLEAN_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Boolean.valueOf(value).booleanValue());
			} else if (SimpleMetaType.BYTE.equals(st)) {
				return new SimpleValueSupport(st, new Byte(value.getBytes()[0]));
			} else if (SimpleMetaType.BYTE_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, value.getBytes()[0]);
			} else if (SimpleMetaType.CHARACTER.equals(st)) {
				return new SimpleValueSupport(st, new Character(value.charAt(0)));
			} else if (SimpleMetaType.CHARACTER_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st,value.charAt(0));
			} else if (SimpleMetaType.DATE.equals(st)) {
				try {
					return new SimpleValueSupport(st, SimpleDateFormat.getInstance().parse(value));
				} catch (ParseException e) {
					throw new TeiidRuntimeException(e, IntegrationPlugin.Util.getString("failed_to_convert", type.getClassName())); //$NON-NLS-1$
				}
			} else if (SimpleMetaType.DOUBLE.equals(st)) {
				return new SimpleValueSupport(st, Double.valueOf(value));
			} else if (SimpleMetaType.DOUBLE_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Double.parseDouble(value));
			} else if (SimpleMetaType.FLOAT.equals(st)) {
				return new SimpleValueSupport(st, Float.parseFloat(value));
			} else if (SimpleMetaType.FLOAT_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Float.valueOf(value));
			} else if (SimpleMetaType.INTEGER.equals(st)) {
				return new SimpleValueSupport(st, Integer.valueOf(value));
			} else if (SimpleMetaType.INTEGER_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Integer.parseInt(value));
			} else if (SimpleMetaType.LONG.equals(st)) {
				return new SimpleValueSupport(st, Long.valueOf(value));
			} else if (SimpleMetaType.LONG_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Long.parseLong(value));
			} else if (SimpleMetaType.SHORT.equals(st)) {
				return new SimpleValueSupport(st, Short.valueOf(value));
			} else if (SimpleMetaType.SHORT_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Short.parseShort(value));
			} else if (SimpleMetaType.STRING.equals(st)) {
				return new SimpleValueSupport(st,value);
			}
		}
		throw new TeiidRuntimeException(IntegrationPlugin.Util.getString("failed_to_convert", type.getClassName())); //$NON-NLS-1$
	}
	
	public static void deployArchive(DeploymentManager deploymentManager, String fileName, final InputStream resource, boolean deployExploded) throws AdminProcessingException {
		deployArchive(deploymentManager, fileName, getTempURL(resource), deployExploded);
	}
	
	public static void deployArchive(DeploymentManager deploymentManager, String fileName, URL resourceURL, boolean deployExploded) throws AdminProcessingException {
		List<DeploymentOption> deploymentOptions = new ArrayList<DeploymentOption>();
		if (deployExploded) {
			deploymentOptions.add(DeploymentOption.Explode);
		}
		// try to deploy
		DeploymentProgress progress = null;
		try {
			progress = deploymentManager.distribute(fileName, resourceURL, deploymentOptions.toArray(new DeploymentOption[deploymentOptions.size()]));
			execute(progress, IntegrationPlugin.Util.getString("distribute_failed", fileName)); //$NON-NLS-1$
		} catch (Exception e) {
			handleException(e);
		}
		
		// Now that we've successfully distributed the deployment, we need to
		// start it.
		String[] deploymentNames = progress.getDeploymentID().getRepositoryNames();
		try {
			progress = deploymentManager.start(deploymentNames);
			execute(progress, IntegrationPlugin.Util.getString("deployment_start_failed", fileName)); //$NON-NLS-1$ 
		} catch(Exception e) {
			try {
				// if failed to start remove it.
				execute(deploymentManager.remove(deploymentNames), IntegrationPlugin.Util.getString("failed_to_remove")); //$NON-NLS-1$ 
			} catch (Exception e1) {
				handleException(e1);
			}
			handleException(e);
		}
	}

	static URL getTempURL(final InputStream resource) {
		try {
			return new URL(null, "temp:#temp", new URLStreamHandler() { //$NON-NLS-1$
				
				@Override
				protected URLConnection openConnection(URL u) throws IOException {
					return new URLConnection(u) {
						
						@Override
						public void connect() throws IOException {
							
						}
						
						@Override
						public InputStream getInputStream() throws IOException {
							return resource;
						}
					};
				}
			});
		} catch (MalformedURLException e2) {
			throw new TeiidRuntimeException(e2);
		}
	}

	public static void handleException(Exception e) throws AdminProcessingException {
		if (e instanceof AdminProcessingException) {
			throw (AdminProcessingException)e;
		}
		throw new AdminProcessingException(e.getMessage(), e);
	}

	public static void execute(DeploymentProgress progress, String errorMessage) throws AdminProcessingException {
	    progress.run();
	    DeploymentStatus status =  progress.getDeploymentStatus();
	    
		if (status.isFailed()) {
			if (status.getFailure() != null) {
				throw new AdminProcessingException(status.getFailure().getMessage(), status.getFailure());
			}
			throw new AdminProcessingException(errorMessage);				
		}
	}

	public static void removeArchive(DeploymentManager deploymentManager, String... deploymentNames) throws AdminProcessingException{
		try {
			execute(deploymentManager.stop(deploymentNames), IntegrationPlugin.Util.getString("failed_to_remove")); //$NON-NLS-1$
			execute(deploymentManager.remove(deploymentNames), IntegrationPlugin.Util.getString("failed_to_remove")); //$NON-NLS-1$ 
		} catch (Exception e) {
			handleException(e);
		}
	}
	
	public static MetaValue executeOperation(ManagedCommon mc, String operation, MetaValue... args) {
		for (ManagedOperation mo:mc.getOperations()) {
			if (mo.getName().equals(operation)) {
				return mo.invoke(args);
			}
		}
		throw new TeiidRuntimeException(IntegrationPlugin.Util.getString("no_operation", operation)); //$NON-NLS-1$ 
	}
}
