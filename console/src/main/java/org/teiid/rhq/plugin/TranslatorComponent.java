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
package org.teiid.rhq.plugin;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.profileservice.spi.NoSuchDeploymentException;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.ConfigurationDefinition;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.domain.resource.CreateResourceStatus;
import org.rhq.core.domain.resource.ResourceType;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.core.pluginapi.measurement.MeasurementFacet;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;

/**
 * Component class for the MetaMatrix Host Controller process.
 * 
 */
public class TranslatorComponent extends Facet {
	private final Log LOG = LogFactory.getLog(TranslatorComponent.class);

	public static interface Config {
		String COMPONENT_TYPE = "componentType";
		String COMPONENT_SUBTYPE = "componentSubtype";
		String COMPONENT_NAME = "componentName";
		String TEMPLATE_NAME = "template-name";
		String RESOURCE_NAME = "resourceName";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.teiid.rhq.plugin.Facet#start(org.rhq.core.pluginapi.inventory.
	 * ResourceContext)
	 */
	@Override
	public void start(ResourceContext context) {
		this.setComponentName(context.getPluginConfiguration().getSimpleValue(
				"name", null));
		this.resourceConfiguration=context.getPluginConfiguration();
		super.start(context);
	}
	
	/**
	 * @see org.teiid.rhq.plugin.Facet#getComponentType()
	 * @since 1.0
	 */
	@Override
	String getComponentType() {
		return PluginConstants.ComponentType.Translator.NAME;
	}

	/**
	 * The plugin container will call this method when your resource component
	 * has been scheduled to collect some measurements now. It is within this
	 * method that you actually talk to the managed resource and collect the
	 * measurement data that is has emitted.
	 * 
	 * @see MeasurementFacet#getValues(MeasurementReport, Set)
	 */
	public void getValues(MeasurementReport report,
			Set<MeasurementScheduleRequest> requests) {
		for (MeasurementScheduleRequest request : requests) {
			String name = request.getName();

			// TODO: based on the request information, you must collect the
			// requested measurement(s)
			// you can use the name of the measurement to determine what you
			// actually need to collect
			try {
				Number value = new Integer(1); // dummy measurement value -
				// this should come from the
				// managed resource
				report.addData(new MeasurementDataNumeric(request, value
						.doubleValue()));
			} catch (Exception e) {
				LOG.error("Failed to obtain measurement [" + name //$NON-NLS-1$
						+ "]. Cause: " + e); //$NON-NLS-1$
			}
		}

		return;
	}
	
	protected void setOperationArguments(String name,
			Configuration configuration, Map argumentMap) {

		if (name
				.equals(ConnectionConstants.ComponentType.Operation.GET_PROPERTIES)) {
			String key = ConnectionConstants.IDENTIFIER;
			argumentMap.put(key, getComponentIdentifier());
		}

	}
	
	/* (non-Javadoc)
	 * @see org.teiid.rhq.plugin.Facet#loadResourceConfiguration()
	 */
	@Override
	public Configuration loadResourceConfiguration() {

		ManagedComponent translator = null;
		try {
			translator = ProfileServiceUtil
			.getManagedComponent(new ComponentType(
					PluginConstants.ComponentType.Translator.TYPE,
					PluginConstants.ComponentType.Translator.SUBTYPE), this.name);
		} catch (NamingException e) {
			final String msg = "NamingException in loadResourceConfiguration(): " + e.getExplanation(); //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e) {
			final String msg = "Exception in loadResourceConfiguration(): " + e.getMessage(); //$NON-NLS-1$
			LOG.error(msg, e);
		}
		
		String translatorName = ProfileServiceUtil.getSimpleValue(
				translator, "name", String.class);

		Configuration c = resourceConfiguration;
		PropertyList list = new PropertyList("translatorList");
		PropertyMap propMap = null;
		c.put(list);

		// First get translator specific properties
		ManagedProperty translatorProps = translator
				.getProperty("translator-property");
		getTranslatorValues(translatorProps.getValue(), propMap, list);

		// Now get common properties
		c.put(new PropertySimple("name", translatorName));
		c.put(new PropertySimple("execution-factory-class",
				ProfileServiceUtil.getSimpleValue(translator,
						"execution-factory-class", String.class)));
		c.put(new PropertySimple("immutable", ProfileServiceUtil
				.getSimpleValue(translator, "immutable", Boolean.class)));
		c.put(new PropertySimple("xa-capable", ProfileServiceUtil
				.getSimpleValue(translator, "xa-capable", Boolean.class)));
		c.put(new PropertySimple("exception-on-max-rows",
				ProfileServiceUtil.getSimpleValue(translator,
						"exception-on-max-rows", Boolean.class)));
		c.put(new PropertySimple("max-result-rows", ProfileServiceUtil
				.getSimpleValue(translator, "max-result-rows",
						Integer.class)));
		c.put(new PropertySimple("template-name", ProfileServiceUtil
						.getSimpleValue(translator, "template-name",
								String.class)));

		return c;

	}
	
	public static <T> void getTranslatorValues(MetaValue pValue,
			PropertyMap map, PropertyList list) {
		MetaType metaType = pValue.getMetaType();
		Map<String, T> unwrappedvalue = null;
		if (metaType.isComposite()) {
			unwrappedvalue = (Map<String, T>) MetaValueFactory
					.getInstance().unwrap(pValue);

			for (String key : unwrappedvalue.keySet()) {
				map = new PropertyMap("translator-properties");
				map.put(new PropertySimple("name", key));
				map.put(new PropertySimple("value", unwrappedvalue.get(key)));
				map.put(new PropertySimple("description", "Custom property"));
				list.add(map);
			}
		} else {
			throw new IllegalStateException(pValue + " is not a Composite type");
		}

	}

}