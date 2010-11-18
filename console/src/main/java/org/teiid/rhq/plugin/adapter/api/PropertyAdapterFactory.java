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
package org.teiid.rhq.plugin.adapter.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.metatype.api.types.MapCompositeMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.PropertiesMetaType;
import org.jboss.metatype.api.values.MetaValue;
import org.rhq.core.domain.configuration.PropertySimple;
import org.teiid.rhq.plugin.adapter.impl.PropertyListToCollectionValueAdapter;
import org.teiid.rhq.plugin.adapter.impl.PropertyMapToCompositeValueSupportAdapter;
import org.teiid.rhq.plugin.adapter.impl.PropertyMapToGenericValueAdapter;
import org.teiid.rhq.plugin.adapter.impl.PropertyMapToMapCompositeValueSupportAdapter;
import org.teiid.rhq.plugin.adapter.impl.PropertyMapToPropertiesValueAdapter;
import org.teiid.rhq.plugin.adapter.impl.PropertyMapToTableValueAdapter;
import org.teiid.rhq.plugin.adapter.impl.PropertySimpleToEnumValueAdapter;
import org.teiid.rhq.plugin.adapter.impl.PropertySimpleToSimpleValueAdapter;
import org.teiid.rhq.plugin.util.PluginConstants;

import com.sun.istack.Nullable;

/**
 * 
 */
public class PropertyAdapterFactory
{
    private static final Log LOG = LogFactory.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

    public static PropertyAdapter getPropertyAdapter(MetaValue metaValue)
    {
        if (metaValue == null)
        {
            LOG.debug("The MetaValue passed in is null.");
            return null;
        }
        MetaType metaType = metaValue.getMetaType();
        return getPropertyAdapter(metaType);
    }

    public static PropertyAdapter getPropertyAdapter(MetaType metaType)
    {
        PropertyAdapter propertyAdapter = null;
        if (metaType.isSimple())
        {
            propertyAdapter = new PropertySimpleToSimpleValueAdapter();
        }
        else if (metaType.isGeneric())
        {
            propertyAdapter = new PropertyMapToGenericValueAdapter();
        }
        else if (metaType.isComposite())
        {
            if (metaType instanceof MapCompositeMetaType)
                propertyAdapter = new PropertyMapToMapCompositeValueSupportAdapter();
            else
                propertyAdapter = new PropertyMapToCompositeValueSupportAdapter();
        }
        else if (metaType.isTable())
        {
            propertyAdapter = new PropertyMapToTableValueAdapter();
        }
        else if (metaType.isCollection())
        {
            propertyAdapter = new PropertyListToCollectionValueAdapter();
        }
        else if (metaType.isEnum())
        {
            propertyAdapter = new PropertySimpleToEnumValueAdapter();
        }
        else if (metaType instanceof PropertiesMetaType)
        {
            propertyAdapter = new PropertyMapToPropertiesValueAdapter();
        }
        else
        {
            LOG.warn("Unsupported MetaType: " + metaType);
        }
        return propertyAdapter;
    }

    @Nullable
    public static PropertyAdapter getCustomPropertyAdapter(PropertySimple customProp)
    {
        if (customProp == null)
            return null;
        String adapterClassName = customProp.getStringValue();
        PropertyAdapter propertyAdapter = null;
        try
        {
            Class adapterClass = Class.forName(adapterClassName);
            propertyAdapter = (PropertyAdapter)adapterClass.newInstance();
        }
        catch (Exception e)
        {
            LOG.error("Unable to create custom adapter class for " + customProp + ".", e);
        }
        return propertyAdapter;
    }
}
