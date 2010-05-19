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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.metatype.api.types.MetaType;

public class MeasurementAdapterFactory
{

    private static final Log LOG = LogFactory.getLog(MeasurementAdapterFactory.class);

    private static Map<String, String> customAdapters = new HashMap<String, String>();

    {
        // Add customAdapters to the map
        customAdapters.put("NoClasses", "NoClasses");
    }

    public static MeasurementAdapter getMeasurementPropertyAdapter(MetaType metaType)
    {
        MeasurementAdapter measurementAdapter = null;
        if (metaType.isSimple())
        {
          //  measurementAdapter = new SimpleMetaValueMeasurementAdapter();
        }
        else if (metaType.isGeneric())
        {
            measurementAdapter = null;
        }
        else if (metaType.isComposite())
        {
            measurementAdapter = null;
        }
        else if (metaType.isTable())
        {
            measurementAdapter = null;
        }
        else if (metaType.isCollection())
        {
            measurementAdapter = null;
        }
        else if (metaType.isArray())
        {
            measurementAdapter = null;
        }

        return measurementAdapter;
    }

    public static MeasurementAdapter getCustomMeasurementPropertyAdapter(String measurementName)
    {
        MeasurementAdapter measurementAdapter = null;
        String adapterClassName = customAdapters.get(measurementName);
        if (adapterClassName != null && !adapterClassName.equals(""))
        {
            try
            {
                measurementAdapter = (MeasurementAdapter)Class.forName(adapterClassName).newInstance();
            }
            catch (InstantiationException e)
            {
                LOG.error("Cannot instantiate adapter class called " + adapterClassName, e);
            }
            catch (IllegalAccessException e)
            {
                LOG.error("Cannot access adapter class called " + adapterClassName, e);
            }
            catch (ClassNotFoundException e)
            {
                LOG.error("Cannot find adapter class called " + adapterClassName, e);
            }
        }
        return measurementAdapter;
    }
}
