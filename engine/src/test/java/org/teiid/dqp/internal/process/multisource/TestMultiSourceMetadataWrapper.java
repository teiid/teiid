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

package org.teiid.dqp.internal.process.multisource;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestMultiSourceMetadataWrapper {

    @Test public void testMultiSourcePseudoElement() throws Exception {
        HashSet<String> multiSourceModels = new HashSet<String>();
        multiSourceModels.add("BQT1");
        MultiSourceMetadataWrapper wrapper = new MultiSourceMetadataWrapper(RealMetadataFactory.exampleBQTCached(), multiSourceModels);
        
        Object groupID = wrapper.getGroupID("BQT1.SmallA"); //$NON-NLS-1$
        List elements = wrapper.getElementIDsInGroupID(groupID);
        assertEquals(18, elements.size());
        
        Object instanceElementID = elements.get(elements.size()-1);
        assertTrue(instanceElementID instanceof MultiSourceElement);
        String fullName = wrapper.getFullName(instanceElementID);
        assertEquals("BQT1.SmallA." + MultiSourceElement.MULTI_SOURCE_ELEMENT_NAME, fullName); //$NON-NLS-1$
        
        assertEquals(instanceElementID, wrapper.getElementID(fullName));
        
        assertEquals(groupID, wrapper.getGroupIDForElementID(instanceElementID));
        assertEquals(null, wrapper.getMaximumValue(instanceElementID));
        assertEquals(null, wrapper.getMinimumValue(instanceElementID));
        assertEquals(wrapper.getModelID(groupID), wrapper.getModelID(instanceElementID));
        assertEquals(null, wrapper.getDefaultValue(instanceElementID));
        assertEquals(255, wrapper.getElementLength(instanceElementID));
        assertEquals(DataTypeManager.DefaultDataTypes.STRING, wrapper.getElementType(instanceElementID)); 
        assertEquals(new Properties(), wrapper.getExtensionProperties(instanceElementID));
        assertEquals(null, wrapper.getNameInSource(instanceElementID));
        assertEquals(null, wrapper.getNativeType(instanceElementID));
        assertEquals(18, wrapper.getPosition(instanceElementID));
        assertEquals(0, wrapper.getPrecision(instanceElementID));
        assertEquals(0, wrapper.getScale(instanceElementID));
        assertEquals(0, wrapper.getRadix(instanceElementID));
        assertEquals(MultiSourceElement.MULTI_SOURCE_ELEMENT_NAME, SingleElementSymbol.getShortName(fullName));
        assertEquals(fullName, wrapper.getFullName(groupID) + SingleElementSymbol.SEPARATOR + MultiSourceElement.MULTI_SOURCE_ELEMENT_NAME);
        
        
        TempMetadataAdapter tma = new TempMetadataAdapter(wrapper, new TempMetadataStore());
        ElementSymbol elementSymbol = new ElementSymbol("y");
        elementSymbol.setType(DataTypeManager.DefaultDataClasses.STRING);
		TempMetadataID id = tma.getMetadataStore().addTempGroup("x", Arrays.asList(elementSymbol));
        
        assertFalse(tma.isMultiSourceElement(id.getElements().get(0)));
        assertTrue(tma.isMultiSourceElement(instanceElementID));
    }
}
