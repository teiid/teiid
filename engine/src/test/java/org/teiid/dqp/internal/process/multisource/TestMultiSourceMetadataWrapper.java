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

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.teiid.dqp.internal.process.multisource.MultiSourceElement;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;

import junit.framework.TestCase;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.dqp.service.FakeVDBService;
import com.metamatrix.query.unittest.FakeMetadataFactory;


/** 
 * @since 4.2
 */
public class TestMultiSourceMetadataWrapper extends TestCase {

    public void testMultiSourcePseudoElement() throws Exception {
        String vdbName = "My Cool VDB"; //$NON-NLS-1$
        String vdbVersion = "1"; //$NON-NLS-1$
        
        FakeVDBService vdbService = new FakeVDBService();
        vdbService.addModel(vdbName, vdbVersion, "BQT1", ModelInfo.PUBLIC, true); //$NON-NLS-1$
        Collection multiSourceModels = vdbService.getMultiSourceModels(vdbName, vdbVersion);
        MultiSourceMetadataWrapper wrapper = new MultiSourceMetadataWrapper(FakeMetadataFactory.exampleBQTCached(), multiSourceModels);
        
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
        assertEquals(MultiSourceElement.MULTI_SOURCE_ELEMENT_NAME, wrapper.getShortElementName(fullName));
        assertEquals(fullName, wrapper.getFullElementName(wrapper.getFullName(groupID), MultiSourceElement.MULTI_SOURCE_ELEMENT_NAME));
    }
}
