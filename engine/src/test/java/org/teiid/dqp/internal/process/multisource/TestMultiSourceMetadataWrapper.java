/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestMultiSourceMetadataWrapper {

    @Test public void testMultiSourcePseudoElement() throws Exception {
        HashSet<String> multiSourceModels = new HashSet<String>();
        multiSourceModels.add("BQT1");
        MultiSourceMetadataWrapper wrapper = new MultiSourceMetadataWrapper(RealMetadataFactory.exampleBQTCached(), multiSourceModels);

        Object groupID = wrapper.getGroupID("BQT1.SmallA"); //$NON-NLS-1$
        List<?> elements = wrapper.getElementIDsInGroupID(groupID);
        assertEquals(18, elements.size());

        Object instanceElementID = elements.get(elements.size()-1);
        String fullName = wrapper.getFullName(instanceElementID);
        assertEquals("BQT1.SmallA." + MultiSourceElement.DEFAULT_MULTI_SOURCE_ELEMENT_NAME, fullName); //$NON-NLS-1$

        assertEquals(instanceElementID, wrapper.getElementID(fullName));

        assertEquals(groupID, wrapper.getGroupIDForElementID(instanceElementID));
        assertEquals(null, wrapper.getMaximumValue(instanceElementID));
        assertEquals(null, wrapper.getMinimumValue(instanceElementID));
        assertEquals(wrapper.getModelID(groupID), wrapper.getModelID(instanceElementID));
        assertEquals(null, wrapper.getDefaultValue(instanceElementID));
        assertEquals(255, wrapper.getElementLength(instanceElementID));
        assertEquals(DataTypeManager.DefaultDataTypes.STRING, wrapper.getElementRuntimeTypeName(instanceElementID));
        assertEquals(new Properties(), wrapper.getExtensionProperties(instanceElementID));
        assertEquals(null, wrapper.getNameInSource(instanceElementID));
        assertEquals(null, wrapper.getNativeType(instanceElementID));
        assertEquals(18, wrapper.getPosition(instanceElementID));
        assertEquals(0, wrapper.getPrecision(instanceElementID));
        assertEquals(0, wrapper.getScale(instanceElementID));
        assertEquals(0, wrapper.getRadix(instanceElementID));
        assertEquals(MultiSourceElement.DEFAULT_MULTI_SOURCE_ELEMENT_NAME, Symbol.getShortName(fullName));
        assertEquals(fullName, wrapper.getFullName(groupID) + Symbol.SEPARATOR + MultiSourceElement.DEFAULT_MULTI_SOURCE_ELEMENT_NAME);

        TempMetadataAdapter tma = new TempMetadataAdapter(wrapper, new TempMetadataStore());
        ElementSymbol elementSymbol = new ElementSymbol("y");
        elementSymbol.setType(DataTypeManager.DefaultDataClasses.STRING);
        TempMetadataID id = tma.getMetadataStore().addTempGroup("x", Arrays.asList(elementSymbol));

        assertFalse(tma.isMultiSourceElement(id.getElements().get(0)));
        assertTrue(tma.isMultiSourceElement(instanceElementID));

        assertTrue(tma.isPseudo(instanceElementID));

        assertEquals(17, tma.getElementIDsInGroupID(tma.getGroupID("VQT.Smalla")).size());
    }
}
