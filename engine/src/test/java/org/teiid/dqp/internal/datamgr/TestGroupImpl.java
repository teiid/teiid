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

package org.teiid.dqp.internal.datamgr;


import junit.framework.TestCase;

import org.teiid.language.NamedTable;
import org.teiid.query.sql.symbol.GroupSymbol;


public class TestGroupImpl extends TestCase {

    /**
     * Constructor for TestGroupImpl.
     * @param name
     */
    public TestGroupImpl(String name) {
        super(name);
    }

    public static GroupSymbol helpExample(String groupName) {
        return helpExample(groupName, null);
    }

    public static GroupSymbol helpExample(String groupName, String definition) {
        return helpExample(groupName, definition, null);
    }

    public static GroupSymbol helpExample(String groupName, String definition, Object metadataID) {
        GroupSymbol symbol = new GroupSymbol(groupName, definition);
        if (metadataID != null) {
            symbol.setMetadataID(metadataID);
        }
        return symbol;
    }

    public static NamedTable example(String groupName) throws Exception {
        return example(groupName, null);
    }

    public static NamedTable example(String groupName, String definition) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(groupName, definition));
    }

    public static NamedTable example(String groupName, String definition, Object metadataID) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(groupName, definition, metadataID));
    }

    public void testGetContext() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getGroupID("pm1.g1"); //$NON-NLS-1$
        assertEquals("x", example("x", "pm1.g1", metadataID).getCorrelationName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testGetDefinition() throws Exception {
        Object metadataID = TstLanguageBridgeFactory.metadata.getGroupID("pm1.g1"); //$NON-NLS-1$
        assertEquals("g1", example("x", "pm1.g1", metadataID).getName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

}
