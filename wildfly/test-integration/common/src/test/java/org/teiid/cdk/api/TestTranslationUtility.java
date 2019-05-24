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

package org.teiid.cdk.api;

import junit.framework.TestCase;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.metadata.AbstractMetadataRecord;

public class TestTranslationUtility extends TestCase {

    /**
     * Constructor for TestTranslationUtility.
     * @param name
     */
    public TestTranslationUtility(String name) {
        super(name);
    }

    public String getTestVDB() {
        return UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb"; //$NON-NLS-1$
    }

    public void helpTestTranslate(String sql, String expectedOutput) {
        TranslationUtility util = new TranslationUtility(getTestVDB());
        Command query = util.parseCommand(sql);
        assertEquals(expectedOutput, query.toString());
    }

    public void testQuery1() {
        helpTestTranslate(
            "select * from partssupplier.parts", //$NON-NLS-1$
            "SELECT PARTS.PART_ID, PARTS.PART_NAME, PARTS.PART_COLOR, PARTS.PART_WEIGHT FROM PARTS"); //$NON-NLS-1$
    }

    public void testInsert1() {
        helpTestTranslate(
            "insert into partssupplier.parts (part_name, part_color) values ('P100', 'Red')", //$NON-NLS-1$
            "INSERT INTO PARTS (PART_NAME, PART_COLOR) VALUES ('P100', 'Red')"); //$NON-NLS-1$
    }

    public void testUpdate1() {
        helpTestTranslate(
            "update partssupplier.parts set part_name = 'P100' where part_color = 'Red'", //$NON-NLS-1$
            "UPDATE PARTS SET PART_NAME = 'P100' WHERE PARTS.PART_COLOR = 'Red'"); //$NON-NLS-1$
    }

    public void testDelete1() {
        helpTestTranslate(
            "delete from partssupplier.parts where part_color = 'Red'", //$NON-NLS-1$
            "DELETE FROM PARTS WHERE PARTS.PART_COLOR = 'Red'"); //$NON-NLS-1$
    }

    public void testGetRMD() throws Exception {
        TranslationUtility util = new TranslationUtility(getTestVDB());

        // Translate command to get some ids
        Select query = (Select) util.parseCommand("select * from partssupplier.parts"); //$NON-NLS-1$
        NamedTable group = (NamedTable) query.getFrom().get(0);
        AbstractMetadataRecord mid = group.getMetadataObject();
        assertEquals("PartsSupplier.PARTSSUPPLIER.PARTS", mid.getFullName()); //$NON-NLS-1$

        // Use RMD to get stuff
        assertEquals("PARTS", mid.getNameInSource()); //$NON-NLS-1$
    }
}
