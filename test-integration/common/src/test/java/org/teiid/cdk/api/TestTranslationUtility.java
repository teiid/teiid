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
