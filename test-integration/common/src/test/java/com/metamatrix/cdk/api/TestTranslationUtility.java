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

package com.metamatrix.cdk.api;

import junit.framework.TestCase;

import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.MetadataObject;

import com.metamatrix.core.util.UnitTestUtil;

public class TestTranslationUtility extends TestCase {

    /**
     * Constructor for TestTranslationUtility.
     * @param name
     */
    public TestTranslationUtility(String name) {
        super(name);
    }

    public String getTestVDB() {
        return UnitTestUtil.getTestDataPath() + "/partssupplier/PartsSupplier.vdb"; //$NON-NLS-1$
    }
    
    public void helpTestTranslate(String vdbFile, String sql, String expectedOutput) {
        TranslationUtility util = new TranslationUtility(getTestVDB());
        ICommand query = util.parseCommand(sql);         
        assertEquals(expectedOutput, query.toString());        
    }

    public void testQuery1() {
        helpTestTranslate(
            getTestVDB(), 
            "select * from partssupplier.parts", //$NON-NLS-1$
            "SELECT PARTS.PART_ID, PARTS.PART_NAME, PARTS.PART_COLOR, PARTS.PART_WEIGHT FROM PARTS"); //$NON-NLS-1$
    }
        
    public void testInsert1() {
        helpTestTranslate(
            getTestVDB(), 
            "insert into partssupplier.parts (part_name, part_color) values ('P100', 'Red')", //$NON-NLS-1$
            "INSERT INTO PARTS (PART_NAME, PART_COLOR) VALUES ('P100', 'Red')"); //$NON-NLS-1$
    }
    
    public void testUpdate1() {
        helpTestTranslate(
            getTestVDB(), 
            "update partssupplier.parts set part_name = 'P100' where part_color = 'Red'", //$NON-NLS-1$
            "UPDATE PARTS SET PART_NAME = 'P100' WHERE PARTS.PART_COLOR = 'Red'"); //$NON-NLS-1$
    }

    public void testDelete1() {
        helpTestTranslate(
            getTestVDB(), 
            "delete from partssupplier.parts where part_color = 'Red'", //$NON-NLS-1$
            "DELETE FROM PARTS WHERE PARTS.PART_COLOR = 'Red'"); //$NON-NLS-1$
    }

    public void testGetRMD() throws Exception {
        TranslationUtility util = new TranslationUtility(getTestVDB());
        
        // Translate command to get some ids
        IQuery query = (IQuery) util.parseCommand("select * from partssupplier.parts"); //$NON-NLS-1$
        IGroup group = (IGroup) query.getFrom().getItems().get(0);
        MetadataObject mid = group.getMetadataObject();
        assertEquals("PartsSupplier.PARTSSUPPLIER.PARTS", mid.getFullName()); //$NON-NLS-1$
        
        // Use RMD to get stuff
        assertEquals("PARTS", mid.getNameInSource()); //$NON-NLS-1$
    }
}
