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
package org.teiid.translator.odata4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;

public class TestODataUpdateVisitor {
    /*
    private void helpFunctionExecute(String query, String expected) throws Exception {
        Call cmd = (Call)this.utility.parseCommand(query);
        ODataProcedureVisitor visitor = new ODataProcedureVisitor(translator, utility.createRuntimeMetadata());
        visitor.visitNode(cmd); 
        String odataCmd = visitor.buildURL();
        
        assertEquals(expected, odataCmd);
        assertEquals("GET", visitor.getMethod());
    }
    
    @Test
    public void testProcedureExec() throws Exception {
        helpFunctionExecute("Exec TopCustomers('newyork')", "TopCustomers?city='newyork'");
    }    
    
    private void helpUpdateExecute(String query, String expected, String expectedMethod, boolean checkPayload) throws Exception {
        Command cmd = this.utility.parseCommand(query);
        
        ODataUpdateVisitor visitor = new ODataUpdateVisitor(translator, utility.createRuntimeMetadata());
        visitor.visitNode(cmd); 
        
        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }
        
        String odataCmd = visitor.buildURL();
        
        if (checkPayload) {
            assertNotNull(visitor.getPayload());
        }
        
        if (printPayload) {
            System.out.println(visitor.getPayload());
        }
        
        assertEquals(expected, odataCmd);
        assertEquals(expectedMethod, visitor.getMethod());
    }   
    
    @Test
    public void testInsert() throws Exception {
        helpUpdateExecute("INSERT INTO Regions (RegionID,RegionDescription) VALUES (10,'Asian')", "Regions", "POST", true);
    }     
    
    @Test(expected=TranslatorException.class)
    public void testDeletewithoutPK() throws Exception {
        helpUpdateExecute("Delete From Regions", "Regions", "DELETE", false);
    }    
    
    @Test
    public void testDelete() throws Exception {
        helpUpdateExecute("Delete From Regions where RegionID=10", "Regions(10)", "DELETE", false);
    }     
    
    @Test(expected=TranslatorException.class)
    public void testDeleteOtherClause() throws Exception {
        helpUpdateExecute("Delete From Regions where RegionDescription='foo'", "Regions", "DELETE", false);
    }     
    
    @Test
    public void testUpdate() throws Exception {
        helpUpdateExecute("UPDATE Regions SET RegionDescription='foo' WHERE RegionID=10", "Regions(10)", "PUT", true);
    }     
    
    @Test(expected=TranslatorException.class)
    public void testUpdatewithoutPK() throws Exception {
        helpUpdateExecute("UPDATE Regions SET RegionDescription='foo'", "Regions(10)", "PATCH", true);
    }   
    
    @Test(expected=TranslatorException.class)
    public void testUpdateOtherClause() throws Exception {
        helpUpdateExecute("UPDATE Regions SET RegionID=10 WHERE RegionDescription='foo'", "Regions(10)", "PATCH", true);
    }
    */
}
