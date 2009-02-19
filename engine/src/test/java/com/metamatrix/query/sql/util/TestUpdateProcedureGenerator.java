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

package com.metamatrix.query.sql.util;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.unittest.FakeMetadataStore;

/**

 */
public class TestUpdateProcedureGenerator extends TestCase{
	
	public TestUpdateProcedureGenerator(String name) { 
		super(name);
	}	

	// ################################## TEST HELPERS ################################
	
	private void helpTest(int procedureType, String vGroup, String sql, QueryMetadataInterface md, String expectedProc) { 	
		try {
			Command command = QueryParser.getQueryParser().parseCommand(sql);
			QueryResolver.resolveCommand(command, md);
			
	        CreateUpdateProcedureCommand actualProc = UpdateProcedureGenerator.createProcedure(procedureType, vGroup, command, md);
	        if (expectedProc == null) {
	        	assertNull(actualProc);
	        } else {
	        	assertEquals("Didn't get expected generated procedure", expectedProc, actualProc.toString()); //$NON-NLS-1$
		        QueryParser.getQueryParser().parseCommand(actualProc.toString());
	        }
		} catch (MetaMatrixException e) {
			throw new RuntimeException(e);
		}
	}

 	public static FakeMetadataFacade example1() { 
 		return example1(true);
 	}

 	public static FakeMetadataFacade example1(boolean allUpdatable) { 
		// Create models
		FakeMetadataObject pm1 = FakeMetadataFactory.createPhysicalModel("pm1"); //$NON-NLS-1$
		FakeMetadataObject vm1 = FakeMetadataFactory.createVirtualModel("vm1");	 //$NON-NLS-1$

		// Create physical groups
		FakeMetadataObject pm1g1 = FakeMetadataFactory.createPhysicalGroup("pm1.g1", pm1); //$NON-NLS-1$
		FakeMetadataObject pm1g2 = FakeMetadataFactory.createPhysicalGroup("pm1.g2", pm1); //$NON-NLS-1$
        FakeMetadataObject pm1g3 = FakeMetadataFactory.createPhysicalGroup("pm1.g3", pm1); //$NON-NLS-1$
				
		// Create physical elements
		List pm1g1e = FakeMetadataFactory.createElements(pm1g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		if (!allUpdatable) {
			((FakeMetadataObject)pm1g1e.get(0)).putProperty(FakeMetadataObject.Props.UPDATE, Boolean.FALSE);
		}
		
		List pm1g2e = FakeMetadataFactory.createElements(pm1g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

        List pm1g3e = FakeMetadataFactory.createElements(pm1g3, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        ((FakeMetadataObject)pm1g3e.get(0)).putProperty(FakeMetadataObject.Props.NULL, Boolean.FALSE);
        ((FakeMetadataObject)pm1g3e.get(0)).putProperty(FakeMetadataObject.Props.DEFAULT_VALUE, null);
        
        ((FakeMetadataObject)pm1g3e.get(1)).putProperty(FakeMetadataObject.Props.NULL, Boolean.FALSE);
        ((FakeMetadataObject)pm1g3e.get(1)).putProperty(FakeMetadataObject.Props.AUTO_INCREMENT, Boolean.TRUE);
        ((FakeMetadataObject)pm1g3e.get(1)).putProperty(FakeMetadataObject.Props.DEFAULT_VALUE, null);
        
        ((FakeMetadataObject)pm1g3e.get(2)).putProperty(FakeMetadataObject.Props.NULL, Boolean.FALSE);
        ((FakeMetadataObject)pm1g3e.get(2)).putProperty(FakeMetadataObject.Props.DEFAULT_VALUE, "xyz"); //$NON-NLS-1$

		// Create virtual groups
		QueryNode vm1g1n1 = new QueryNode("vm1.g1", "SELECT e1 as a, e2 FROM pm1.g1 WHERE e3 > 5"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g1 = FakeMetadataFactory.createUpdatableVirtualGroup("vm1.g1", vm1, vm1g1n1); //$NON-NLS-1$
		QueryNode vm1g2n1 = new QueryNode("vm1.g2", "SELECT e1, e2, e3, e4 FROM pm1.g2 WHERE e3 > 5"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g2 = FakeMetadataFactory.createUpdatableVirtualGroup("vm1.g2", vm1, vm1g2n1); //$NON-NLS-1$
        QueryNode vm1g3n1 = new QueryNode("vm1.g3", "SELECT e1, e3 FROM pm1.g3"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g3 = FakeMetadataFactory.createUpdatableVirtualGroup("vm1.g3", vm1, vm1g3n1); //$NON-NLS-1$
        QueryNode vm1g4n1 = new QueryNode("vm1.g4", "SELECT e1, e2 FROM pm1.g3"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g4 = FakeMetadataFactory.createUpdatableVirtualGroup("vm1.g4", vm1, vm1g4n1); //$NON-NLS-1$
        QueryNode vm1g5n1 = new QueryNode("vm1.g5", "SELECT e2, e3 FROM pm1.g3"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g5 = FakeMetadataFactory.createUpdatableVirtualGroup("vm1.g5", vm1, vm1g5n1); //$NON-NLS-1$

		// Create virtual elements
		List vm1g1e = FakeMetadataFactory.createElements(vm1g1, 
			new String[] { "a", "e2"}, //$NON-NLS-1$ //$NON-NLS-2$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER});
		List vm1g2e = FakeMetadataFactory.createElements(vm1g2, 
			new String[] { "e1", "e2","e3", "e4"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g3e = FakeMetadataFactory.createElements(vm1g3, 
            new String[] { "e1", "e2"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER,  });
        List vm1g4e = FakeMetadataFactory.createElements(vm1g4, 
            new String[] { "e1", "e3"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.BOOLEAN });
        List vm1g5e = FakeMetadataFactory.createElements(vm1g5, 
            new String[] { "e2","e3"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN });

        // Stored queries
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs1p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$
        QueryNode sq1n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs1p1 }), sq1n1);  //$NON-NLS-1$

		// Add all objects to the store
		FakeMetadataStore store = new FakeMetadataStore();
		store.addObject(pm1);
		store.addObject(pm1g1);		
		store.addObjects(pm1g1e);
      	store.addObject(pm1g2);		
		store.addObjects(pm1g2e);
        store.addObject(pm1g3);     
        store.addObjects(pm1g3e);
		
		store.addObject(vm1);
		store.addObject(vm1g1);
		store.addObjects(vm1g1e);
		store.addObject(vm1g2);
		store.addObjects(vm1g2e);
        store.addObject(vm1g3);
        store.addObjects(vm1g3e);
        store.addObject(vm1g4);
        store.addObjects(vm1g4e);
        store.addObject(vm1g5);
        store.addObjects(vm1g5e);
        
        store.addObject(rs1);
        store.addObject(sq1);

		// Create the facade from the store
		return new FakeMetadataFacade(store);
	}	
 	
	//actual tests
	public void testCreateInsertCommand(){
		helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE, 
			"vm1.g1", //$NON-NLS-1$
			"select e1 as a, e2 from pm1.g1 where e4 > 5",              //$NON-NLS-1$
		    TestUpdateProcedureGenerator.example1(),
            "CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = INSERT INTO pm1.g1 (pm1.g1.e1, pm1.g1.e2) VALUES (INPUT.a, INPUT.e2);\nEND"); //$NON-NLS-1$
	}
	
	public void testCreateInsertCommand2(){ //put a constant in select statement
		helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE, 
			"vm1.g1", //$NON-NLS-1$
			"select e1 as a, 5 from pm1.g1 where e4 > 5",              //$NON-NLS-1$
		    TestUpdateProcedureGenerator.example1(),
            "CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = INSERT INTO pm1.g1 (pm1.g1.e1) VALUES (INPUT.a);\nEND"); //$NON-NLS-1$
	}
	
	public void testCreateInsertCommand3(){ 
		helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE, 
			"vm1.g2", //$NON-NLS-1$
			"select * from pm1.g2 where e4 > 5",              //$NON-NLS-1$
		    TestUpdateProcedureGenerator.example1(),
            "CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = INSERT INTO pm1.g2 (pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4) VALUES (INPUT.e1, INPUT.e2, INPUT.e3, INPUT.e4);\nEND"); //$NON-NLS-1$
	}
	
	public void testCreateInsertCommand4(){ //test group alias
		helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE, 
			"vm1.g2", //$NON-NLS-1$
			"select * from pm1.g2 as g_alias",              //$NON-NLS-1$
			TestUpdateProcedureGenerator.example1(),
			"CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = INSERT INTO pm1.g2 (pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4) VALUES (INPUT.e1, INPUT.e2, INPUT.e3, INPUT.e4);\nEND"); //$NON-NLS-1$
	}	

	public void testCreateInsertCommand5(){
		helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE, 
			"vm1.g1", //$NON-NLS-1$
			"select e1 as a, e2 from pm1.g1 as g_alias where e4 > 5",              //$NON-NLS-1$
			TestUpdateProcedureGenerator.example1(),
			"CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = INSERT INTO pm1.g1 (pm1.g1.e1, pm1.g1.e2) VALUES (INPUT.a, INPUT.e2);\nEND"); //$NON-NLS-1$
	}
		
	public void testCreateUpdateCommand(){
		helpTest(UpdateProcedureGenerator.UPDATE_PROCEDURE, 
			"vm1.g1", //$NON-NLS-1$
			"select e1 as a, e2 from pm1.g1 where e4 > 5",              //$NON-NLS-1$
		    TestUpdateProcedureGenerator.example1(),
            "CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = UPDATE pm1.g1 SET e1 = INPUT.a, e2 = INPUT.e2 WHERE TRANSLATE CRITERIA;\nEND"); //$NON-NLS-1$
	}
	
	public void testCreateDeleteCommand(){
		helpTest(UpdateProcedureGenerator.DELETE_PROCEDURE, 
			"vm1.g1", //$NON-NLS-1$
			"select e1 as a, e2 from pm1.g1 where e4 > 5",              //$NON-NLS-1$
		    TestUpdateProcedureGenerator.example1(),
            "CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = DELETE FROM pm1.g1 WHERE TRANSLATE CRITERIA;\nEND"); //$NON-NLS-1$
	}

    public void testCreateInsertCommand1_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    public void testCreateInsertCommand2_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "SELECT CONCAT(pm1.g1.e1, convert(pm1.g2.e1, string)) as x FROM pm1.g1, pm1.g2", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    public void testCreateInsertCommand3_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "SELECT e1 FROM pm1.g1 UNION SELECT e1 FROM pm1.g2", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    public void testCreateInsertCommand4_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "SELECT COUNT(*) FROM pm1.g1", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    public void testCreateInsertCommand5_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "SELECT * FROM pm1.g1 GROUP BY e1", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    public void testCreateInsertCommand6_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "EXEC pm1.sq1()", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    public void testCreateInsertCommand7_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "INSERT INTO pm1.g1 (e1) VALUES ('x')", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    public void testCreateInsertCommand8_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "UPDATE pm1.g1 SET e1='x'", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    public void testCreateInsertCommand9_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "DELETE FROM pm1.g1", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    public void testCreateInsertCommand10_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "SELECT COUNT(*) FROM pm1.g1", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    public void testCreateInsertCommand11_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "SELECT COUNT(e1) as x FROM pm1.g1", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }
    
    public void testCreateInsertCommand12_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE, 
            "vm1.g1", //$NON-NLS-1$
            "SELECT * FROM (EXEC pm1.sq1()) AS a",              //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }    

    public void testCreateInsertCommand13_fail(){
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE, 
            "vm1.g1", //$NON-NLS-1$
            "SELECT 1",              //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }    
    
    // Check that e3 is not required (it has a default value)
    public void testRequiredElements1() {
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g3", //$NON-NLS-1$
            "SELECT e1, e2 FROM pm1.g3", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            "CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = INSERT INTO pm1.g3 (pm1.g3.e1, pm1.g3.e2) VALUES (INPUT.e1, INPUT.e2);\nEND"); //$NON-NLS-1$
    }

    // Check that e2 is not required (it is auto-incremented)
    public void testRequiredElements2() {
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g4", //$NON-NLS-1$
            "SELECT e1, e3 FROM pm1.g3", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            "CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = INSERT INTO pm1.g3 (pm1.g3.e1, pm1.g3.e3) VALUES (INPUT.e1, INPUT.e3);\nEND"); //$NON-NLS-1$
    }

    // Check that e1 is required (it is not-nullable, not auto-incrementable, and has no default value)
    public void testRequiredElements3() {
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g5", //$NON-NLS-1$
            "SELECT e2, e3 FROM pm1.g3", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(),
            null);
    }

    // Verify that elements that are not updateable are exlcluded from update and delete procedures
    public void testNonUpdateableElements() {
        helpTest(UpdateProcedureGenerator.UPDATE_PROCEDURE, 
                    "vm1.g1", //$NON-NLS-1$
                    "select e1 as a, e2 from pm1.g1 where e4 > 5",              //$NON-NLS-1$
                    TestUpdateProcedureGenerator.example1(false),
                    "CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = UPDATE pm1.g1 SET e2 = INPUT.e2 WHERE TRANSLATE CRITERIA;\nEND"); //$NON-NLS-1$
	}
	
    // Verify that elements that are not updateable are exlcluded from update and delete procedures
    public void testNonUpdateableElements2() {
        helpTest(UpdateProcedureGenerator.INSERT_PROCEDURE,
            "vm1.g1", //$NON-NLS-1$
            "SELECT e1, e2 FROM pm1.g1", //$NON-NLS-1$
            TestUpdateProcedureGenerator.example1(false),
            "CREATE PROCEDURE\nBEGIN\nROWS_UPDATED = INSERT INTO pm1.g1 (pm1.g1.e2) VALUES (INPUT.e2);\nEND"); //$NON-NLS-1$
    }

}
