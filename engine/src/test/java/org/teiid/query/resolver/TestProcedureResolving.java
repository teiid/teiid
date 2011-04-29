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

package org.teiid.query.resolver;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Map;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.CreateUpdateProcedureCommand;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.CommandCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.unittest.FakeMetadataFacade;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.unittest.FakeMetadataObject;


public class TestProcedureResolving {

 	private QueryMetadataInterface exampleStoredProcedure(String procedure) {
		FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        
        FakeMetadataObject pm1 = metadata.getStore().findObject("pm1",FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("pm1.rs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode(procedure); 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        metadata.getStore().addObject(rs2);
        metadata.getStore().addObject(sq1);
        
        return metadata;
	}
 	
    private void helpFailUpdateProcedure(String procedure, String userUpdateStr, String procedureType) {
        helpFailUpdateProcedure(procedure, userUpdateStr, procedureType, null);
    }
    
	private void helpFailUpdateProcedure(String procedure, String userUpdateStr, String procedureType, String msg) {
        // resolve
        try {
    		helpResolveUpdateProcedure(procedure, userUpdateStr, procedureType);
            fail("Expected a QueryResolverException but got none."); //$NON-NLS-1$
        } catch(QueryResolverException ex) {
        	if (msg != null) {
                assertEquals(msg, ex.getMessage());
            }
        } catch (TeiidComponentException e) {
        	throw new RuntimeException(e);
		} catch (QueryParserException e) {
			throw new RuntimeException(e);
		} 
	}	
	
    @Test public void testDefect13029_CorrectlySetUpdateProcedureTempGroupIDs() throws Exception {
        StringBuffer proc = new StringBuffer("CREATE VIRTUAL PROCEDURE") //$NON-NLS-1$
            .append("\nBEGIN") //$NON-NLS-1$
            .append("\nDECLARE string var1;") //$NON-NLS-1$
            .append("\nvar1 = '';") //$NON-NLS-1$
            .append("\n  LOOP ON (SELECT pm1.g1.e1 FROM pm1.g1) AS loopCursor") //$NON-NLS-1$
            .append("\n  BEGIN") //$NON-NLS-1$
            .append("\n    LOOP ON (SELECT pm1.g2.e1 FROM pm1.g2 WHERE loopCursor.e1 = pm1.g2.e1) AS loopCursor2") //$NON-NLS-1$
            .append("\n    BEGIN") //$NON-NLS-1$
            .append("\n      var1 = CONCAT(var1, CONCAT(' ', loopCursor2.e1));") //$NON-NLS-1$
            .append("\n    END") //$NON-NLS-1$
            .append("\n  END") //$NON-NLS-1$
            .append("\nEND"); //$NON-NLS-1$
            
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        Command command = helpResolveUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE);
        Map tempIDs = command.getTemporaryMetadata();
        assertNotNull(tempIDs);
        assertNull(tempIDs.get("LOOPCURSOR")); //$NON-NLS-1$
        assertNull(tempIDs.get("LOOPCURSOR2")); //$NON-NLS-1$
        
        Command subCommand = CommandCollectorVisitor.getCommands(command).get(0);
        tempIDs = subCommand.getTemporaryMetadata();
        assertNotNull(tempIDs);
        assertNull(tempIDs.get("LOOPCURSOR")); //$NON-NLS-1$
        assertNull(tempIDs.get("LOOPCURSOR2")); //$NON-NLS-1$

        subCommand = CommandCollectorVisitor.getCommands(command).get(1);
        tempIDs = subCommand.getTemporaryMetadata();
        assertNotNull(tempIDs);
        assertNotNull(tempIDs.get("LOOPCURSOR")); //$NON-NLS-1$
        assertNull(tempIDs.get("LOOPCURSOR2")); //$NON-NLS-1$
    }
    
	private CreateUpdateProcedureCommand helpResolveUpdateProcedure(String procedure, String userUpdateStr, String procedureType) throws QueryParserException, QueryResolverException, TeiidComponentException {
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleUpdateProc(procedureType, procedure);
		return resolveProcedure(userUpdateStr, metadata);
	}

	private CreateUpdateProcedureCommand resolveProcedure(String userUpdateStr,
			QueryMetadataInterface metadata) throws QueryParserException,
			QueryResolverException, TeiidComponentException,
			QueryMetadataException {
		ProcedureContainer userCommand = (ProcedureContainer)QueryParser.getQueryParser().parseCommand(userUpdateStr); 
        QueryResolver.resolveCommand(userCommand, metadata);
        metadata = new TempMetadataAdapter(metadata, new TempMetadataStore(userCommand.getTemporaryMetadata()));
        return (CreateUpdateProcedureCommand)QueryResolver.expandCommand(userCommand, metadata, null);
	}

	private void helpResolveException(String userUpdateStr, QueryMetadataInterface metadata, String msg) throws QueryParserException, TeiidComponentException {
		try {
			helpResolve(userUpdateStr, metadata);
		} catch (QueryResolverException e) {
			assertEquals(msg, e.getMessage());
		}
	}

	private CreateUpdateProcedureCommand helpResolve(String userUpdateStr, QueryMetadataInterface metadata) throws QueryParserException, QueryResolverException, TeiidComponentException {
		return resolveProcedure(userUpdateStr, metadata);
	}
    
    /**
     *  Constants will now auto resolve if they are consistently representable in the target type
     */
    @Test public void testDefect23257() throws Exception{
    	CreateUpdateProcedureCommand command = helpResolve("EXEC pm5.vsp59()", FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
        
        CommandStatement cs = (CommandStatement)command.getBlock().getStatements().get(1);
        
        Insert insert = (Insert)cs.getCommand();
        
        assertEquals(DataTypeManager.DefaultDataClasses.SHORT, ((Expression)insert.getValues().get(1)).getType());
    } 
    
    @Test public void testProcedureScoping() throws Exception {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        //note that this declare takes presedense over the proc INPUTS.e1 and CHANGING.e1 variables
        .append("\n  declare integer e1 = 1;") //$NON-NLS-1$
        .append("\n  e1 = e1;") //$NON-NLS-1$
        .append("\n  LOOP ON (SELECT pm1.g1.e1 FROM pm1.g1) AS loopCursor") //$NON-NLS-1$
        .append("\n  BEGIN") //$NON-NLS-1$
        //inside the scope of the loop, an unqualified e1 should resolve to the loop variable group
        .append("\n    variables.e1 = convert(e1, integer);") //$NON-NLS-1$
        .append("\n  END") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        CreateUpdateProcedureCommand command = helpResolveUpdateProcedure(proc.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
        
        Block block = command.getBlock();
        
        AssignmentStatement assStmt = (AssignmentStatement)block.getStatements().get(1);
        assertEquals(ProcedureReservedWords.VARIABLES, assStmt.getVariable().getGroupSymbol().getCanonicalName());
        assertEquals(ProcedureReservedWords.VARIABLES, ((ElementSymbol)assStmt.getValue()).getGroupSymbol().getCanonicalName());
        
        Block inner = ((LoopStatement)block.getStatements().get(2)).getBlock();
        
        assStmt = (AssignmentStatement)inner.getStatements().get(0);
        
        ElementSymbol value = ElementCollectorVisitor.getElements(assStmt.getValue(), false).iterator().next();
        
        assertEquals("LOOPCURSOR", value.getGroupSymbol().getCanonicalName()); //$NON-NLS-1$
    }
    
	// variable resolution, variable used in if statement, variable compared against
	// different datatype element
    @Test public void testCreateUpdateProcedure4() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1);\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// variable resolution, variable used in if statement, invalid operation on variable
    @Test public void testCreateUpdateProcedure5() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = var1 + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 whwre var1 = var1+var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }    
    
	// variable resolution, variables declared in different blocks local variables
	// should not override
    @Test public void testCreateUpdateProcedure6() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where var1 = pm1.g1.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE, "Variable var1 was previously declared."); //$NON-NLS-1$
    }
    
	// variable resolution, variables declared in different blocks local variables
	// inner block using outer block variables
    @Test public void testCreateUpdateProcedure7() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var2;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where var1 = pm1.g1.e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// variable resolution, variables declared in different blocks local variables
	// outer block cannot use inner block variables
    @Test public void testCreateUpdateProcedure8() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var2;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where var1 = pm1.g1.e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$
        procedure = procedure + "var2 = 1\n";                 //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }     
    
	// variable resolution, variables declared in different blocks local variables
	// should override, outer block variables still valid afetr inner block is declared
    @Test public void testCreateUpdateProcedure9() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where var1 = pm1.g1.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = var1 +1;\n";                 //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }    
    
	// special variable ROWS_UPDATED resolution
    @Test public void testCreateUpdateProcedure10() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUTS.e1, pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED = ROWS_UPDATED + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable ROWS_UPDATED used with declared variable
    @Test public void testCreateUpdateProcedure11() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUTS.e1, pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable INPUT used with declared variable
    @Test public void testCreateUpdateProcedure12() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, INPUTS.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUTS.e1, pm1.g1.e2 = INPUTS.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable CHANGING used with declared variable
    @Test public void testCreateUpdateProcedure14() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e1 = 'true')\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, INPUTS.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUTS.e1, pm1.g1.e2 = INPUTS.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable CHANGING and INPUT used in compound criteria
    @Test public void testCreateUpdateProcedure15() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e1='false' and INPUTS.e1=1)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, INPUTS.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUTS.e1, pm1.g1.e2 = INPUTS.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable CHANGING and INPUT used in compound criteria, with declared variables
    @Test public void testCreateUpdateProcedure16() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e4 ='true' and INPUTS.e2=1 or var1 < 30)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, INPUTS.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUTS.e1, pm1.g1.e2 = INPUTS.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// special variable CHANGING compared against integer no implicit conversion available
    @Test public void testCreateUpdateProcedure17() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "if(CHANGING.e4 = {d'2000-01-01'})\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, INPUTS.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE, "Error Code:ERR.015.008.0027 Message:The expressions in this criteria are being compared but are of differing types (boolean and date) and no implicit conversion is available:  CHANGING.e4 = {d'2000-01-01'}"); //$NON-NLS-1$
    }       
    
	// virtual group elements used in procedure(HAS CRITERIA)
    @Test public void testCreateUpdateProcedure18() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where HAS CRITERIA ON (vm1.g1.e1, vm1.g1.e1);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// virtual group elements used in procedure in if statement(HAS CRITERIA)
    @Test public void testCreateUpdateProcedure19() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(HAS CRITERIA ON (vm1.g1.e1, vm1.g1.e1))\n";                 //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where HAS CRITERIA ON (vm1.g1.e1, vm1.g1.e1);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }    
    
	// virtual group elements used in procedure(TRANSLATE CRITERIA)
    @Test public void testCreateUpdateProcedure20() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = 1, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// virtual group elements used in procedure(TRANSLATE CRITERIA)
    @Test public void testCreateUpdateProcedure21() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED = Select pm1.g1.e2 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = 1, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = INPUTS.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using undefined variable should fail
    @Test public void testCreateUpdateProcedure22() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
//        procedure = procedure + "DECLARE integer var1;\n";
        procedure = procedure + "var3 = var2+var1;\n";         //$NON-NLS-1$
        procedure = procedure + "var2 = Select pm1.g1.e2 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = 1, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = INPUTS.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using undefined variable declared is of invalid datatype
    @Test public void testCreateUpdateProcedure23() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE struct var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = 1, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 'x', pm1.g1.e2 = INPUTS.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using declare variable that has parts
    @Test public void testCreateUpdateProcedure24() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var2.var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using declare variable is qualified
    @Test public void testCreateUpdateProcedure26() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using declare variable is qualified but has more parts
    @Test public void testCreateUpdateProcedure27() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.var1.var2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using a variable that has not been declared in an assignment stmt
    @Test public void testCreateUpdateProcedure28() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using a variable that has not been declared in an assignment stmt
    @Test public void testCreateUpdateProcedure29() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = 1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using invalid function in assignment expr
    @Test public void testCreateUpdateProcedure30() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare integer var1;\n";         //$NON-NLS-1$
        procedure = procedure + "var1 = 'x' + ROWS_UPDATED;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }    
    
	// using invalid function in assignment expr
    @Test public void testCreateUpdateProcedure31() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare integer var1;\n";         //$NON-NLS-1$
        procedure = procedure + "var1 = 'x' + ROWS_UPDATED;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// using a variable being used inside a subcomand
    @Test public void testCreateUpdateProcedure32() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select var1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// variable resolution, variables declared in different blocks local variables
	// should override, outer block variables still valid afetr inner block is declared
	// fails as variable being compared against incorrect type
    @Test public void testCreateUpdateProcedure33() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE timestamp var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where var1 = pm1.g1.e2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = var1 +1;\n";                 //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// physical elements used on criteria of the if statement
    @Test public void testCreateUpdateProcedure34() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(pm1.g1.e2 =1 and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE, "Symbol pm1.g1.e2 is specified with an unknown group context"); //$NON-NLS-1$
    }
    
	// virtual elements used on criteria of the if statement
    @Test public void testCreateUpdateProcedure35() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(HAS CRITERIA ON (vm1.g1.e1) and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// physical elements used on criteria of the if statement
    @Test public void testCreateUpdateProcedure36() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(pm1.g1.e2 =1 and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }          
    
	// TranslateCriteria on criteria of the if statement
    @Test public void testCreateUpdateProcedure37() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(TRANSLATE CRITERIA ON (vm1.g1.e1) WITH (vm1.g1.e1 = 1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// validating Translate CRITERIA, elements on it should be virtual group elements
	// but can use variables
    @Test public void testCreateUpdateProcedure38() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e1 from pm1.g1 where Translate CRITERIA WITH (pm1.g1.e2 = var1);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// physical elements used on criteria of the if statement
    @Test public void testCreateUpdateProcedure39() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(pm1.g1.e2 =1 and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// TranslateCriteria on criteria of the if statement
    @Test public void testCreateUpdateProcedure40() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(TRANSLATE CRITERIA ON (e1) WITH (g1.e1 = 1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// TranslateCriteria on criteria of the if statement
    @Test public void testCreateUpdateProcedure41() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(HAS CRITERIA ON (e1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE CRITERIA ON (e1) WITH (g1.e1 = 1);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// TranslateCriteria on criteria of the if statement
    @Test public void testCreateUpdateProcedure42() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(HAS CRITERIA ON (e1))\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE CRITERIA ON (e1) WITH (g1.e1 = 1);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// TranslateCriteria on criteria of the if statement
    @Test public void testCreateUpdateProcedure43() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE CRITERIA ON (e1) WITH (g1.e1 = 1);\n";         //$NON-NLS-1$
//        procedure = procedure + "Select pm1.g1.e2, INPUTS.e2 from pm1.g1;\n";
//        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUTS.e1, pm1.g1.e2 = INPUTS.e2;\n";
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);

        Command procCommand = QueryParser.getQueryParser().parseCommand(procedure);
		GroupSymbol virtualGroup = new GroupSymbol("vm1.g1"); //$NON-NLS-1$
		virtualGroup.setMetadataID(metadata.getGroupID("vm1.g1")); //$NON-NLS-1$
        QueryResolver.resolveCommand(procCommand, virtualGroup, Command.TYPE_UPDATE, metadata);
    }
    
	// special variable CHANGING compared against integer no implicit conversion available
    @Test public void testCreateUpdateProcedure44() throws Exception {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "if(INPUTS.e1 = 10)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, INPUTS.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "INSERT into vm1.g1 (e1) values('x')"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.INSERT_PROCEDURE);
    }
    
	// special variable CHANGING compared against integer no implicit conversion available
    @Test public void testCreateUpdateProcedure45() throws Exception {
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "if(INPUTS.e1 = 10)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2, INPUTS.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        Command procCommand = QueryParser.getQueryParser().parseCommand(procedure);
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.INSERT_PROCEDURE, procedure);        
        
		GroupSymbol virtualGroup = new GroupSymbol("vm1.g1"); //$NON-NLS-1$
		virtualGroup.setMetadataID(metadata.getGroupID("vm1.g1")); //$NON-NLS-1$
		
        QueryResolver.resolveCommand(procCommand, virtualGroup, Command.TYPE_INSERT, metadata);
    }
    
	// special variable CHANGING compared against integer no implicit conversion available
    @Test public void testCreateUpdateProcedure46() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = INPUTS.e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        Command procCommand = QueryParser.getQueryParser().parseCommand(procedure);
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleUpdateProc(FakeMetadataObject.Props.UPDATE_PROCEDURE, procedure);        
        
		GroupSymbol virtualGroup = new GroupSymbol("vm1.g1"); //$NON-NLS-1$
		virtualGroup.setMetadataID(metadata.getGroupID("vm1.g1")); //$NON-NLS-1$

        QueryResolver.resolveCommand(procCommand, virtualGroup, Command.TYPE_UPDATE, metadata);
    }

	// TranslateCriteria on criteria of the if statement
	@Test public void testCreateUpdateProcedure47() throws Exception {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "if(HAS CRITERIA ON (e1))\n"; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e2 from pm1.g1 where TRANSLATE CRITERIA ON (e1) WITH (vm1.g1.e1 = pm1.g1.e1);\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// validating Translate CRITERIA, elements(left elements on  on it should be virtual group elements
	@Test public void testCreateUpdateProcedure48() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e1 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = 1, INPUTS.e2 = 2);\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// resolving Translate CRITERIA, right element should be present on the command
	@Test public void testCreateUpdateProcedure49() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e1 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e1 = pm1.g2.e1);\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// resolving criteria selector(on HAS CRITERIA), elements on it should be virtual group elements
	@Test public void testCreateUpdateProcedure50() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "if(HAS CRITERIA ON (vm1.g1.E1, vm1.g1.e1, INPUTS.e1))\n";                 //$NON-NLS-1$
		procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// resolving Translate CRITERIA, right side expression in the translate criteria should be elements on the command
	@Test public void testCreateUpdateProcedure51() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "var1=1;\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e1 from pm1.g1 where Translate CRITERIA WITH (vm1.g1.e2 = var1+vm1.g1.e2, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// validating Translate CRITERIA, elements on it should be virtual group elements
	// but can use variables, gut left exprs should always be virtual elements
	@Test public void testCreateUpdateProcedure52() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "Select pm1.g1.e1 from pm1.g1 where Translate CRITERIA WITH (var1 = vm1.g1.e2, vm1.g1.e1 = 2);\n"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// resolving AssignmentStatement, variable type and assigned type 
	// do not match and no implicit conversion available
	@Test public void testCreateUpdateProcedure53() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
		procedure = procedure + "var1 = INPUTS.e4;"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
	
	// resolving AssignmentStatement, variable type and assigned type 
	// do not match, but implicit conversion available
	@Test public void testCreateUpdateProcedure54() throws Exception {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE string var1;\n"; //$NON-NLS-1$
		procedure = procedure + "var1 = 1+1;"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}
    
	// resolving AssignmentStatement, variable type and assigned type 
	// do not match, but implicit conversion available
	@Test public void testCreateUpdateProcedure55() throws Exception {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
		procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
		procedure = procedure + "DECLARE string var1;\n"; //$NON-NLS-1$
		procedure = procedure + "var1 = 1+ROWS_UPDATED;"; //$NON-NLS-1$
		procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
		procedure = procedure + "END\n"; //$NON-NLS-1$

		String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
	}	

    @Test public void testDefect14912_CreateUpdateProcedure57_FunctionWithElementParamInAssignmentStatement() {
        // Tests that the function params are resolved before the function for assignment statements
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = badFunction(badElement);"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userCommand = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userCommand, FakeMetadataObject.Props.UPDATE_PROCEDURE, "Element \"badElement\" is not defined by any relevant group."); //$NON-NLS-1$
    }
    
	// addresses Cases 4624.  Before change to UpdateProcedureResolver,
    // this case failed with assertion exception.
    @Test public void testCase4624() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "VARIABLES.ROWS_UPDATED = 0;\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = {b'false'};\n"; //$NON-NLS-1$
        procedure = procedure + "IF(var1 = {b 'true'})\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT Rack_ID, RACK_MDT_TYPE INTO #racks FROM Bert_MAP.BERT3.RACK;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userCommand = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userCommand, FakeMetadataObject.Props.UPDATE_PROCEDURE, "Group does not exist: Bert_MAP.BERT3.RACK"); //$NON-NLS-1$
    }

	// addresses Cases 5474.  
    @Test public void testCase5474() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.NLEVELS;\n"; //$NON-NLS-1$
        procedure = procedure + "VARIABLES.NLEVELS = SELECT COUNT(*) FROM (SELECT oi.e1 AS Col1, oi.e2 AS Col2, oi.e3 FROM pm1.g2 AS oi) AS TOBJ, pm2.g2 AS TModel WHERE TModel.e3 = TOBJ.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        TestResolver.helpResolve(procedure, FakeMetadataFactory.example1Cached());
    }
    
    @Test public void testIssue174102() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  \n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string crit = 'WHERE pm1.sq2.in = \"test\"';\n"; //$NON-NLS-1$
        procedure = procedure + "CREATE LOCAL TEMPORARY TABLE #TTable (e1 string);"; //$NON-NLS-1$
        procedure = procedure + "EXECUTE STRING ('SELECT e1 FROM pm1.sq2 ' || crit ) AS e1 string INTO #TTable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        TestResolver.helpResolve(procedure, FakeMetadataFactory.example1Cached());
    }
    
    // Address Issue 174519.
    // Expected result is resolver failure, but with different error.
    @Test public void testIssue174519() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  \n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string VARIABLES.l_in = pm1.sq1.in;\n"; //$NON-NLS-1$
        procedure = procedure + "INSERT INTO #temp \n"; //$NON-NLS-1$
        procedure = procedure + "SELECT pm1.sq3.e1 FROM pm1.sq3 WHERE pm1.sq3.in = VARIABLES.l_in;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = exampleStoredProcedure(procedure);
        helpResolveException("EXEC pm1.sq1(1)", metadata, "Error Code:ERR.015.008.0010 Message:INSERT statement must have the same number of elements and values specified.  This statement has 0 elements and 0 values."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
	/*@Test public void testCommandUpdatingCountFromLastStatement() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  \n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "declare integer x = convert(pm1.sq1.in, integer) + 5;\n"; //$NON-NLS-1$
        procedure = procedure + "insert into pm1.g1 values (null, null, null, null);"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = exampleStoredProcedure(procedure);
        Command command = helpResolve(helpParse("exec pm1.sq1(1)"), metadata, null); //$NON-NLS-1$
        
        assertEquals(1, command.updatingModelCount(new TempMetadataAdapter(metadata, new TempMetadataStore())));
	}*/

    //baseline test to ensure that a declare assignment cannot contain the declared variable
    @Test public void testDeclareStatement() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.var1 = VARIABLES.var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    @Test public void testDynamicIntoInProc() throws Exception {
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("execute string 'SELECT e1, e2, e3, e4 FROM pm1.g2' as e1 string, e2 string, e3 string, e4 string INTO #myTempTable;\n") //$NON-NLS-1$
                                .append("select e1 from #myTempTable;\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    @Test public void testDynamicStatement() throws Exception {
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("execute string 'SELECT e1, e2, e3, e4 FROM pm1.g2';\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    @Test public void testDynamicStatementType() {
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("DECLARE object VARIABLES.X = null;\n") //$NON-NLS-1$
                                .append("execute string VARIABLES.X;\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpFailUpdateProcedure(procedure.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }

	// variable resolution
    @Test public void testCreateUpdateProcedure1() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 1, pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1=1"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
	// variable resolution, variable used in if statement
    @Test public void testCreateUpdateProcedure3() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpResolveUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }

    @Test public void testSelectIntoInProc() throws Exception {
        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                            .append("BEGIN\n") //$NON-NLS-1$
                                            .append("SELECT e1, e2, e3, e4 INTO pm1.g1 FROM pm1.g2;\n") //$NON-NLS-1$
                                            .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                            .append("END\n"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
        
        procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("SELECT e1, e2, e3, e4 INTO #myTempTable FROM pm1.g2;\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }

    @Test public void testSelectIntoInProcNoFrom() throws Exception {
        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                            .append("BEGIN\n") //$NON-NLS-1$
                                            .append("SELECT 'a', 19, {b'true'}, 13.999 INTO pm1.g1;\n") //$NON-NLS-1$
                                            .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                            .append("END\n"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
        
        procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("SELECT 'a', 19, {b'true'}, 13.999 INTO #myTempTable;\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    /*@Test public void testCommandUpdating3() throws Exception{
        StringBuffer procedure = new StringBuffer("CREATE PROCEDURE  ") //$NON-NLS-1$
        .append("BEGIN\n") //$NON-NLS-1$
        .append("INSERT INTO pm1.g1 (e1) VALUES (INPUTS.e1);\n") //$NON-NLS-1$
        .append("ROWS_UPDATED = INSERT INTO pm1.g2 (e1) VALUES (INPUTS.e1);\n") //$NON-NLS-1$
        .append("END\n"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        Command command = helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   FakeMetadataObject.Props.UPDATE_PROCEDURE);
        assertEquals(2, command.updatingModelCount(metadata));
    }*/

    /*@Test public void testCommandUpdatingCount6() throws Exception{
        String procedure = "CREATE PROCEDURE "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "if(INPUTS.e1 = 10)\n";         //$NON-NLS-1$
        procedure = procedure + "BEGIN\n";         //$NON-NLS-1$
        procedure = procedure + "INSERT INTO pm1.g1 (e2) VALUES (INPUTS.e2);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "INSERT into vm1.g1 (e1) values('x')"; //$NON-NLS-1$
        
        Command command = helpResolveUpdateProcedure(procedure, userUpdateStr,
                                     FakeMetadataObject.Props.INSERT_PROCEDURE);
        assertEquals(2, command.updatingModelCount(metadata));
    }*/
    
    // variable declared is of special type ROWS_RETURNED
    @Test public void testDeclareRowsUpdated() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer rows_updated;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Variable rows_updated was previously declared."); //$NON-NLS-1$
    }
    
    // validating INPUT element assigned
    @Test public void testAssignInput() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "INPUTS.e1 = Select pm1.g1.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    // validating CHANGING element assigned
    @Test public void testAssignChanging() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "CHANGING.e1 = Select pm1.g1.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    // variables cannot be used among insert elements
    @Test public void testVariableInInsert() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Insert into pm1.g1 (pm1.g1.e2, var1) values (1, 2);\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e3= 1"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userQuery, 
                FakeMetadataObject.Props.UPDATE_PROCEDURE, "Column variables do not reference columns on group \"pm1.g1\": [Unable to resolve 'var1': Element \"var1\" is not defined by any relevant group.]"); //$NON-NLS-1$
    }
    
    // variables cannot be used among insert elements
    @Test public void testVariableInInsert2() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Insert into pm1.g1 (pm1.g1.e2, INPUTS.x) values (1, 2);\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e3= 1"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userQuery, 
                FakeMetadataObject.Props.UPDATE_PROCEDURE, "Column variables do not reference columns on group \"pm1.g1\": [Unable to resolve 'INPUTS.x': Symbol INPUTS.x is specified with an unknown group context]"); //$NON-NLS-1$
    }
    
    //should resolve first to the table's column
    @Test public void testVariableInInsert3() throws Exception {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer e2;\n"; //$NON-NLS-1$
        procedure = procedure + "Insert into pm1.g1 (e2) values (1);\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e3= 1"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure, userQuery, 
                FakeMetadataObject.Props.UPDATE_PROCEDURE); 
    }
    
    @Test public void testAmbigousInput() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "select e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Element \"e1\" is ambiguous, it exists in two or more groups."); //$NON-NLS-1$
    }
    
    @Test public void testLoopRedefinition() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  declare string var1;") //$NON-NLS-1$
        .append("\n  LOOP ON (SELECT pm1.g1.e1 FROM pm1.g1) AS loopCursor") //$NON-NLS-1$
        .append("\n  BEGIN") //$NON-NLS-1$
        .append("\n    LOOP ON (SELECT pm1.g2.e1 FROM pm1.g2 WHERE loopCursor.e1 = pm1.g2.e1) AS loopCursor") //$NON-NLS-1$
        .append("\n    BEGIN") //$NON-NLS-1$
        .append("\n      var1 = CONCAT(var1, CONCAT(' ', loopCursor.e1));") //$NON-NLS-1$
        .append("\n    END") //$NON-NLS-1$
        .append("\n  END") //$NON-NLS-1$
        .append("\n  END"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(proc.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Nested Loop can not use the same cursor name as that of its parent."); //$NON-NLS-1$
    }
    
    @Test public void testTempGroupElementShouldNotBeResolable() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  select 1 as a into #temp;") //$NON-NLS-1$
        .append("\n  select #temp.a from pm1.g1;") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(proc.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Symbol #temp.a is specified with an unknown group context"); //$NON-NLS-1$
    }
    
    @Test public void testTempGroupElementShouldNotBeResolable1() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  select 1 as a into #temp;") //$NON-NLS-1$
        .append("\n  insert into #temp (a) values (#temp.a);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(proc.toString(), userUpdateStr,
                                     FakeMetadataObject.Props.UPDATE_PROCEDURE, "Symbol #temp.a is specified with an unknown group context"); //$NON-NLS-1$
    }
    
    @Test public void testProcedureCreate() throws Exception {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string);") //$NON-NLS-1$
        .append("\n  select e1 from t1;") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string, e2 integer);") //$NON-NLS-1$
        .append("\n  select e2 from t1;") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE); 
    }
    
    /**
     * it is not ok to redefine the loopCursor 
     */
    @Test public void testProcedureCreate1() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  LOOP ON (SELECT pm1.g1.e1 FROM pm1.g1) AS loopCursor") //$NON-NLS-1$
        .append("\n  BEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table loopCursor (e1 string);") //$NON-NLS-1$
        .append("\nEND") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE, "Cannot create temporary table \"loopCursor\". A table with the same name already exists."); //$NON-NLS-1$
    }
    
    @Test public void testProcedureCreateDrop() {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n drop table t1;") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpFailUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE, "Group does not exist: t1"); //$NON-NLS-1$
    }
    
    @Test public void testProcedureCreateDrop1() throws Exception {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string);") //$NON-NLS-1$
        .append("\n  drop table t1;") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE);
    }
    
    @Test public void testCreateAfterImplicitTempTable() throws Exception {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  select e1 into #temp from pm1.g1;") //$NON-NLS-1$
        .append("\n  create local temporary table #temp (e1 string);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE); 
    }
    
    @Test public void testInsertAfterCreate() throws Exception {
        StringBuffer proc = new StringBuffer("CREATE PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table #temp (e1 string, e2 string);") //$NON-NLS-1$
        .append("\n  insert into #temp (e1) values ('a');") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, FakeMetadataObject.Props.UPDATE_PROCEDURE); 
    }
    
	/**
	 * delete procedures should not reference input or changing vars.
	 */
	@Test public void testDefect16451() {
		String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure += "BEGIN\n"; //$NON-NLS-1$
        procedure += "Select pm1.g1.e2 from pm1.g1 where e1 = INPUTS.e1;\n"; //$NON-NLS-1$
        procedure += "ROWS_UPDATED = 0;"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$
        
        String userUpdateStr = "delete from vm1.g1 where e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
									 FakeMetadataObject.Props.DELETE_PROCEDURE, "Symbol INPUTS.e1 is specified with an unknown group context"); //$NON-NLS-1$
	}
	
    @Test public void testInvalidVirtualProcedure3() throws Exception {
    	helpResolveException("EXEC pm1.vsp18()", FakeMetadataFactory.example1Cached(), "Group does not exist: temptable"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    // variable resolution, variable compared against
    // different datatype element for which there is no implicit transformation)
    @Test public void testCreateUpdateProcedure2() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure += "BEGIN\n"; //$NON-NLS-1$
        procedure += "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure += "ROWS_UPDATED = UPDATE pm1.g1 SET pm1.g1.e4 = convert(var1, string), pm1.g1.e1 = var1;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1=1"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
				 FakeMetadataObject.Props.UPDATE_PROCEDURE, "Error Code:ERR.015.008.0041 Message:Cannot set symbol 'pm1.g1.e4' with expected type double to expression 'convert(var1, string)'"); //$NON-NLS-1$
    }
    
    // special variable INPUT compared against invalid type
    @Test public void testInvalidInputInUpdate() {
        String procedure = "CREATE PROCEDURE  "; //$NON-NLS-1$
        procedure += "BEGIN\n"; //$NON-NLS-1$
        procedure += "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure += "Select pm1.g1.e2, INPUTS.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure += "ROWS_UPDATED = UPDATE pm1.g1 SET pm1.g1.e1 = INPUTS.e1, pm1.g1.e2 = INPUTS.e1;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$
        
		helpFailUpdateProcedure(procedure, userUpdateStr,
				 FakeMetadataObject.Props.UPDATE_PROCEDURE, "Error Code:ERR.015.008.0041 Message:Cannot set symbol 'pm1.g1.e2' with expected type integer to expression 'INPUTS.e1'"); //$NON-NLS-1$
    }
    
    @Test public void testVirtualProcedure() throws Exception {
        helpResolve("EXEC pm1.vsp1()", FakeMetadataFactory.example1Cached());   //$NON-NLS-1$
    }
    
    @Test public void testVirtualProcedure2() throws Exception {
        helpResolve("EXEC pm1.vsp14()", FakeMetadataFactory.example1Cached());   //$NON-NLS-1$
    }
    
    @Test public void testVirtualProcedurePartialParameterReference() throws Exception {
        helpResolve("EXEC pm1.vsp58(5)", FakeMetadataFactory.example1Cached()); //$NON-NLS-1$
    }
    
    //cursor starts with "#" Defect14924
    @Test public void testVirtualProcedureInvalid1() throws Exception {
    	helpResolveException("EXEC pm1.vsp32()",FakeMetadataFactory.example1Cached(), "Cursor names cannot begin with \"#\" as that indicates the name of a temporary table: #mycursor.");   //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testVirtualProcedureWithOrderBy() throws Exception {
        helpResolve("EXEC pm1.vsp29()", FakeMetadataFactory.example1Cached());   //$NON-NLS-1$
    }
    
    @Test public void testVirtualProcedureWithTempTableAndOrderBy() throws Exception {
        helpResolve("EXEC pm1.vsp33()", FakeMetadataFactory.example1Cached());   //$NON-NLS-1$
    }
    
    @Test public void testVirtualProcedureWithConstAndOrderBy() throws Exception {
        helpResolve("EXEC pm1.vsp34()", FakeMetadataFactory.example1Cached());   //$NON-NLS-1$
    }
    
    @Test public void testVirtualProcedureWithNoFromAndOrderBy() throws Exception {
        helpResolve("EXEC pm1.vsp28()", FakeMetadataFactory.example1Cached());   //$NON-NLS-1$
    }
    
    @Test public void testInvalidVirtualProcedure2() throws Exception {
        helpResolveException("EXEC pm1.vsp12()", FakeMetadataFactory.example1Cached(), "Symbol mycursor.e2 is specified with an unknown group context"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testLoopRedefinition2() throws Exception {
        helpResolveException("EXEC pm1.vsp11()", FakeMetadataFactory.example1Cached(), "Nested Loop can not use the same cursor name as that of its parent."); //$NON-NLS-1$ //$NON-NLS-2$
    }
        
    @Test public void testVariableResolutionWithIntervening() throws Exception {
        StringBuffer proc = new StringBuffer("CREATE VIRTUAL PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  declare string x;") //$NON-NLS-1$
        .append("\n  x = '1';") //$NON-NLS-1$
        .append("\n  declare string y;") //$NON-NLS-1$
        .append("\n  y = '1';") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$
        
        TestResolver.helpResolve(proc.toString(), FakeMetadataFactory.example1Cached()); 
    }
    
    @Test public void testVDBQualified() throws Exception {
        helpResolve("EXEC myvdb.pm1.vsp29()", FakeMetadataFactory.example1Cached());   //$NON-NLS-1$
    }
    
}
