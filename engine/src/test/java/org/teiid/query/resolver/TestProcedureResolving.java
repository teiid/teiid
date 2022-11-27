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

package org.teiid.query.resolver;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.datamgr.LanguageBridgeFactory;
import org.teiid.language.Call;
import org.teiid.metadata.Table;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.CommandCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.TestValidator;

@SuppressWarnings("nls")
public class TestProcedureResolving {

    private void helpFailUpdateProcedure(String procedure, String userUpdateStr, Table.TriggerEvent procedureType) {
        helpFailUpdateProcedure(procedure, userUpdateStr, procedureType, null);
    }

    private void helpFailUpdateProcedure(String procedure, String userUpdateStr, Table.TriggerEvent procedureType, String msg) {
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
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
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

        Command command = helpResolveUpdateProcedure(proc.toString(), userUpdateStr, Table.TriggerEvent.UPDATE);
        Map<String, TempMetadataID> tempIDs = command.getTemporaryMetadata().getData();
        assertNotNull(tempIDs);
        assertNull(tempIDs.get("LOOPCURSOR")); //$NON-NLS-1$
        assertNull(tempIDs.get("LOOPCURSOR2")); //$NON-NLS-1$

        Command subCommand = CommandCollectorVisitor.getCommands(command).get(0);
        tempIDs = subCommand.getTemporaryMetadata().getData();
        assertNotNull(tempIDs);
        assertNull(tempIDs.get("LOOPCURSOR")); //$NON-NLS-1$
        assertNull(tempIDs.get("LOOPCURSOR2")); //$NON-NLS-1$

        subCommand = CommandCollectorVisitor.getCommands(command).get(1);
        tempIDs = subCommand.getTemporaryMetadata().getData();
        assertNotNull(tempIDs);
        assertNotNull(tempIDs.get("LOOPCURSOR")); //$NON-NLS-1$
        assertNull(tempIDs.get("LOOPCURSOR2")); //$NON-NLS-1$
    }

    private TriggerAction helpResolveUpdateProcedure(String procedure, String userUpdateStr, Table.TriggerEvent procedureType) throws QueryParserException, QueryResolverException, TeiidComponentException {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleUpdateProc(procedureType, procedure);
        return (TriggerAction) resolveProcedure(userUpdateStr, metadata);
    }

    private Command resolveProcedure(String userUpdateStr,
            QueryMetadataInterface metadata) throws QueryParserException,
            QueryResolverException, TeiidComponentException,
            QueryMetadataException {
        ProcedureContainer userCommand = (ProcedureContainer)QueryParser.getQueryParser().parseCommand(userUpdateStr);
        QueryResolver.resolveCommand(userCommand, metadata);
        metadata = new TempMetadataAdapter(metadata, userCommand.getTemporaryMetadata());
        return QueryResolver.expandCommand(userCommand, metadata, null);
    }

    private void helpResolveException(String userUpdateStr, QueryMetadataInterface metadata, String msg) throws QueryParserException, TeiidComponentException {
        try {
            helpResolve(userUpdateStr, metadata);
            fail();
        } catch (QueryResolverException e) {
            assertEquals(msg, e.getMessage());
        }
    }

    private Command helpResolve(String userUpdateStr, QueryMetadataInterface metadata) throws QueryParserException, QueryResolverException, TeiidComponentException {
        return resolveProcedure(userUpdateStr, metadata);
    }

    /**
     *  Constants will now auto resolve if they are consistently representable in the target type
     */
    @Test public void testDefect23257() throws Exception{
        CreateProcedureCommand command = (CreateProcedureCommand) helpResolve("EXEC pm6.vsp59()", RealMetadataFactory.example1Cached()); //$NON-NLS-1$

        CommandStatement cs = (CommandStatement)command.getBlock().getStatements().get(1);

        Insert insert = (Insert)cs.getCommand();

        assertEquals(DataTypeManager.DefaultDataClasses.SHORT, ((Expression)insert.getValues().get(1)).getType());
    }

    @Test public void testProcedureScoping() throws Exception {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
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

        TriggerAction command = helpResolveUpdateProcedure(proc.toString(), userUpdateStr,
                                     Table.TriggerEvent.UPDATE);

        Block block = command.getBlock();

        AssignmentStatement assStmt = (AssignmentStatement)block.getStatements().get(1);
        assertEquals(ProcedureReservedWords.VARIABLES, assStmt.getVariable().getGroupSymbol().getName());
        assertEquals(ProcedureReservedWords.VARIABLES, ((ElementSymbol)assStmt.getExpression()).getGroupSymbol().getName());

        Block inner = ((LoopStatement)block.getStatements().get(2)).getBlock();

        assStmt = (AssignmentStatement)inner.getStatements().get(0);

        ElementSymbol value = ElementCollectorVisitor.getElements(assStmt.getExpression(), false).iterator().next();

        assertEquals("loopCursor", value.getGroupSymbol().getName()); //$NON-NLS-1$
    }

    @Test(expected=QueryResolverException.class) public void testBlockResolving() throws Exception {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        //note that this declare takes presedense over the proc INPUTS.e1 and CHANGING.e1 variables
        .append("\n  declare integer e1 = 1;") //$NON-NLS-1$
        .append("\n  BEGIN") //$NON-NLS-1$
        //inside the scope of the loop, an unqualified e1 should resolve to the loop variable group
        .append("\n    variables.e1 = e2;") //$NON-NLS-1$
        .append("\n  END") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(proc.toString(), userUpdateStr,
                                     Table.TriggerEvent.UPDATE);

    }

    // variable resolution, variable used in if statement, variable compared against
    // different datatype element
    @Test public void testCreateUpdateProcedure4() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1);\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // variable resolution, variable used in if statement, invalid operation on variable
    @Test public void testCreateUpdateProcedure5() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = var1 + var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1 whwre var1 = var1+var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // variable resolution, variables declared in different blocks local variables
    // should not override
    @Test public void testCreateUpdateProcedure6() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
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
                                     Table.TriggerEvent.UPDATE, "Variable var1 was previously declared."); //$NON-NLS-1$
    }

    // variable resolution, variables declared in different blocks local variables
    // inner block using outer block variables
    @Test public void testCreateUpdateProcedure7() throws Exception {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
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
                                     Table.TriggerEvent.UPDATE);
    }

    // variable resolution, variables declared in different blocks local variables
    // outer block cannot use inner block variables
    @Test public void testCreateUpdateProcedure8() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
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
                                     Table.TriggerEvent.UPDATE);
    }

    // variable resolution, variables declared in different blocks local variables
    // should override, outer block variables still valid afetr inner block is declared
    @Test public void testCreateUpdateProcedure9() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
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
                                     Table.TriggerEvent.UPDATE);
    }

    // using declare variable that has parts
    @Test public void testCreateUpdateProcedure24() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var2.var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // using declare variable is qualified
    @Test public void testCreateUpdateProcedure26() throws Exception {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // using declare variable is qualified but has more parts
    @Test public void testCreateUpdateProcedure27() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.var1.var2;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // using a variable that has not been declared in an assignment stmt
    @Test public void testCreateUpdateProcedure28() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // using a variable that has not been declared in an assignment stmt
    @Test public void testCreateUpdateProcedure29() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = 1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // using invalid function in assignment expr
    @Test public void testCreateUpdateProcedure30() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare integer var1;\n";         //$NON-NLS-1$
        procedure = procedure + "var1 = 'x' + ROWS_UPDATED;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // using invalid function in assignment expr
    @Test public void testCreateUpdateProcedure31() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare integer var1;\n";         //$NON-NLS-1$
        procedure = procedure + "var1 = 'x' + ROWS_UPDATED;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // using a variable being used inside a subcomand
    @Test public void testCreateUpdateProcedure32() throws Exception {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Declare integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Select var1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // variable resolution, variables declared in different blocks local variables
    // should override, outer block variables still valid afetr inner block is declared
    // fails as variable being compared against incorrect type
    @Test public void testCreateUpdateProcedure33() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
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
                                     Table.TriggerEvent.UPDATE);
    }

    // physical elements used on criteria of the if statement
    @Test public void testCreateUpdateProcedure34() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(pm1.g1.e2 =1 and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE, "TEIID31119 Symbol pm1.g1.e2 is specified with an unknown group context"); //$NON-NLS-1$
    }

    // physical elements used on criteria of the if statement
    @Test public void testCreateUpdateProcedure36() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(pm1.g1.e2 =1 and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // physical elements used on criteria of the if statement
    @Test public void testCreateUpdateProcedure39() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(pm1.g1.e2 =1 and var1=1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // resolving AssignmentStatement, variable type and assigned type
    // do not match and no implicit conversion available
    @Test public void testCreateUpdateProcedure53() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = INPUTS.e4;"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // resolving AssignmentStatement, variable type and assigned type
    // do not match, but implicit conversion available
    @Test public void testCreateUpdateProcedure54() throws Exception {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = 1+1;"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    @Test public void testDefect14912_CreateUpdateProcedure57_FunctionWithElementParamInAssignmentStatement() {
        // Tests that the function params are resolved before the function for assignment statements
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = badFunction(badElement);"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userCommand = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userCommand, Table.TriggerEvent.UPDATE, "TEIID31118 Element \"badElement\" is not defined by any relevant group."); //$NON-NLS-1$
    }

    // addresses Cases 4624.  Before change to UpdateProcedureResolver,
    // this case failed with assertion exception.
    @Test public void testCase4624() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = {b'false'};\n"; //$NON-NLS-1$
        procedure = procedure + "IF(var1 = {b 'true'})\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT Rack_ID, RACK_MDT_TYPE INTO #racks FROM Bert_MAP.BERT3.RACK;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userCommand = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userCommand, Table.TriggerEvent.UPDATE, "Group does not exist: Bert_MAP.BERT3.RACK"); //$NON-NLS-1$
    }

    // addresses Cases 5474.
    @Test public void testCase5474() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.NLEVELS;\n"; //$NON-NLS-1$
        procedure = procedure + "VARIABLES.NLEVELS = SELECT COUNT(*) FROM (SELECT oi.e1 AS Col1, oi.e2 AS Col2, oi.e3 FROM pm1.g2 AS oi) AS TOBJ, pm2.g2 AS TModel WHERE TModel.e3 = TOBJ.e3;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        TestResolver.helpResolve(procedure, RealMetadataFactory.example1Cached());
    }

    // addresses Cases 5474.
    @Test public void testProcWithReturn() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "call sptest9(1);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        TestResolver.helpResolve(procedure, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testIssue174102() throws Exception {
        String procedure = "CREATE VIRTUAL PROCEDURE  \n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string crit = 'WHERE pm1.sq2.in = \"test\"';\n"; //$NON-NLS-1$
        procedure = procedure + "CREATE LOCAL TEMPORARY TABLE #TTable (e1 string);"; //$NON-NLS-1$
        procedure = procedure + "EXECUTE STRING ('SELECT e1 FROM pm1.sq2 ' || crit ) AS e1 string INTO #TTable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        TestResolver.helpResolve(procedure, RealMetadataFactory.example1Cached());
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
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer VARIABLES.var1 = VARIABLES.var1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr, Table.TriggerEvent.UPDATE);
    }

    @Test public void testDynamicIntoInProc() throws Exception {
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        StringBuffer procedure = new StringBuffer("FOR EACH ROW ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("execute string 'SELECT e1, e2, e3, e4 FROM pm1.g2' as e1 string, e2 string, e3 string, e4 string INTO #myTempTable;\n") //$NON-NLS-1$
                                .append("select e1 from #myTempTable;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   Table.TriggerEvent.UPDATE);
    }

    @Test public void testDynamicStatement() throws Exception {
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        StringBuffer procedure = new StringBuffer("FOR EACH ROW ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("execute string 'SELECT e1, e2, e3, e4 FROM pm1.g2';\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   Table.TriggerEvent.UPDATE);
    }

    @Test public void testDynamicStatementType() {
        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        StringBuffer procedure = new StringBuffer("FOR EACH ROW ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("DECLARE object VARIABLES.X = null;\n") //$NON-NLS-1$
                                .append("execute string VARIABLES.X;\n") //$NON-NLS-1$
                                .append("ROWS_UPDATED =0;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpFailUpdateProcedure(procedure.toString(), userUpdateStr, Table.TriggerEvent.UPDATE);
    }

    // variable resolution
    @Test public void testCreateUpdateProcedure1() throws Exception {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "UPDATE pm1.g1 SET pm1.g1.e1 = 1, pm1.g1.e2 = var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1=1"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // variable resolution, variable used in if statement
    @Test public void testCreateUpdateProcedure3() throws Exception {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "if(var1 =1)\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    @Test public void testSelectIntoInProc() throws Exception {
        StringBuffer procedure = new StringBuffer("FOR EACH ROW ") //$NON-NLS-1$
                                            .append("BEGIN\n") //$NON-NLS-1$
                                            .append("SELECT e1, e2, e3, e4 INTO pm1.g1 FROM pm1.g2;\n") //$NON-NLS-1$
                                            .append("END\n"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                     Table.TriggerEvent.UPDATE);

        procedure = new StringBuffer("FOR EACH ROW ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("SELECT e1, e2, e3, e4 INTO #myTempTable FROM pm1.g2;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   Table.TriggerEvent.UPDATE);
    }

    @Test public void testSelectIntoInProcNoFrom() throws Exception {
        StringBuffer procedure = new StringBuffer("FOR EACH ROW ") //$NON-NLS-1$
                                            .append("BEGIN\n") //$NON-NLS-1$
                                            .append("SELECT 'a', 19, {b'true'}, 13.999 INTO pm1.g1;\n") //$NON-NLS-1$
                                            .append("END\n"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                     Table.TriggerEvent.UPDATE);

        procedure = new StringBuffer("FOR EACH ROW ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("SELECT 'a', 19, {b'true'}, 13.999 INTO #myTempTable;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$
        helpResolveUpdateProcedure(procedure.toString(), userUpdateStr,
                                   Table.TriggerEvent.UPDATE);
    }

    // validating INPUT element assigned
    @Test public void testAssignInput() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "INPUTS.e1 = Select pm1.g1.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // validating CHANGING element assigned
    @Test public void testAssignChanging() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "CHANGING.e1 = Select pm1.g1.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // variables cannot be used among insert elements
    @Test public void testVariableInInsert() {
        String procedure = "FOR EACH ROW  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Insert into pm1.g1 (pm1.g1.e2, var1) values (1, 2);\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e3= 1"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE, "TEIID30126 Column variables do not reference columns on group \"pm1.g1\": [Unable to resolve 'var1': TEIID31118 Element \"var1\" is not defined by any relevant group.]"); //$NON-NLS-1$
    }

    // variables cannot be used among insert elements
    @Test public void testVariableInInsert2() {
        String procedure = "FOR EACH ROW  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "Insert into pm1.g1 (pm1.g1.e2, INPUTS.x) values (1, 2);\n"; //$NON-NLS-1$
        procedure = procedure + "ROWS_UPDATED =0;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e3= 1"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE, "TEIID30126 Column variables do not reference columns on group \"pm1.g1\": [Unable to resolve 'INPUTS.x': TEIID31119 Symbol INPUTS.x is specified with an unknown group context]"); //$NON-NLS-1$
    }

    //should resolve first to the table's column
    @Test public void testVariableInInsert3() throws Exception {
        String procedure = "FOR EACH ROW  "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer e2;\n"; //$NON-NLS-1$
        procedure = procedure + "Insert into pm1.g1 (e2) values (1);\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where e3= 1"; //$NON-NLS-1$

        helpResolveUpdateProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    @Test public void testAmbigousInput() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN ATOMIC\n"; //$NON-NLS-1$
        procedure = procedure + "select e1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE, "TEIID31117 Element \"e1\" is ambiguous and should be qualified, at a single scope it exists in [CHANGING, \"NEW\", \"OLD\"]"); //$NON-NLS-1$
    }

    @Test public void testLoopRedefinition() {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
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
                                     Table.TriggerEvent.UPDATE, "TEIID30124 Loop cursor or exception group name loopCursor already exists."); //$NON-NLS-1$
    }

    @Test public void testTempGroupElementShouldNotBeResolable() {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  select 1 as a into #temp;") //$NON-NLS-1$
        .append("\n  select #temp.a from pm1.g1;") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(proc.toString(), userUpdateStr,
                                     Table.TriggerEvent.UPDATE, "TEIID31119 Symbol #temp.a is specified with an unknown group context"); //$NON-NLS-1$
    }

    @Test public void testTempGroupElementShouldNotBeResolable1() {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  select 1 as a into #temp;") //$NON-NLS-1$
        .append("\n  insert into #temp (a) values (#temp.a);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(proc.toString(), userUpdateStr,
                                     Table.TriggerEvent.UPDATE, "TEIID31119 Symbol #temp.a is specified with an unknown group context"); //$NON-NLS-1$
    }

    @Test public void testProcedureCreate() throws Exception {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string);") //$NON-NLS-1$
        .append("\n  select e1 from t1;") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string, e2 integer);") //$NON-NLS-1$
        .append("\n  select e2 from t1;") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, Table.TriggerEvent.UPDATE);
    }

    /**
     * it is not ok to redefine the loopCursor
     */
    @Test public void testProcedureCreate1() {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  LOOP ON (SELECT pm1.g1.e1 FROM pm1.g1) AS loopCursor") //$NON-NLS-1$
        .append("\n  BEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table loopCursor (e1 string);") //$NON-NLS-1$
        .append("\nEND") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(proc.toString(), userUpdateStr, Table.TriggerEvent.UPDATE, "TEIID30118 Cannot create temporary table \"loopCursor\". An object with the same name already exists."); //$NON-NLS-1$
    }

    @Test public void testProcedureCreateDrop() {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n drop table t1;") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(proc.toString(), userUpdateStr, Table.TriggerEvent.UPDATE, "Group does not exist: t1"); //$NON-NLS-1$
    }

    @Test public void testProcedureCreateDrop1() throws Exception {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table t1 (e1 string);") //$NON-NLS-1$
        .append("\n  drop table t1;") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, Table.TriggerEvent.UPDATE);
    }

    @Test public void testCreateAfterImplicitTempTable() throws Exception {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  select e1 into #temp from pm1.g1;") //$NON-NLS-1$
        .append("\n  create local temporary table #temp (e1 string);") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, Table.TriggerEvent.UPDATE);
    }

    @Test public void testInsertAfterCreate() throws Exception {
        StringBuffer proc = new StringBuffer("FOR EACH ROW") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  create local temporary table #temp (e1 string, e2 string);") //$NON-NLS-1$
        .append("\n  insert into #temp (e1) values ('a');") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpResolveUpdateProcedure(proc.toString(), userUpdateStr, Table.TriggerEvent.UPDATE);
    }

    /**
     * delete procedures should not reference input or changing vars.
     */
    @Test public void testDefect16451() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure += "BEGIN ATOMIC\n"; //$NON-NLS-1$
        procedure += "Select pm1.g1.e2 from pm1.g1 where e1 = NEW.e1;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        String userUpdateStr = "delete from vm1.g1 where e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.DELETE, "TEIID31119 Symbol \"NEW\".e1 is specified with an unknown group context"); //$NON-NLS-1$
    }

    @Test public void testInvalidVirtualProcedure3() throws Exception {
        helpResolveException("EXEC pm1.vsp18()", RealMetadataFactory.example1Cached(), "Group does not exist: temptable"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // variable resolution, variable compared against
    // different datatype element for which there is no implicit transformation)
    @Test public void testCreateUpdateProcedure2() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure += "BEGIN\n"; //$NON-NLS-1$
        procedure += "DECLARE boolean var1;\n"; //$NON-NLS-1$
        procedure += "ROWS_UPDATED = UPDATE pm1.g1 SET pm1.g1.e4 = convert(var1, string), pm1.g1.e1 = var1;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1=1"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                 Table.TriggerEvent.UPDATE, "TEIID30082 Cannot set symbol 'pm1.g1.e4' with expected type double to expression 'convert(var1, string)'"); //$NON-NLS-1$
    }

    // special variable INPUT compared against invalid type
    @Test public void testInvalidInputInUpdate() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure += "BEGIN ATOMIC\n"; //$NON-NLS-1$
        procedure += "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure += "Select pm1.g1.e2, new.e2 from pm1.g1;\n"; //$NON-NLS-1$
        procedure += "UPDATE pm1.g1 SET pm1.g1.e1 = new.e1, pm1.g1.e2 = new.e1;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailUpdateProcedure(procedure, userUpdateStr,
                 Table.TriggerEvent.UPDATE, "TEIID30082 Cannot set symbol 'pm1.g1.e2' with expected type integer to expression '\"new\".e1'"); //$NON-NLS-1$
    }

    @Test public void testVirtualProcedure() throws Exception {
        helpResolve("EXEC pm1.vsp1()", RealMetadataFactory.example1Cached());   //$NON-NLS-1$
    }

    @Test public void testVirtualProcedure2() throws Exception {
        helpResolve("EXEC pm1.vsp14()", RealMetadataFactory.example1Cached());   //$NON-NLS-1$
    }

    @Test public void testVirtualProcedurePartialParameterReference() throws Exception {
        helpResolve("EXEC pm1.vsp58(5)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    //cursor starts with "#" Defect14924
    @Test public void testVirtualProcedureInvalid1() throws Exception {
        helpResolveException("EXEC pm1.vsp32()",RealMetadataFactory.example1Cached(), "TEIID30125 Cursor or exception group names cannot begin with \"#\" as that indicates the name of a temporary table: #mycursor.");   //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testVirtualProcedureWithOrderBy() throws Exception {
        helpResolve("EXEC pm1.vsp29()", RealMetadataFactory.example1Cached());   //$NON-NLS-1$
    }

    @Test public void testVirtualProcedureWithTempTableAndOrderBy() throws Exception {
        helpResolve("EXEC pm1.vsp33()", RealMetadataFactory.example1Cached());   //$NON-NLS-1$
    }

    @Test public void testVirtualProcedureWithConstAndOrderBy() throws Exception {
        helpResolve("EXEC pm1.vsp34()", RealMetadataFactory.example1Cached());   //$NON-NLS-1$
    }

    @Test public void testVirtualProcedureWithNoFromAndOrderBy() throws Exception {
        helpResolve("EXEC pm1.vsp28()", RealMetadataFactory.example1Cached());   //$NON-NLS-1$
    }

    @Test public void testInvalidVirtualProcedure2() throws Exception {
        helpResolveException("EXEC pm1.vsp12()", RealMetadataFactory.example1Cached(), "TEIID31119 Symbol mycursor.e2 is specified with an unknown group context"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLoopRedefinition2() throws Exception {
        helpResolveException("EXEC pm1.vsp11()", RealMetadataFactory.example1Cached(), "TEIID30124 Loop cursor or exception group name mycursor already exists."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testVariableResolutionWithIntervening() throws Exception {
        StringBuffer proc = new StringBuffer("CREATE VIRTUAL PROCEDURE") //$NON-NLS-1$
        .append("\nBEGIN") //$NON-NLS-1$
        .append("\n  declare string x;") //$NON-NLS-1$
        .append("\n  x = '1';") //$NON-NLS-1$
        .append("\n  declare string y;") //$NON-NLS-1$
        .append("\n  y = '1';") //$NON-NLS-1$
        .append("\nEND"); //$NON-NLS-1$

        TestResolver.helpResolve(proc.toString(), RealMetadataFactory.example1Cached());
    }

    @Test public void testVDBQualified() throws Exception {
        helpResolve("EXEC example1.pm1.vsp29()", RealMetadataFactory.example1Cached());   //$NON-NLS-1$
    }

    @Test public void testOptionalParams() throws Exception {
        String ddl = "create foreign procedure proc (x integer, y string);\n";
        TransformationMetadata tm = createMetadata(ddl);

        String sql = "call proc (1)"; //$NON-NLS-1$

        StoredProcedure sp = (StoredProcedure) TestResolver.helpResolve(sql, tm);

        assertEquals(new Constant(null, DataTypeManager.DefaultDataClasses.STRING), sp.getParameter(2).getExpression());

        sql = "call proc (1, 'a')"; //$NON-NLS-1$

        sp = (StoredProcedure) TestResolver.helpResolve(sql, tm);

        assertEquals(new Constant("a", DataTypeManager.DefaultDataClasses.STRING), sp.getParameter(2).getExpression());
    }

    public static TransformationMetadata createMetadata(String ddl) throws Exception {
        return RealMetadataFactory.fromDDL(ddl, "test", "test");
    }

    @Test public void testOptionalParams1() throws Exception {
        String ddl = "create foreign procedure proc (x integer, y string NOT NULL, z integer);\n";
        TransformationMetadata tm = createMetadata(ddl);

        String sql = "call proc (1, 'a')"; //$NON-NLS-1$

        StoredProcedure sp = (StoredProcedure) TestResolver.helpResolve(sql, tm);

        assertEquals(new Constant("a", DataTypeManager.DefaultDataClasses.STRING), sp.getParameter(2).getExpression());
    }

    @Test public void testVarArgs() throws Exception {
        String ddl = "create foreign procedure proc (x integer, VARIADIC z integer) returns (x string);\n";
        TransformationMetadata tm = createMetadata(ddl);
        String sql = "call proc (1, 2, 3)"; //$NON-NLS-1$

        StoredProcedure sp = (StoredProcedure) TestResolver.helpResolve(sql, tm);
        assertEquals("EXEC proc(1, 2, 3)", sp.toString());
        assertEquals(new Constant(1), sp.getParameter(1).getExpression());
        assertEquals(new Array(DataTypeManager.DefaultDataClasses.INTEGER, Arrays.asList((Expression)new Constant(2), new Constant(3))), sp.getParameter(2).getExpression());
        assertEquals(SPParameter.RESULT_SET, sp.getParameter(3).getParameterType());

        sql = "call proc (1)"; //$NON-NLS-1$
        sp = (StoredProcedure) TestResolver.helpResolve(sql, tm);
        assertEquals("EXEC proc(1)", sp.toString());
        assertEquals(new Array(DataTypeManager.DefaultDataClasses.INTEGER, new ArrayList<Expression>(0)), sp.getParameter(2).getExpression());

        sp = (StoredProcedure) QueryRewriter.evaluateAndRewrite(sp, new Evaluator(null, null, null), null, tm);
        LanguageBridgeFactory lbf = new LanguageBridgeFactory(tm);
        Call call = (Call)lbf.translate(sp);
        assertEquals("EXEC proc(1)", call.toString());

        sql = "call proc (1, (2, 3))"; //$NON-NLS-1$
        sp = (StoredProcedure) TestResolver.helpResolve(sql, tm);
        assertEquals("EXEC proc(1, (2, 3))", sp.toString());
        assertEquals(new Constant(1), sp.getParameter(1).getExpression());
        assertEquals(new Array(DataTypeManager.DefaultDataClasses.INTEGER, Arrays.asList((Expression)new Constant(2), new Constant(3))), sp.getParameter(2).getExpression());
        assertEquals(SPParameter.RESULT_SET, sp.getParameter(3).getParameterType());
    }

    @Test public void testVarArgs1() throws Exception {
        String ddl = "create foreign procedure proc (VARIADIC z integer) returns (x string);\n";
        TransformationMetadata tm = createMetadata(ddl);

        String sql = "call proc ()"; //$NON-NLS-1$
        StoredProcedure sp = (StoredProcedure) TestResolver.helpResolve(sql, tm);
        assertEquals("EXEC proc()", sp.toString());
        assertEquals(new Array(DataTypeManager.DefaultDataClasses.INTEGER, new ArrayList<Expression>(0)), sp.getParameter(1).getExpression());

        sp = (StoredProcedure) QueryRewriter.evaluateAndRewrite(sp, new Evaluator(null, null, null), null, tm);
        LanguageBridgeFactory lbf = new LanguageBridgeFactory(tm);
        Call call = (Call)lbf.translate(sp);
        assertEquals("EXEC proc()", call.toString());
        //we pass to the translator level flattened, so no argument
        assertEquals(0, call.getArguments().size());
    }

    @Test public void testVarArgs2() throws Exception {
        String ddl = "create foreign procedure proc (VARIADIC z object) returns (x string);\n";
        TransformationMetadata tm = createMetadata(ddl);

        String sql = "call proc ()"; //$NON-NLS-1$
        StoredProcedure sp = (StoredProcedure) TestResolver.helpResolve(sql, tm);
        assertEquals("EXEC proc()", sp.toString());
        assertEquals(new Array(DataTypeManager.DefaultDataClasses.OBJECT, new ArrayList<Expression>(0)), sp.getParameter(1).getExpression());

        sql = "call proc (1, (2, 3))"; //$NON-NLS-1$
        sp = (StoredProcedure) TestResolver.helpResolve(sql, tm);
        assertEquals("EXEC proc(1, (2, 3))", sp.toString());
        ArrayList<Expression> expressions = new ArrayList<Expression>();
        expressions.add(new Constant(1));
        expressions.add(new Array(DataTypeManager.DefaultDataClasses.INTEGER, Arrays.asList((Expression)new Constant(2), new Constant(3))));
        assertEquals(new Array(DataTypeManager.DefaultDataClasses.OBJECT, expressions), sp.getParameter(1).getExpression());
    }

    @Test public void testAnonBlock() throws Exception {
        String sql = "begin select 1 as something; end"; //$NON-NLS-1$
        CreateProcedureCommand sp = (CreateProcedureCommand) TestResolver.helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(1, sp.getResultSetColumns().size());
        assertEquals("something", Symbol.getName(sp.getResultSetColumns().get(0)));
        assertEquals(1, sp.getProjectedSymbols().size());
        assertTrue(sp.returnsResultSet());
    }

    @Test public void testAnonBlockNoResult() throws Exception {
        String sql = "begin select 1 as something without return; end"; //$NON-NLS-1$
        CreateProcedureCommand sp = (CreateProcedureCommand) TestResolver.helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(0, sp.getProjectedSymbols().size());
        assertFalse(sp.returnsResultSet());
    }

    @Test public void testReturnAndResultSet() throws Exception {
        String ddl = "CREATE FOREIGN PROCEDURE proc (OUT param STRING RESULT) RETURNS TABLE (a INTEGER, b STRING);"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.fromDDL(ddl, "x", "y");
        StoredProcedure sp = (StoredProcedure) TestResolver.helpResolve("exec proc()", tm);
        assertEquals(2, sp.getProjectedSymbols().size());
        assertEquals("y.proc.b", sp.getProjectedSymbols().get(1).toString());
        assertTrue(sp.returnsResultSet());

        sp.setCallableStatement(true);
        assertEquals(3, sp.getProjectedSymbols().size());
        assertEquals("y.proc.param", sp.getProjectedSymbols().get(2).toString());

        CreateProcedureCommand cpc = (CreateProcedureCommand) TestResolver.helpResolve("begin exec proc(); end", tm);
        assertEquals(2, cpc.getProjectedSymbols().size());
        assertEquals(2, ((CommandStatement)cpc.getBlock().getStatements().get(0)).getCommand().getProjectedSymbols().size());
        assertTrue(cpc.returnsResultSet());

        TestValidator.helpValidate("begin declare string var; var = exec proc(); select var; end", new String[] {"SELECT var;"}, tm);
    }

    @Test public void testDotInName() throws Exception {
        String ddl = "CREATE FOREIGN PROCEDURE \"my.proc\" (param STRING) RETURNS TABLE (a INTEGER, b STRING);"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.fromDDL(ddl, "x", "y");
        StoredProcedure sp = (StoredProcedure) TestResolver.helpResolve("exec \"my.proc\"()", tm);
        assertEquals(2, sp.getProjectedSymbols().size());
        assertEquals("y.my.proc.b", sp.getProjectedSymbols().get(1).toString());
        TestValidator.helpValidate("begin exec proc(); end", new String[] {}, tm);
    }

}
