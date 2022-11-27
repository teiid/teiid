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

package org.teiid.query.processor.proc;

import static org.junit.Assert.*;
import static org.teiid.query.processor.proc.TestProcedureProcessor.*;

import java.util.Arrays;
import java.util.List;

import javax.transaction.Transaction;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.dqp.service.TransactionService;
import org.teiid.events.EventDistributor;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.resolver.TestProcedureResolving;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestProcErrors {

    @Test public void testInvalidException() throws Exception {
        String ddl =
                "create virtual procedure vproc (x integer) returns integer as begin declare object e = sqlexception 'hello'; raise e; raise sqlexception 'hello world' sqlstate 'abc', 1 chain e; end;";
        try {
            TestProcedureResolving.createMetadata(ddl);
            fail();
        } catch (RuntimeException e) {
            assertEquals("TEIID31080 test.vproc validation error: TEIID31120 An exception may only be chained to another exception. e is not valid.", e.getMessage());
        }
    }

    @Test public void testExceptionAndWarning() throws Exception {
        String ddl =
                "create virtual procedure vproc (x integer) returns integer as begin declare exception e = sqlexception 'hello'; raise sqlwarning e; raise sqlexception 'hello world' sqlstate 'abc', 1 chain e; end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call vproc(1)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);

        try {
            helpTestProcess(plan, null, dataManager, tm);
            fail();
        } catch (TeiidProcessingException e) {
            TeiidSQLException tsw = (TeiidSQLException) plan.getContext().getAndClearWarnings().get(0);
            assertEquals("hello", tsw.getMessage());

            assertEquals(e.getCause().getCause(), tsw);
            TeiidSQLException tse = (TeiidSQLException)e.getCause();
            assertEquals("hello world", tse.getMessage());
            assertEquals("abc", tse.getSQLState());
            assertEquals(1, tse.getErrorCode());
        }
    }

    @Test public void testExceptionGroup() throws Exception {
        String ddl =
                "create virtual procedure vproc () returns string as begin select 1/0; exception e \"return\" = e.state || ' ' || e.errorcode || ' ' || e.teiidcode || ' ' || cast(e.exception as string) || ' ' || cast(e.chain as string); end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call vproc()"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);

        helpTestProcess(plan, new List[] {Arrays.asList("50000 30328 TEIID30328 org.teiid.jdbc.TeiidSQLException: TEIID30328 Unable to evaluate (1 / 0): TEIID30384 Error while evaluating function / org.teiid.api.exception.query.ExpressionEvaluationException: TEIID30328 Unable to evaluate (1 / 0): TEIID30384 Error while evaluating function /")}, dataManager, tm);
    }

    @Test public void testExceptionHandling() throws Exception {
        String ddl =
                "create virtual procedure vproc (x integer) returns integer as begin " +
                "raise sqlexception 'hello world' sqlstate 'abc', 1;" +
                "exception e " +
                "raise sqlwarning sqlexception 'caught' chain e.exception; " +
                "\"return\" = 1;"+
                "end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call vproc(1)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);

        helpTestProcess(plan, new List[] {Arrays.asList(1)}, dataManager, tm);

        TeiidSQLException tse = (TeiidSQLException)plan.getContext().getAndClearWarnings().get(0);
        assertEquals("caught", tse.getMessage());
        assertEquals("hello world", tse.getCause().getMessage());
    }

    /**
     * ensures that a processing error is trappable
     */
    @Test public void testExceptionHandlingWithResultSet() throws Exception {
        String ddl =
                "create virtual procedure proc2 (x integer) returns table(y integer) as begin atomic select 1; begin select 1/x; end exception e end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call proc2(0)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);

        helpTestProcess(plan, new List[] {Arrays.asList(1)}, dataManager, tm);
    }

    /**
     * ensures that the whole result is formed so that the error does not escape the handler
     */
    @Test public void testExceptionHandlingWithResultSet1() throws Exception {
        String ddl =
                "create virtual procedure proc2 (x integer) as begin create local temporary table t (i integer); insert into t (i) values (1); declare integer y = 0; while (y < 16) begin insert into t (i) select 1 from t; y = y+1; end insert into t (i) values (0); select cast(1/i as string) from t; exception e end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call proc2(0)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);

        helpTestProcess(plan, new List[0], dataManager, tm);
    }

    @Test public void testExceptionHandlingWithDynamic() throws Exception {
        String ddl =
                "create virtual procedure vproc (x integer) returns integer as begin " +
                "raise sqlexception 'hello world' sqlstate 'abc', 5;" +
                "exception e " +
                "execute immediate 'select \"ERRORCODE\"' as x integer into #temp; " +
                "\"return\" = (select x from #temp);"+
                "end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call vproc(1)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);

        helpTestProcess(plan, new List[] {Arrays.asList(5)}, dataManager, tm);
    }

    @Test public void testDynamicAnon() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        String query = "BEGIN atomic\n"
                + " declare string VARIABLES.RESULT = 1/0;\n"
                + " select VARIABLES.RESULT;" +
                "exception e " +
                "execute immediate 'select \"ERRORCODE\"' as x string into #temp;" +
                " select x from #temp; end";

        ProcessorPlan plan = getProcedurePlan(query, metadata);

        // Create expected results
        List[] expected = new List[] { Arrays.asList("30328"), //$NON-NLS-1$
        };
        helpTestProcess(plan, expected, new HardcodedDataManager(), metadata);
    }

    /**
     * Ensures that values in the block are not resolvable
     */
    @Test(expected=QueryResolverException.class) public void testErrorResolving() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        String query = "BEGIN atomic\n"
                + " declare string VARIABLES.RESULT = 1/0;\n"
                + " select VARIABLES.RESULT;" +
                "exception e " +
                "execute immediate 'select \"ERRORCODE\"' || VARIABLES.RESULT as x string into #temp;" +
                " select x from #temp; end";

        getProcedurePlan(query, metadata);
    }

    @Test public void testNestedBeginAtomicException() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.example1Cached();
        String query = "BEGIN atomic\n"
                + " declare string VARIABLES.RESULT;\n"
                + " begin atomic select 1/0; exception e end end";

        ProcessorPlan plan = getProcedurePlan(query, tm);

        // Create expected results
        List<?>[] expected = new List[0];

        CommandContext context = new CommandContext("pID", null, null, null, 1); //$NON-NLS-1$
        QueryMetadataInterface metadata = new TempMetadataAdapter(tm, new TempMetadataStore());
        context.setMetadata(metadata);

        TransactionContext tc = new TransactionContext();
        Transaction txn = Mockito.mock(Transaction.class);
        tc.setTransaction(txn);
        tc.setTransactionType(Scope.REQUEST);
        TransactionService ts = Mockito.mock(TransactionService.class);
        context.setTransactionService(ts);
        context.setTransactionContext(tc);

        TestProcessor.helpProcess(plan, context, new HardcodedDataManager(), expected);

        Mockito.verify(txn, Mockito.times(3)).setRollbackOnly();
    }

    @Test public void testExceptionHandlingWithLoops() throws Exception {
        String ddl =
                "create virtual procedure proc2 (out x integer result) as "
                + "begin create local temporary table t (i integer); insert into t (i) values (0); "
                + "begin loop on (select * from t) as x select 1/0; exception e end "
                + "insert into t (i) values (1); "
                + "declare integer result = 0; "
                + "loop on (select * from t) as x result = result + 1; "
                + "x = result;"
                + "select result; end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call proc2()"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);

        helpTestProcess(plan, new List[] {Arrays.asList(2)}, dataManager, tm);
    }

    @Test public void testExceptionHandlingDynamicError() throws Exception {
        String ddl =
                "create virtual procedure vproc (x integer) returns integer as begin " +
                "execute immediate 'select x/0';" +
                "exception e " +
                "execute immediate 'select x' as x integer into #temp; " +
                "\"return\" = (select x from #temp);"+
                "end;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "call vproc(1)"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);

        helpTestProcess(plan, new List[] {Arrays.asList(1)}, dataManager, tm);
    }

    @Test public void testExceptionHandlingLoopException() throws Exception {
        String ddl =
                "create virtual procedure pr() as\n" +
                "begin\n" +
                "    error 'a';\n" +
                "end ;;";
        TransformationMetadata tm = TestProcedureResolving.createMetadata(ddl);

        String sql = "BEGIN\n" +
                "    LOOP ON (SELECT 1 union all select 2) AS market\n" +
                "    BEGIN\n" +
                "        CALL pr();\n" +
                "    EXCEPTION e\n" +
                "    END\n" +
                "END ;"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);

        helpTestProcess(plan, new List[] {}, dataManager, tm);
    }

    @Test public void testExceptionHandlingLoopExceptionUpdate() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.example1Cached();

        String sql = "BEGIN\n" +
                "    LOOP ON (SELECT 1 union all select 2) AS market\n" +
                "    BEGIN\n" +
                "        insert into pm1.g1 (e1) values ('a');\n" +
                "    EXCEPTION e\n" +
                "    END\n" +
                "END ;"; //$NON-NLS-1$

        ProcessorPlan plan = getProcedurePlan(sql, tm);

        ProcessorDataManager dataManager = new ProcessorDataManager() {

            @Override
            public TupleSource registerRequest(CommandContext context,
                    Command command, String modelName,
                    RegisterRequestParameter parameterObject)
                    throws TeiidComponentException, TeiidProcessingException {
                return new TupleSource() {

                    @Override
                    public List<?> nextTuple()
                            throws TeiidComponentException, TeiidProcessingException {
                        throw new TeiidProcessingException(); //will be caught
                    }

                    @Override
                    public void closeSource() {

                    }
                };
            }

            @Override
            public Object lookupCodeValue(CommandContext context,
                    String codeTableName, String returnElementName,
                    String keyElementName, Object keyValue)
                    throws BlockedException, TeiidComponentException,
                    TeiidProcessingException {
                return null;
            }

            @Override
            public EventDistributor getEventDistributor() {
                return null;
            }
        };

        helpTestProcess(plan, new List[] {}, dataManager, tm);
    }

}
