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

package org.teiid.translator.jdbc;

import static org.junit.Assert.*;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.GeometryType;
import org.teiid.dqp.internal.datamgr.FakeExecutionContextImpl;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.Parameter;
import org.teiid.translator.TranslatorBatchException;

@SuppressWarnings("nls")
public class TestJDBCUpdateExecution {

    @Test public void testInsertIteratorUpdate() throws Exception {
        Insert command = (Insert)TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "insert into BQT1.SmallA (IntKey, IntNum) values (1, 2)"); //$NON-NLS-1$
        Parameter param = new Parameter();
        param.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        param.setValueIndex(0);
        List<Expression> values = ((ExpressionValueSource)command.getValueSource()).getValues();
        values.set(0, param);
        param = new Parameter();
        param.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        param.setValueIndex(1);
        values.set(1, param);
        command.setParameterValues(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(1, 2)).iterator());
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement p = Mockito.mock(PreparedStatement.class);
        Mockito.stub(p.executeBatch()).toReturn(new int [] {1, 1});
        Mockito.stub(connection.prepareStatement("INSERT INTO SmallA (IntKey, IntNum) VALUES (?, ?)")).toReturn(p); //$NON-NLS-1$

        JDBCExecutionFactory config = new JDBCExecutionFactory();

        JDBCUpdateExecution updateExecution = new JDBCUpdateExecution(command, connection, new FakeExecutionContextImpl(), config);
        updateExecution.execute();
        Mockito.verify(p, Mockito.times(2)).addBatch();
    }

   @Test public void testPreparedInsertWithGeometry() throws Exception {
        Insert command = (Insert)TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "insert into cola_markets(name,shape) values('foo124', ST_GeomFromText('POINT (300 100)', 8307))"); //$NON-NLS-1$
        Parameter param = new Parameter();
        param.setType(DataTypeManager.DefaultDataClasses.STRING);
        param.setValueIndex(0);
        List<Expression> values = ((ExpressionValueSource)command.getValueSource()).getValues();
        values.set(0, param);
        param = new Parameter();
        param.setType(DataTypeManager.DefaultDataClasses.GEOMETRY);
        param.setValueIndex(1);
        values.set(1, param);
        GeometryType value = new GeometryType();
        value.setSrid(123);
        command.setParameterValues(Arrays.asList(Arrays.asList("a", value)).iterator());
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement p = Mockito.mock(PreparedStatement.class);
        Mockito.stub(p.executeBatch()).toReturn(new int [] {1});
        Mockito.stub(connection.prepareStatement("INSERT INTO COLA_MARKETS (NAME, SHAPE) VALUES (?, st_geomfromwkb(?, ?))")).toReturn(p); //$NON-NLS-1$

        JDBCExecutionFactory config = new JDBCExecutionFactory();

        JDBCUpdateExecution updateExecution = new JDBCUpdateExecution(command, connection, new FakeExecutionContextImpl(), config);
        updateExecution.execute();
        Mockito.verify(p, Mockito.times(1)).addBatch();
        Mockito.verify(p, Mockito.times(1)).setObject(1, "a", Types.VARCHAR);
        Mockito.verify(p, Mockito.times(1)).setInt(3, 123);
    }

    @Test public void testAutoGeneretionKeys() throws Exception {
        Insert command = (Insert)TranslationHelper.helpTranslate("create foreign table SmallA (IntKey integer auto_increment primary key, IntNum integer)", "insert into SmallA (IntKey, IntNum) values (1, 2)"); //$NON-NLS-1$

        Connection connection = Mockito.mock(Connection.class);
        Statement s = Mockito.mock(Statement.class);
        Mockito.stub(connection.createStatement()).toReturn(s);

        JDBCExecutionFactory config = new JDBCExecutionFactory() {
            @Override
            public boolean supportsGeneratedKeys() {
                return true;
            }

            @Override
            public boolean useColumnNamesForGeneratedKeys() {
                return true;
            }
        };
        ResultSet r = Mockito.mock(ResultSet.class);
        ResultSetMetaData rs = Mockito.mock(ResultSetMetaData.class);
        Mockito.stub(r.getMetaData()).toReturn(rs);

        Mockito.stub(s.getGeneratedKeys()).toReturn(r);

        FakeExecutionContextImpl context = new FakeExecutionContextImpl();
        context.setGeneratedKeyColumns(command);
        JDBCUpdateExecution updateExecution = new JDBCUpdateExecution(command, connection, context, config);
        updateExecution.execute();
        Mockito.verify(s, Mockito.times(1)).getGeneratedKeys();
        Mockito.verify(s, Mockito.times(1)).executeUpdate("INSERT INTO SmallA (IntKey, IntNum) VALUES (1, 2)", new String[] {"IntKey"});

        config = new JDBCExecutionFactory() {
            @Override
            public boolean supportsGeneratedKeys() {
                return true;
            }
        };

        s = Mockito.mock(Statement.class);
        Mockito.stub(connection.createStatement()).toReturn(s);
        Mockito.stub(s.getGeneratedKeys()).toReturn(r);

        updateExecution = new JDBCUpdateExecution(command, connection, context, config);
        updateExecution.execute();
        Mockito.verify(s, Mockito.times(1)).getGeneratedKeys();
        Mockito.verify(s, Mockito.times(1)).executeUpdate("INSERT INTO SmallA (IntKey, IntNum) VALUES (1, 2)", Statement.RETURN_GENERATED_KEYS);
    }

    @Test public void testAutoGeneretionKeysPrepared() throws Exception {
        Insert command = (Insert)TranslationHelper.helpTranslate("create foreign table SmallA (IntKey integer auto_increment primary key, IntNum integer)", "insert into SmallA (IntKey, IntNum) values (1, 2)"); //$NON-NLS-1$
        ((Literal)((ExpressionValueSource)command.getValueSource()).getValues().get(0)).setBindEligible(true);
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement s = Mockito.mock(PreparedStatement.class);
        Mockito.stub(connection.prepareStatement("INSERT INTO SmallA (IntKey, IntNum) VALUES (?, 2)", new String[] {"IntKey"})).toReturn(s);

        JDBCExecutionFactory config = new JDBCExecutionFactory() {
            @Override
            public boolean supportsGeneratedKeys() {
                return true;
            }

            @Override
            public boolean useColumnNamesForGeneratedKeys() {
                return true;
            }
        };
        ResultSet r = Mockito.mock(ResultSet.class);
        Mockito.stub(r.next()).toReturn(true).toReturn(false);

        ResultSetMetaData rs = Mockito.mock(ResultSetMetaData.class);
        Mockito.stub(r.getMetaData()).toReturn(rs);

        Mockito.stub(s.getGeneratedKeys()).toReturn(r);

        FakeExecutionContextImpl context = new FakeExecutionContextImpl();
        context.setGeneratedKeyColumns(command);
        JDBCUpdateExecution updateExecution = new JDBCUpdateExecution(command, connection, context, config);
        updateExecution.execute();
        Mockito.verify(s, Mockito.times(1)).getGeneratedKeys();
        Mockito.verify(s, Mockito.times(1)).executeUpdate();

        config = new JDBCExecutionFactory() {
            @Override
            public boolean supportsGeneratedKeys() {
                return true;
            }
        };

        assertEquals(0, context.getCommandContext().getGeneratedKeys().getKeyIterator().next().get(0));
    }

    @Test public void testBulkUpdate() throws Exception {
        Insert command = (Insert)TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "insert into BQT1.SmallA (IntKey) values (1)"); //$NON-NLS-1$
        Parameter param = new Parameter();
        param.setType(Integer.class);
        param.setValueIndex(0);
        ExpressionValueSource evs = new ExpressionValueSource(Arrays.asList((Expression)param));
        command.setValueSource(evs);
        List<List<?>> vals = new ArrayList<List<?>>();
        for (int i = 0; i < 8; i++) {
            vals.add(Arrays.asList(i));
        }
        command.setParameterValues(vals.iterator());
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement p = Mockito.mock(PreparedStatement.class);
        Mockito.stub(p.executeBatch()).toReturn(new int[] {1, 1});
        Mockito.stub(connection.prepareStatement("INSERT INTO SmallA (IntKey) VALUES (?)")).toReturn(p); //$NON-NLS-1$

        JDBCExecutionFactory config = new JDBCExecutionFactory();
        config.setMaxPreparedInsertBatchSize(2);
        ResultSet r = Mockito.mock(ResultSet.class);
        ResultSetMetaData rs = Mockito.mock(ResultSetMetaData.class);
        Mockito.stub(r.getMetaData()).toReturn(rs);

        Mockito.stub(p.getGeneratedKeys()).toReturn(r);

        FakeExecutionContextImpl context = new FakeExecutionContextImpl();

        JDBCUpdateExecution updateExecution = new JDBCUpdateExecution(command, connection, context, config);
        updateExecution.execute();
        assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1, 1, 1}, updateExecution.getUpdateCounts());
    }

    @Test public void testBatchedUpdate() throws Exception {
        Insert command = (Insert)TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "insert into BQT1.SmallA (IntKey) values (1)"); //$NON-NLS-1$
        Insert command1 = (Insert)TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "insert into BQT1.SmallA (StringKey) values ('1')"); //$NON-NLS-1$
        Connection connection = Mockito.mock(Connection.class);
        Statement s = Mockito.mock(Statement.class);
        Mockito.stub(s.executeBatch()).toReturn(new int[] {1, 1});
        Mockito.stub(connection.createStatement()).toReturn(s);

        JDBCExecutionFactory config = new JDBCExecutionFactory();
        ResultSet r = Mockito.mock(ResultSet.class);
        ResultSetMetaData rs = Mockito.mock(ResultSetMetaData.class);
        Mockito.stub(r.getMetaData()).toReturn(rs);

        Mockito.stub(s.getGeneratedKeys()).toReturn(r);

        FakeExecutionContextImpl context = new FakeExecutionContextImpl();

        JDBCUpdateExecution updateExecution = new JDBCUpdateExecution(new BatchedUpdates(Arrays.asList((Command)command, command1)), connection, context, config);
        updateExecution.execute();
        assertArrayEquals(new int[] {1, 1}, updateExecution.getUpdateCounts());
    }

    @Test public void testBatchedUpdateFailed() throws Exception {
        Insert command = (Insert)TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "insert into BQT1.SmallA (IntKey) values (1)"); //$NON-NLS-1$
        Insert command1 = (Insert)TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "insert into BQT1.SmallA (StringKey) values ('1')"); //$NON-NLS-1$
        Connection connection = Mockito.mock(Connection.class);
        Statement s = Mockito.mock(Statement.class);
        Mockito.stub(s.executeBatch()).toThrow(new BatchUpdateException(new int[] {Statement.EXECUTE_FAILED}));
        Mockito.stub(connection.createStatement()).toReturn(s);

        JDBCExecutionFactory config = new JDBCExecutionFactory();
        ResultSet r = Mockito.mock(ResultSet.class);
        ResultSetMetaData rs = Mockito.mock(ResultSetMetaData.class);
        Mockito.stub(r.getMetaData()).toReturn(rs);

        Mockito.stub(s.getGeneratedKeys()).toReturn(r);

        FakeExecutionContextImpl context = new FakeExecutionContextImpl();

        JDBCUpdateExecution updateExecution = new JDBCUpdateExecution(new BatchedUpdates(Arrays.asList((Command)command, command1)), connection, context, config);
        try {
            updateExecution.execute();
            fail();
        } catch (TranslatorBatchException e) {
            int[] counts = e.getUpdateCounts();
            assertArrayEquals(new int[] {-3}, counts);
        }
    }

    @Test public void testPreparedBatchedUpdateFailed() throws Exception {
        Insert command = (Insert)TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "insert into BQT1.SmallA (IntKey) values (1)"); //$NON-NLS-1$
        Parameter param = new Parameter();
        param.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        param.setValueIndex(0);
        List<Expression> values = ((ExpressionValueSource)command.getValueSource()).getValues();
        values.set(0, param);
        command.setParameterValues(Arrays.asList(Arrays.asList(1), Arrays.asList(1)).iterator());

        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement s = Mockito.mock(PreparedStatement.class);
        Mockito.stub(s.executeBatch()).toThrow(new BatchUpdateException(new int[] {1, Statement.EXECUTE_FAILED}));
        Mockito.stub(connection.prepareStatement("INSERT INTO SmallA (IntKey) VALUES (?)")).toReturn(s);

        JDBCExecutionFactory config = new JDBCExecutionFactory();
        ResultSet r = Mockito.mock(ResultSet.class);
        ResultSetMetaData rs = Mockito.mock(ResultSetMetaData.class);
        Mockito.stub(r.getMetaData()).toReturn(rs);

        FakeExecutionContextImpl context = new FakeExecutionContextImpl();

        JDBCUpdateExecution updateExecution = new JDBCUpdateExecution(command, connection, context, config);
        try {
            updateExecution.execute();
            fail();
        } catch (TranslatorBatchException e) {
            int[] counts = e.getUpdateCounts();
            assertArrayEquals(new int[] {1, -3}, counts);
        }

        //test multiple batches
        connection = Mockito.mock(Connection.class);
        updateExecution = new JDBCUpdateExecution(command, connection, context, config);
        command.setParameterValues(Arrays.asList(Arrays.asList(1), Arrays.asList(1)).iterator());
        s = Mockito.mock(PreparedStatement.class);
        Mockito.stub(connection.prepareStatement("INSERT INTO SmallA (IntKey) VALUES (?)")).toReturn(s);
        Mockito.stub(s.executeBatch())
        .toReturn(new int[] {1})
        .toThrow(new BatchUpdateException(new int[] {Statement.EXECUTE_FAILED}));
        updateExecution.setMaxPreparedInsertBatchSize(1);
        try {
            updateExecution.execute();
            fail();
        } catch (TranslatorBatchException e) {
            int[] counts = e.getUpdateCounts();
            assertArrayEquals(new int[] {1, -3}, counts);
        }

        //test only a single update count
        connection = Mockito.mock(Connection.class);
        updateExecution = new JDBCUpdateExecution(command, connection, context, config);
        command.setParameterValues(Arrays.asList(Arrays.asList(1), Arrays.asList(1)).iterator());
        s = Mockito.mock(PreparedStatement.class);
        Mockito.stub(connection.prepareStatement("INSERT INTO SmallA (IntKey) VALUES (?)")).toReturn(s);
        Mockito.stub(s.executeBatch())
        .toThrow(new BatchUpdateException(new int[] {1}));
        try {
            updateExecution.execute();
            fail();
        } catch (TranslatorBatchException e) {
            int[] counts = e.getUpdateCounts();
            assertArrayEquals(new int[] {1}, counts);
        }
    }
}
