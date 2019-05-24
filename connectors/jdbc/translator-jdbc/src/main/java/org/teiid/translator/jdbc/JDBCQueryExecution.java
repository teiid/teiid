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

/*
 */

package org.teiid.translator.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;


/**
 *
 */
public class JDBCQueryExecution extends JDBCBaseExecution implements ResultSetExecution {

    private static final class RenamingVisitor extends HierarchyVisitor {
        private Map<String, String> nameMap;

        private RenamingVisitor(Map<String, String> nameMap) {
            super(true);
            this.nameMap = nameMap;
        }

        @Override
        public void visit(NamedTable obj) {
            if (obj.getMetadataObject() != null) {
                return;
            }
            String name = obj.getName();
            String val = nameMap.get(name);
            if (val != null) {
                obj.setName(val);
            }
            if (obj.getCorrelationName() == null) {
                obj.setCorrelationName(name);
            }
        }
    }

    private static final String KEY_TABLE_PREFIX = "TEIID_DKJ"; //$//$NON-NLS-1$
    private static final String FULL_TABLE_PREFIX = "TEIID_DJ"; //$//$NON-NLS-1$
    private static final String COL_PREFIX = "COL"; //$//$NON-NLS-1$

    protected ResultSet results;
    protected Class<?>[] columnDataTypes;
    protected List<NamedTable> tempTables;

    public JDBCQueryExecution(Command command, Connection connection, ExecutionContext context, JDBCExecutionFactory env) {
        super(command, connection, context, env);
    }

    @Override
    public void execute() throws TranslatorException {
        // get column types
        QueryExpression qe = (QueryExpression)command;

        columnDataTypes = qe.getColumnTypes();
        TranslatedCommand translatedComm = null;

        boolean usingTxn = false;
        boolean success = false;
        try {

            if (command instanceof Select) {
                Select select = (Select)command;
                if (select.getDependentValues() != null) {
                    usingTxn = createTempTables(select);
                }
            }
            if (qe.getWith() != null) {
                usingTxn = createFullTempTables(qe, usingTxn);
            }

            // translate command
            translatedComm = translateCommand(command);

            String sql = translatedComm.getSql();

            if (!translatedComm.isPrepared()) {
                results = getStatement().executeQuery(sql);
            } else {
                PreparedStatement pstatement = getPreparedStatement(sql);
                bind(pstatement, translatedComm.getPreparedValues(), null);
                results = pstatement.executeQuery();
            }
            addStatementWarnings();
            success = true;
        } catch (SQLException e) {
            if (translatedComm == null) {
                throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11008, e, command.toString());
            }
            throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11008, e, translatedComm);
        } finally {
            if (usingTxn) {
                try {
                    try {
                        if (success) {
                            connection.commit();
                        } else {
                            connection.rollback();
                        }
                    } finally {
                        connection.setAutoCommit(true);
                    }
                } catch (SQLException e) {
                }
            }
        }
    }

    /**
     *
     * @param qe
     * @param usingTxn
     * @return
     * @throws SQLException
     * @throws TranslatorException
     */
    protected boolean createFullTempTables(QueryExpression qe, boolean usingTxn)
            throws SQLException, TranslatorException {
        //TODO: should likely consolidate the two temp table mechansims

        With with = qe.getWith();
        int t = 1;
        Map<String, String> nameMap = null;
        for (Iterator<WithItem> iter = with.getItems().iterator(); iter.hasNext();) {
            WithItem item = iter.next();
            if (item.getDependentValues() == null) {
                continue;
            }
            List<ColumnReference> cols = item.getColumns();
            if (!usingTxn && this.executionFactory.tempTableRequiresTransaction() && connection.getAutoCommit()) {
                usingTxn = true;
                connection.setAutoCommit(false);
            }
            String tableName = this.executionFactory.createTempTable(FULL_TABLE_PREFIX + (t++), cols, this.context, getConnection());
            if (nameMap == null) {
                nameMap = new HashMap<String, String>();
            }
            nameMap.put(item.getTable().getName(), tableName);
            NamedTable table = new NamedTable(tableName, null, null);
            if (tempTables == null) {
                tempTables = new ArrayList<NamedTable>();
            }
            tempTables.add(table);
            iter.remove();
            List<Expression> params = new ArrayList<Expression>(item.getColumns().size());
            for (int i = 0; i < cols.size(); i++) {
                Parameter parameter = new Parameter();
                parameter.setType(cols.get(i).getType());
                parameter.setValueIndex(i);
                params.add(parameter);
            }
            loadTempTable(cols, params, tableName, table, item.getDependentValues());
        }
        //substitute the from with a real table name - TODO: associate real metadata through out
        if (nameMap != null) {
            HierarchyVisitor hv = new RenamingVisitor(nameMap);
            qe.acceptVisitor(hv);

            if (qe.getWith().getItems().isEmpty()) {
                qe.setWith(null);
            }
        }
        return usingTxn;
    }

    /**
     *
     * @param select
     * @return
     * @throws SQLException
     * @throws TranslatorException
     */
    protected boolean createTempTables(Select select) throws SQLException, TranslatorException {
        boolean result = false;
        if (this.executionFactory.tempTableRequiresTransaction() && connection.getAutoCommit()) {
            result = true;
            connection.setAutoCommit(false);
        }
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "creating temporary tables for key set dependent join processing"); //$NON-NLS-1$
        tempTables = new ArrayList<NamedTable>();
        Condition c = select.getWhere();
        List<Condition> conditions = LanguageUtil.separateCriteriaByAnd(c);
        Map<String, List<Comparison>> tables = new HashMap<String, List<Comparison>>();
        //build a list of comparisons for each dependent source
        for (Iterator<Condition> iter = conditions.iterator(); iter.hasNext();) {
            Condition condition = iter.next();
            //TODO: this would be easier with a specific type of condition
            if (!(condition instanceof Comparison)) {
                continue;
            }
            Comparison comp = (Comparison)condition;
            if (comp.getOperator() != Operator.EQ) {
                continue;
            }
            Parameter p = null;
            if (comp.getRightExpression() instanceof Parameter) {
                iter.remove();
                p = (Parameter)comp.getRightExpression();
            } else if (comp.getRightExpression() instanceof Array) {
                Array array = (Array)comp.getRightExpression();
                if (array.getExpressions().get(0) instanceof Parameter) {
                    iter.remove();
                    p = (Parameter)array.getExpressions().get(0);
                }
            }
            if (p == null) {
                continue;
            }
            List<Comparison> compares = tables.get(p.getDependentValueId());
            if (compares == null) {
                compares = new ArrayList<Comparison>();
                tables.put(p.getDependentValueId(), compares);
            }
            compares.add(comp);
        }

        //turn each dependent source into a temp table
        int t = 1;
        for (Map.Entry<String, List<Comparison>> entry : tables.entrySet()) {
            List<ColumnReference> cols = new ArrayList<ColumnReference>();
            List<Expression> params = new ArrayList<Expression>();
            for (Comparison comp : entry.getValue()) {
                Expression ex = comp.getLeftExpression();
                if (ex instanceof Array) {
                    Array array = (Array)ex;
                    params.addAll(((Array)comp.getRightExpression()).getExpressions());
                    for (Expression expr : array.getExpressions()) {
                        cols.add(createTempColumn(cols.size()+1, expr));
                    }
                } else {
                    params.add(comp.getRightExpression());
                    cols.add(createTempColumn(cols.size()+1, ex));
                }
            }
            //TODO: this should return a proper Table metadata object
            String tableName = this.executionFactory.createTempTable(KEY_TABLE_PREFIX + (t++), cols, this.context, getConnection());
            NamedTable table = new NamedTable(tableName, null, null);
            tempTables.add(table);

            select.getFrom().add(0, table); //TODO: assumes that ansi and non-ansi can be mixed
            //replace each condition with the appropriate comparison
            int i = 1;
            for (Comparison comp : entry.getValue()) {
                Expression ex = comp.getLeftExpression();
                if (ex instanceof Array) {
                    Array array = (Array)ex;
                    for (Expression expr : array.getExpressions()) {
                        conditions.add(new Comparison(expr, new ColumnReference(table, COL_PREFIX+i++, null, expr.getType()), Comparison.Operator.EQ));
                    }
                } else {
                    conditions.add(new Comparison(ex, new ColumnReference(table, COL_PREFIX+i++, null, ex.getType()), Comparison.Operator.EQ));
                }
            }

            //bulk load
            List<? extends List<?>> list = select.getDependentValues().get(entry.getKey());
            loadTempTable(cols, params, tableName, table, list);
        }

        select.setDependentValues(null);
        select.setWhere(LanguageUtil.combineCriteria(conditions));
        return result;
    }

    private void loadTempTable(List<ColumnReference> cols,
            List<Expression> params, String tableName, NamedTable table,
            List<? extends List<?>> vals) throws TranslatorException,
            SQLException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "loading temporary table", tableName, "with", vals.size(), "rows"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ExpressionValueSource evs = new ExpressionValueSource(params);
        for (ColumnReference col : cols) {
            col.setMetadataObject(null); //we don't want to confuse the insert handling
        }
        Insert insert = new Insert(table, cols, evs);
        insert.setParameterValues(vals.iterator());
        JDBCUpdateExecution ex = this.executionFactory.createUpdateExecution(insert, context, context.getRuntimeMetadata(), getConnection());
        int size = this.executionFactory.getMaxDependentInPredicates() * this.executionFactory.getMaxInCriteriaSize() / cols.size();
        ex.setMaxPreparedInsertBatchSize(Math.max(size, this.executionFactory.getMaxPreparedInsertBatchSize()));
        ex.setAtomic(false);
        ex.execute();
        ex.statement.close();
        this.executionFactory.loadedTemporaryTable(tableName, this.context, this.connection);
    }

    private ColumnReference createTempColumn(int i, Expression ex) {
        if (ex instanceof ColumnReference) {
            ColumnReference left = (ColumnReference)ex;
            return new ColumnReference(null, COL_PREFIX + i, left.getMetadataObject(), ex.getType());
        }
        //just an expression - there's a lot of metadata lost here
        return new ColumnReference(null, COL_PREFIX + i, null, ex.getType());
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        try {
            if (results.next()) {
                // New row for result set
                List<Object> vals = new ArrayList<Object>(columnDataTypes.length);

                for (int i = 0; i < columnDataTypes.length; i++) {
                    // Convert from 0-based to 1-based
                    Object value = this.executionFactory.retrieveValue(results, i+1, columnDataTypes[i]);
                    vals.add(value);
                }

                return vals;
            }
        } catch (SQLException e) {
            throw new TranslatorException(e,
                    JDBCPlugin.Util.getString("JDBCTranslator.Unexpected_exception_translating_results___8", e.getMessage())); //$NON-NLS-1$
        }

        return null;
    }

    /**
     * @see org.teiid.translator.jdbc.JDBCBaseExecution#close()
     */
    public void close() {
        // first we would need to close the result set here then we can close
        // the statement, using the base class.
        try {
            if (results != null) {
                try {
                    results.close();
                    results = null;
                } catch (SQLException e) {
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception closing"); //$NON-NLS-1$
                }
            }
        } finally {
            if (tempTables != null) {
                Statement s = null;
                try {
                    s = getConnection().createStatement();
                    for (NamedTable temp : tempTables) {
                        try {
                            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "dropping temporary table", temp.getName()); //$NON-NLS-1$
                            s.execute(this.executionFactory.getDialect().getDefaultMultiTableBulkIdStrategy().getIdTableSupport().getDropIdTableCommand() + " " + temp.getName()); //$NON-NLS-1$
                        } catch (SQLException e) {
                            //TODO: could refine this logic as drop is being performed as part of the txn cleanup for some sources
                        }
                    }
                } catch (SQLException e1) {
                } finally {
                    tempTables = null;
                    if (s != null) {
                        try {
                            s.close();
                        } catch (SQLException e) {
                        }
                    }
                }
            }
            super.close();
        }
    }

}
