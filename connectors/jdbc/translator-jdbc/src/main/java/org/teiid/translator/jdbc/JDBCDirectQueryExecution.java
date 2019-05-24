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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;


public class JDBCDirectQueryExecution extends JDBCQueryExecution implements ProcedureExecution {

    protected int columnCount;
    private List<Argument> arguments;
    protected int updateCount = -1;

    public JDBCDirectQueryExecution(List<Argument> arguments, Command command, Connection connection, ExecutionContext context, JDBCExecutionFactory env) {
        super(command, connection, context, env);
        this.arguments = arguments;
    }

    @Override
    public void execute() throws TranslatorException {
        String sourceSQL = (String) this.arguments.get(0).getArgumentValue().getValue();
        List<Argument> parameters = this.arguments.subList(1, this.arguments.size());

        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Source-specific command: ", sourceSQL); //$NON-NLS-1$
        context.logCommand(sourceSQL);
        int paramCount = parameters.size();

        try {
            Statement stmt;
            boolean hasResults = false;

            if(paramCount > 0) {
                PreparedStatement pstatement = getPreparedStatement(sourceSQL);
                for (int i = 0; i < paramCount; i++) {
                    Argument arg = parameters.get(i);
                    //TODO: if ParameterMetadata is supported we could use that type
                    this.executionFactory.bindValue(pstatement, arg.getArgumentValue().getValue(), arg.getArgumentValue().getType(), i + 1);
                }
                stmt = pstatement;
                hasResults = pstatement.execute();
            }
            else {
                //TODO: when array support becomes more robust calling like "exec native('sql', ARRAY[]) could still be prepared
                stmt = getStatement();
                hasResults = stmt.execute(sourceSQL);
            }

            if (hasResults) {
                this.results = stmt.getResultSet();
                this.columnCount = this.results.getMetaData().getColumnCount();
            }
            else {
                this.updateCount = stmt.getUpdateCount();
            }
            addStatementWarnings();
        } catch (SQLException e) {
             throw new JDBCExecutionException(JDBCPlugin.Event.TEIID11008, e, sourceSQL);
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        try {
            ArrayList<Object[]> row = new ArrayList<Object[]>(1);

            if (this.results != null) {
                if (this.results.next()) {
                    // New row for result set
                    List<Object> vals = new ArrayList<Object>(this.columnCount);

                    for (int i = 0; i < this.columnCount; i++) {
                        // Convert from 0-based to 1-based
                        Object value = this.executionFactory.retrieveValue(this.results, i+1, TypeFacility.RUNTIME_TYPES.OBJECT);
                        vals.add(value);
                    }
                    row.add(vals.toArray(new Object[vals.size()]));
                    return row;
                }
            }
            else if (this.updateCount != -1) {
                List<Object> vals = new ArrayList<Object>(1);
                vals.add(new Integer(this.updateCount));
                this.updateCount = -1;
                row.add(vals.toArray(new Object[vals.size()]));
                return row;
            }
        } catch (SQLException e) {
            throw new TranslatorException(e,JDBCPlugin.Util.getString("JDBCTranslator.Unexpected_exception_translating_results___8", e.getMessage())); //$NON-NLS-1$
        }
        return null;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return null;  //could support as an array of output values via given that the native procedure returns an array value
    }
}
