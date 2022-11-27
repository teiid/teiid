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
package org.teiid.translator.olap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;

import org.olap4j.Axis;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;
import org.olap4j.Position;
import org.olap4j.metadata.Member;
import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

/**
 * Executes the given MDX and packs the results into an array
 */
public class OlapQueryExecution implements ProcedureExecution {

    protected Command command;
    protected OlapConnection connection;
    protected ExecutionContext context;
    protected OlapExecutionFactory executionFactory;
    private OlapStatement stmt;
    private CellSet cellSet;
    private CellSetAxis columnsAxis;
    private int colWidth;
    private ListIterator<Position> rowPositionIterator;
    private String mdxQuery;
    private boolean returnsArray;

    public OlapQueryExecution(List<Argument> arguments, Command command, OlapConnection connection, ExecutionContext context, OlapExecutionFactory executionFactory, String mdxQuery, boolean returnsArray) {
        this.mdxQuery = mdxQuery;
        if (arguments.size() > 0 || !returnsArray) { //TODO this is a hack at backwards compatibility
            StringBuilder buffer = new StringBuilder();
            SQLStringVisitor.parseNativeQueryParts(mdxQuery, arguments, buffer, new SQLStringVisitor.Substitutor() {

                @Override
                public void substitute(Argument arg, StringBuilder builder, int index) {
                    Literal argumentValue = arg.getArgumentValue();
                    Object value = argumentValue.getValue();
                    if (value == null || value instanceof Number || value instanceof Boolean || value instanceof String) {
                        builder.append(argumentValue);
                    } else if (value instanceof Date) {
                        //bind as a string literal
                        builder.append(new Literal(value.toString(), String.class));
                    } else {
                        //bind as a string literal using the teiid format - this is likely not what the user wants
                        builder.append(new Literal(argumentValue.toString(), String.class));
                    }
                }
            });
            this.mdxQuery = buffer.toString();
        }
        this.command = command;
        this.connection = connection;
        this.context = context;
        this.executionFactory = executionFactory;
        this.returnsArray = returnsArray;
    }

    @Override
    public void execute() throws TranslatorException {
        try {
            stmt = this.connection.createStatement();
            cellSet = stmt.executeOlapQuery(mdxQuery);
            CellSetAxis rowAxis = this.cellSet.getAxes().get(Axis.ROWS.axisOrdinal());
            rowPositionIterator = rowAxis.iterator();
            columnsAxis = cellSet.getAxes().get(Axis.COLUMNS.axisOrdinal());
            colWidth = rowAxis.getAxisMetaData().getHierarchies().size() + this.columnsAxis.getPositions().size();
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public void cancel() throws TranslatorException {
        try {
            OlapStatement olapStatement = this.stmt;
            if (olapStatement != null) {
                olapStatement.cancel();
            }
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (this.stmt != null) {
                this.stmt.close();
                this.stmt = null;
            }
        } catch (SQLException e) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception closing"); //$NON-NLS-1$
        }
    }

    @Override
    public List<?> next() throws TranslatorException {
        if (!rowPositionIterator.hasNext()) {
            return null;
        }
        Position rowPosition = rowPositionIterator.next();
        Object[] result = new Object[colWidth];
        int i = 0;
        // add in rows axis
        List<Member> members = rowPosition.getMembers();
        for (Member member:members) {
            String columnName = member.getName();
            result[i++] = columnName;
        }

        // add col axis
        for (Position colPos : columnsAxis) {
            Cell cell = cellSet.getCell(colPos, rowPosition);
            result[i++] = cell.getValue();
        }
        if (returnsArray) {
            ArrayList<Object[]> results = new ArrayList<Object[]>(1);
            results.add(result);
            return results;
        }
        return Arrays.asList(result);
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return null;
    }

}
