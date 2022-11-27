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

import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.BulkCommand;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.Parameter;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;


/**
 * This is a utility class used to translate an ICommand using a SQLConversionVisitor.
 * The SQLConversionVisitor should not be invoked directly; this object will use it to
 * translate the ICommand.
 */
public class TranslatedCommand {

    private String sql;
    private boolean prepared;
    private List preparedValues;

    private JDBCExecutionFactory executionFactory;
    private ExecutionContext context;

    public TranslatedCommand(ExecutionContext context, JDBCExecutionFactory executionFactory){
        this.executionFactory = executionFactory;
        this.context = context;
    }

    /**
     * The method to cause this object to do it's thing.  This method should
     * be called right after the constructor; afterward, all of the getter methods
     * can be called to retrieve results.
     * @param command ICommand to be translated
     * @throws TranslatorException
     */
    public void translateCommand(Command command) throws TranslatorException {
        SQLConversionVisitor sqlConversionVisitor = executionFactory.getSQLConversionVisitor();
        sqlConversionVisitor.setExecutionContext(context);
        if (executionFactory.usePreparedStatements() || hasBindValue(command)) {
            sqlConversionVisitor.setPrepared(true);
        }

        sqlConversionVisitor.append(command);
        this.sql = sqlConversionVisitor.toString();
        this.preparedValues = sqlConversionVisitor.getPreparedValues();
        this.prepared = command instanceof BulkCommand?sqlConversionVisitor.isUsingBinding():sqlConversionVisitor.isPrepared();
    }

    /**
     * Simple check to see if any values in the command should be replaced with bind values
     *
     * @param command
     * @return
     */
    private boolean hasBindValue(Command command) {
        if (!CollectorVisitor.collectObjects(Parameter.class, command).isEmpty()) {
            return true;
        }
        for (Literal l : CollectorVisitor.collectObjects(Literal.class, command)) {
            if (isBindEligible(l)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param l
     * @return
     */
    static boolean isBindEligible(Literal l) {
        return DataTypeManager.isLOB(l.getType())
                || TypeFacility.RUNTIME_TYPES.OBJECT.equals(l.getType());
    }

    /**
     * Return List of values to set on a prepared statement, if
     * necessary.
     * @return List of values to be set on a prepared statement
     */
    public List getPreparedValues() {
        return preparedValues;
    }

    /**
     * Get String SQL of translated command.
     * @return SQL of translated command
     */
    public String getSql() {
        return sql;
    }

    /**
     * Returns whether the statement is prepared.
     * @return true if the statement is prepared
     */
    public boolean isPrepared() {
        return prepared;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        if (prepared) {
            sb.append("Prepared Values: ").append(preparedValues).append(" "); //$NON-NLS-1$ //$NON-NLS-2$
        }
        sb.append("SQL: ").append(sql); //$NON-NLS-1$
        return sb.toString();
    }

}
