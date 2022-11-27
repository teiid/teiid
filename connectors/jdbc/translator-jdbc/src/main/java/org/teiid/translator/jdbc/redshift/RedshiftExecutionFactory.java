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

package org.teiid.translator.jdbc.redshift;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.postgresql.PostgreSQLExecutionFactory;



/**
 * Translator class for Red Shift.
 */
@Translator(name="redshift", description="A translator for Redshift")
public class RedshiftExecutionFactory extends PostgreSQLExecutionFactory {

    @Override
    public void start() throws TranslatorException {
        super.start();
        getFunctionModifiers().remove(SourceSystemFunctions.SUBSTRING); //redshift doesn't use substr
        //to_timestamp is not supported
        this.parseModifier.setPrefix("TO_DATE("); //$NON-NLS-1$
        //redshift needs explicit precision/scale
        this.convertModifier.addTypeMapping("decimal(38, 19)", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
    }

    @Override
    public void intializeConnectionAfterCancel(Connection c)
            throws SQLException {
        //cancel can leave the connection in an invalid state, issue another query to clear any flags
        Statement s = c.createStatement();
        try {
            s.execute("select 1"); //$NON-NLS-1$
        } finally {
            s.close();
        }
    }

    @Override
    public boolean hasTimeType() {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        if (toType == TypeFacility.RUNTIME_CODES.TIME) {
            return false;
        }
        return super.supportsConvert(fromType, toType);
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> functions = super.getSupportedFunctions();
        functions.remove(SourceSystemFunctions.ASCII);
        return functions;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return false;
    }

    @Override
    public Object convertToken(String group) {
        //timezone not supported
        if (group.charAt(0) == 'Z') {
            throw new IllegalArgumentException();
        }
        //TODO: time fields are probably not supported for parsing
        return super.convertToken(group);
    }

    @Override
    public String getCreateTemporaryTablePostfix(boolean inTransaction) {
        return ""; //$NON-NLS-1$ //redshift does not support the ON COMMIT clause
    }

}
