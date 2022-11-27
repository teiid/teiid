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

package org.teiid.translator.jdbc.db2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCMetadataProcessor;
import org.teiid.util.Version;

@Translator(name="db2", description="A translator for IBM DB2 Database")
public class DB2ExecutionFactory extends BaseDB2ExecutionFactory {

    public static final Version NINE_1 = Version.getVersion("9.1"); //$NON-NLS-1$
    public static final Version NINE_5 = Version.getVersion("9.5"); //$NON-NLS-1$
    public static final Version TEN_0 = Version.getVersion("10.0"); //$NON-NLS-1$

    public static final Version SIX_1 = Version.getVersion("6.1"); //$NON-NLS-1$
    private static final String WEEK_ISO = "WEEK_ISO"; //$NON-NLS-1$

    private boolean dB2ForI;

    private boolean supportsCommonTableExpressions = true;

    public DB2ExecutionFactory() {
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
        supportedFunctions.add("ABS"); //$NON-NLS-1$
        supportedFunctions.add("ACOS"); //$NON-NLS-1$
        supportedFunctions.add("ASIN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        supportedFunctions.add("CEILING"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add("COT"); //$NON-NLS-1$
        supportedFunctions.add("DEGREES"); //$NON-NLS-1$
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        //supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        //supportedFunctions.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        //supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("RAND"); //$NON-NLS-1$
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        //supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        //supportedFunctions.add("TRANSLATE"); //$NON-NLS-1$
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add("DAY"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL"); //$NON-NLS-1$
        supportedFunctions.add("COALESCE"); //$NON-NLS-1$
        if (getVersion().compareTo(isdB2ForI()?SIX_1:NINE_5) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.ROUND);
        }
        return supportedFunctions;
    }

    @Override
    public boolean supportsFunctionsInGroupBy() {
        return !dB2ForI;
    }

    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
        return true;
    }

    public void setSupportsCommonTableExpressions(boolean supportsCommonTableExpressions) {
        this.supportsCommonTableExpressions = supportsCommonTableExpressions;
    }

    @TranslatorProperty(display="Supports Common Table Expressions", description="Supports Common Table Expressions",advanced=true)
    @Override
    public boolean supportsCommonTableExpressions() {
        return supportsCommonTableExpressions;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsElementaryOlapOperations() {
        return getVersion().compareTo(isdB2ForI()?SIX_1:NINE_1) >= 0;
    }

    @Override
    public boolean supportsWindowFunctionNtile() {
        return false;
    }

    @Override
    public boolean supportsWindowFunctionPercentRank() {
        return false;
    }

    @Override
    public boolean supportsWindowFunctionCumeDist() {
        return false;
    }

    @Override
    public boolean supportsWindowFunctionNthValue() {
        return false;
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
        registerFunctionModifier(SourceSystemFunctions.TRIM, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                List<Expression> p = function.getParameters();
                return Arrays.asList("STRIP(", p.get(2), ", ", ((Literal)p.get(0)).getValue(), ", ", p.get(1), ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            }
        });
        registerFunctionModifier(SourceSystemFunctions.WEEK, new AliasModifier(WEEK_ISO));
        addPushDownFunction("db2", "substr", "string", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.INTEGER, TypeFacility.RUNTIME_NAMES.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @TranslatorProperty(display="Is DB2 for i", description="If the server is DB2 for i (formally known as DB2/AS).",advanced=true)
    public boolean isdB2ForI() {
        return dB2ForI;
    }

    public void setdB2ForI(boolean dB2ForI) {
        this.dB2ForI = dB2ForI;
    }

    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }

    @Override
    public String getHibernateDialectClassName() {
        return "org.hibernate.dialect.DB2Dialect"; //$NON-NLS-1$
    }

    @Override
    public String getTemporaryTableName(String prefix) {
        return "session." + super.getTemporaryTableName(prefix); //$NON-NLS-1$
    }

    @Override
    public boolean supportsGroupByRollup() {
        return true;
    }

    @Override
    protected boolean supportsBooleanExpressions() {
        return false;
    }

    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
        return new JDBCMetadataProcessor() {

            @Override
            protected ResultSet executeSequenceQuery(Connection conn)
                    throws SQLException {
                String query = "select null as sequence_catalog, seqschema as sequence_schema, seqname as sequence_name from sysibm.syssequences " //$NON-NLS-1$
                        + "where seqschema like ? and seqname like ?"; //$NON-NLS-1$
                PreparedStatement ps = conn.prepareStatement(query);
                ps.setString(1, getSchemaPattern()==null?"%":getSchemaPattern()); //$NON-NLS-1$
                ps.setString(2, getSequenceNamePattern()==null?"%":getSequenceNamePattern()); //$NON-NLS-1$
                return ps.executeQuery();
            }

        };
    }

    @Override
    public boolean supportsRecursiveCommonTableExpressions() {
        return getVersion().compareTo(TEN_0) >= 0;
    }

    @Override
    public boolean preserveNullTyping() {
        return true;
    }

}
