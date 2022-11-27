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

package org.teiid.translator.jdbc.derby;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.DerivedColumn;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.EscapeSyntaxModifier;
import org.teiid.translator.jdbc.db2.BaseDB2ExecutionFactory;
import org.teiid.translator.jdbc.oracle.LeftOrRightFunctionModifier;
import org.teiid.util.Version;

/**
 * @since 4.3
 */
@Translator(name="derby", description="A translator for Apache Derby Database")
public class DerbyExecutionFactory extends BaseDB2ExecutionFactory {

    public static final Version TEN_1 = Version.getVersion("10.1"); //$NON-NLS-1$
    public static final Version TEN_2 = Version.getVersion("10.2"); //$NON-NLS-1$
    public static final Version TEN_3 = Version.getVersion("10.3"); //$NON-NLS-1$
    public static final Version TEN_4 = Version.getVersion("10.4"); //$NON-NLS-1$
    public static final Version TEN_5 = Version.getVersion("10.5"); //$NON-NLS-1$
    public static final Version TEN_6 = Version.getVersion("10.6"); //$NON-NLS-1$
    public static final Version TEN_7 = Version.getVersion("10.7"); //$NON-NLS-1$

    public DerbyExecutionFactory() {
        setSupportsFullOuterJoins(false); //Derby supports only left and right outer joins.
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
        //additional derby functions
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));

        //overrides of db2 functions
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new EscapeSyntaxModifier());
    }

    @Override
    public boolean addSourceComment() {
        return false;
    }

    @Override
    public boolean supportsOrderByNullOrdering() {
        return getVersion().compareTo(TEN_4) >= 0;
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getDefaultSupportedFunctions());

        supportedFunctions.add("ABS"); //$NON-NLS-1$
        if (getVersion().compareTo(TEN_2) >= 0) {
            supportedFunctions.add("ACOS"); //$NON-NLS-1$
            supportedFunctions.add("ASIN"); //$NON-NLS-1$
            supportedFunctions.add("ATAN"); //$NON-NLS-1$
        }
        if (getVersion().compareTo(TEN_4) >= 0) {
            supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        }
        // These are executed within the server and never pushed down
        //supportedFunctions.add("BITAND"); //$NON-NLS-1$
        //supportedFunctions.add("BITNOT"); //$NON-NLS-1$
        //supportedFunctions.add("BITOR"); //$NON-NLS-1$
        //supportedFunctions.add("BITXOR"); //$NON-NLS-1$
        if (getVersion().compareTo(TEN_2) >= 0) {
            supportedFunctions.add("CEILING"); //$NON-NLS-1$
            supportedFunctions.add("COS"); //$NON-NLS-1$
            supportedFunctions.add("COT"); //$NON-NLS-1$
            supportedFunctions.add("DEGREES"); //$NON-NLS-1$
            supportedFunctions.add("EXP"); //$NON-NLS-1$
            supportedFunctions.add("FLOOR"); //$NON-NLS-1$
            supportedFunctions.add("LOG"); //$NON-NLS-1$
            supportedFunctions.add("LOG10"); //$NON-NLS-1$
        }
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        if (getVersion().compareTo(TEN_2) >= 0) {
            supportedFunctions.add("PI"); //$NON-NLS-1$
            //supportedFunctions.add("POWER"); //$NON-NLS-1$
            supportedFunctions.add("RADIANS"); //$NON-NLS-1$
            //supportedFunctions.add("ROUND"); //$NON-NLS-1$
            if (getVersion().compareTo(TEN_4) >= 0) {
                supportedFunctions.add("SIGN"); //$NON-NLS-1$
            }
            supportedFunctions.add("SIN"); //$NON-NLS-1$
        }
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        //supportedFunctions.add("TAN"); //$NON-NLS-1$

        //supportedFunctions.add("ASCII"); //$NON-NLS-1$
        //supportedFunctions.add("CHR"); //$NON-NLS-1$
        //supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        //supportedFunctions.add("INSERT"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        //supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        //supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        //supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        //supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        //supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        if (getVersion().compareTo(TEN_3) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.TRIM);
        }
        supportedFunctions.add("UCASE"); //$NON-NLS-1$

        // These are executed within the server and never pushed down
        //supportedFunctions.add("CURDATE"); //$NON-NLS-1$
        //supportedFunctions.add("CURTIME"); //$NON-NLS-1$
        //supportedFunctions.add("NOW"); //$NON-NLS-1$
        //supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        //supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        //supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$

        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("FORMATDATE"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIME"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        //supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$

        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("PARSEDATE"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIME"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIMESTAMP"); //$NON-NLS-1$
        //supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        //supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$

        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("COALESCE"); //$NON-NLS-1$
        return supportedFunctions;
    }

    @Override
    public boolean supportsRowLimit() {
        return this.getVersion().compareTo(TEN_5) >= 0;
    }

    @Override
    public boolean supportsRowOffset() {
        return this.getVersion().compareTo(TEN_5) >= 0;
    }

    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }

    @Override
    public String getHibernateDialectClassName() {
        if (this.getVersion().compareTo(TEN_6) >= 0) {
            if (this.getVersion().compareTo(TEN_7) >= 0) {
                return "org.hibernate.dialect.DerbyTenSevenDialect"; //$NON-NLS-1$
            }
            return "org.hibernate.dialect.DerbyTenSixDialect"; //$NON-NLS-1$
        }
        return "org.hibernate.dialect.DerbyTenFiveDialect"; //$NON-NLS-1$
    }

    @Override
    public boolean supportsGroupByRollup() {
        return this.getVersion().compareTo(TEN_6) >= 0;
    }

    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof DerivedColumn) {
            DerivedColumn selectSymbol = (DerivedColumn)obj;

            if (selectSymbol.getExpression().getType() == TypeFacility.RUNTIME_TYPES.XML) {
                if (selectSymbol.getAlias() == null) {
                    return Arrays.asList("XMLSERIALIZE(", selectSymbol.getExpression(), " AS CLOB)"); //$NON-NLS-1$//$NON-NLS-2$
                }
                //we're assuming that alias quoting shouldn't be needed
                return Arrays.asList("XMLSERIALIZE(", selectSymbol.getExpression(), " AS CLOB) AS ", selectSymbol.getAlias());  //$NON-NLS-1$//$NON-NLS-2$
            }
        }
        return super.translate(obj, context);
    }

    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
        if (limit.getRowOffset() > 0) {
            return Arrays.asList("OFFSET ", limit.getRowOffset(), " ROWS FETCH FIRST ", limit.getRowLimit(), " ROWS ONLY"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        return super.translateLimit(limit, context);
    }

}
