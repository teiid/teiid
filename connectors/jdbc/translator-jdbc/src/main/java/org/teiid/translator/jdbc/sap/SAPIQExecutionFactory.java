package org.teiid.translator.jdbc.sap;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.EscapeSyntaxModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.TemplateFunctionModifier;
import org.teiid.translator.jdbc.hsql.AddDiffModifier;
import org.teiid.translator.jdbc.oracle.ConcatFunctionModifier;
import org.teiid.translator.jdbc.sybase.BaseSybaseExecutionFactory;
import org.teiid.util.Version;

/**
 * A translator for Sybase/SAP IQ 15.1+
 */
@Translator(name="sap-iq", description="A translator for hte Sybase/SAP IQ and Database")
public class SAPIQExecutionFactory extends BaseSybaseExecutionFactory {

    public static final Version FIFTEEN_4 = Version.getVersion("15.4"); //$NON-NLS-1$

    protected Map<String, Integer> formatMap = new HashMap<String, Integer>();

    private Boolean jConnectDriver;

    public SAPIQExecutionFactory() {
        setSupportsFullOuterJoins(false);
        setMaxInCriteriaSize(250);
        setMaxDependentInPredicates(7);
    }

    public void start() throws TranslatorException {
        super.start();

        registerFunctionModifier(SourceSystemFunctions.CONCAT, new ConcatFunctionModifier(getLanguageFactory()) {
            @Override
            public List<?> translate(Function function) {
                function.setName("||"); //$NON-NLS-1$
                return super.translate(function);
            }
        });
        registerFunctionModifier(SourceSystemFunctions.CONCAT2, new AliasModifier("STRING")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new TemplateFunctionModifier("DATEPART(dy,",0, ")")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.WEEK, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new AddDiffModifier(true, this.getLanguageFactory()).supportsQuarter(true).literalPart(false));
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new AddDiffModifier(false, this.getLanguageFactory()).supportsQuarter(true).literalPart(false));
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier(SourceSystemFunctions.COALESCE));
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                List<Expression> params = function.getParameters();
                Expression param1 = params.get(0);
                Expression param2 = params.set(1, param1);
                params.set(0, param2);
                return null;
            }
        });

        //add in type conversion
        ConvertModifier convertModifier = new ConvertModifier();
        convertModifier.setBooleanNullable(booleanNullable());
        convertModifier.addNumericBooleanConversions();
        //boolean isn't treated as bit, since it doesn't support null
        //byte is treated as smallint, since tinyint is unsigned
        convertModifier.addTypeMapping("smallint", FunctionModifier.BYTE, FunctionModifier.SHORT); //$NON-NLS-1$
        convertModifier.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
        convertModifier.addTypeMapping("int", FunctionModifier.INTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("double", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convertModifier.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
        convertModifier.addTypeMapping("numeric(38, 0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("numeric(38, 19)", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
        convertModifier.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
        convertModifier.addTypeMapping("varchar(4000)", FunctionModifier.STRING); //$NON-NLS-1$
        convertModifier.addTypeMapping("varbinary", FunctionModifier.VARBINARY); //$NON-NLS-1$
        convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
        convertModifier.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
        convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
        convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return convertTimeToString(function);
            }
        });
        convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return convertDateToString(function);
            }
        });
        convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new FunctionModifier() {
            @Override
            public List<?> translate(Function function) {
                return convertTimestampToString(function);
            }
        });
        registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
    }

    private List<Object> convertTimeToString(Function function) {
        return Arrays.asList("convert(varchar, ", function.getParameters().get(0), ", 8)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    protected List<Object> convertDateToString(Function function) {
        return Arrays.asList("stuff(stuff(convert(varchar, ", function.getParameters().get(0), ", 102), 5, 1, '-'), 8, 1, '-')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    protected List<?> convertTimestampToString(Function function) {
        return Arrays.asList("convert(varchar, ", function.getParameters().get(0), ", 121)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
        supportedFunctions.add("ABS"); //$NON-NLS-1$
        supportedFunctions.add("ACOS"); //$NON-NLS-1$
        supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("ASIN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add("COT"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.CONCAT2);
        supportedFunctions.add("DEGREES"); //$NON-NLS-1$
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("PI"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$

        supportedFunctions.add(SourceSystemFunctions.REPEAT);
        //supportedFunctions.add("RAND"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SPACE"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        //supportedFunctons.add("CURDATE"); //$NON-NLS-1$
        //supportedFunctons.add("CURTIME"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        //supportedFunctions.add("NOW"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);
        //supportedFunctions.add("FORMATTIMESTAMP");   //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        return supportedFunctions;
    }

    @Override
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    public boolean supportsFunctionsInGroupBy() {
        return true;
    }

    @Override
    public int getMaxFromGroups() {
        return 50;
    }

    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
        return true;
    }

    public boolean booleanNullable() {
        return false;
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "CAST('" + formatDateValue(timeValue) +"' AS TIME)"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "CAST('" + formatDateValue(timestampValue) +"' AS TIMESTAMP)"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public String translateLiteralDate(Date dateValue) {
        return "CAST('" + formatDateValue(dateValue) +"' AS DATE)"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public boolean supportsRowLimit() {
        return getVersion().compareTo(FIFTEEN_4) >= 0;
    }

    @Override
    protected boolean usesDatabaseVersion() {
        return true;
    }

    @Override
    public boolean supportsSelectWithoutFrom() {
        return true;
    }

    @Override
    public String getHibernateDialectClassName() {
        return "org.hibernate.dialect.SybaseAnywhereDialect"; //$NON-NLS-1$
    }

    @Override
    public boolean supportsGroupByRollup() {
        return true;
    }

    @Override
    public boolean useUnicodePrefix() {
        return true;
    }

    @Override
    public boolean hasTimeType() {
        return true;
    }

    @Override
    public boolean useAsInGroupAlias() {
        return true;
    }

    @Override
    public void initCapabilities(Connection connection)
            throws TranslatorException {
        try {
            this.jConnectDriver = connection.getMetaData().getDriverName().contains("jConnect"); //$NON-NLS-1$
            if (this.jConnectDriver) {
                //jConnect does not use the correct bind logic
                setUseBindVariables(false);
                setUseBindingsForDependentJoin(false);
            }
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
        super.initCapabilities(connection);
    }

    public boolean isSourceRequiredForCapabilities() {
        return super.isSourceRequiredForCapabilities() || jConnectDriver == null;
    }

    @Override
    public boolean useParensForJoins() {
        return true;
    }

}
