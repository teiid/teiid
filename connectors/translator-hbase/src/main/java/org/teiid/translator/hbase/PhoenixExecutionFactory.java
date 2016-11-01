package org.teiid.translator.hbase;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.DATE;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.INTEGER;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.OBJECT;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.TIME;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.TIMESTAMP;

import java.util.ArrayList;
import java.util.List;

import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.util.Version;

@Translator(name="phoenix", description="A translator for Phoenix/HBase")
public class PhoenixExecutionFactory extends HBaseExecutionFactory{
    
    public static String HBASE = "phoenix"; //$NON-NLS-1$
    public static final Version V_4_8 = Version.getVersion("4.8"); //$NON-NLS-1$
    
    @Override
    public void start() throws TranslatorException {
        super.start();
        
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("SUBSTR")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("UPPER")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("LOWER")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new AliasModifier("INSTR")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, new AliasModifier("TO_TIMESTAMP")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("CURRENT_TIME")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("LN")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG10, new AliasModifier("LOG")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.PARSEBIGDECIMAL, new AliasModifier("TO_NUMBER")); //$NON-NLS-1$
        
        addPushDownFunction(HBASE, "REVERSE", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HBASE, "REGEXP_SUBSTR", STRING, STRING, STRING, INTEGER); //$NON-NLS-1$
        addPushDownFunction(HBASE, "REGEXP_REPLACE", STRING, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HBASE, "REGEXP_SPLIT", OBJECT, STRING, STRING); //$NON-NLS-1$ 
        addPushDownFunction(HBASE, "TO_DATE", DATE, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HBASE, "TO_TIME", TIME, STRING, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HBASE, "TIMEZONE_OFFSET", INTEGER, STRING, DATE); //$NON-NLS-1$
        addPushDownFunction(HBASE, "TIMEZONE_OFFSET", INTEGER, STRING, TIME); //$NON-NLS-1$
        addPushDownFunction(HBASE, "TIMEZONE_OFFSET", INTEGER, STRING, TIMESTAMP); //$NON-NLS-1$
        addPushDownFunction(HBASE, "CONVERT_TZ", DATE, DATE, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(HBASE, "CONVERT_TZ", TIME, TIME, STRING, STRING); //$NON-NLS-1$
    }

    @Override
    public List<String> getSupportedFunctions() {
        
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
        
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);        
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        
        supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.NOW);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.WEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        
        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.LOG); 
        supportedFunctions.add(SourceSystemFunctions.LOG10); 
        supportedFunctions.add(SourceSystemFunctions.RAND);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.PARSEBIGDECIMAL);
        
        return supportedFunctions;
    }
    
    @Override
    public boolean supportsRowOffset() {
        return getVersion().compareTo(V_4_8) >= 0;
    }

}
