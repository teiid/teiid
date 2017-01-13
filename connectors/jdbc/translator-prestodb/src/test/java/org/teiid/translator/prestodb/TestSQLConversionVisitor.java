package org.teiid.translator.prestodb;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.Arrays;

import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class TestSQLConversionVisitor {
    
    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();
    
    private static TranslationUtility translationUtility = new TranslationUtility(queryMetadataInterface());
    
    private static TransformationMetadata queryMetadataInterface() {
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("prestodbModel");
            
            MetadataFactory mf = new MetadataFactory("prestodb", 1, SystemMetadata.getInstance().getRuntimeTypeMap(), mmd);
            mf.setParser(new QueryParser());
            mf.parse(new FileReader(UnitTestUtil.getTestDataFile("sample.ddl")));

            TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x");
            ValidatorReport report = new MetadataValidator().validate(tm.getVdbMetaData(), tm.getMetadataStore());
            if (report.hasItems()) {
                throw new RuntimeException(report.getFailureMessage());
            }
            return tm;
        } catch (MetadataException | FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void helpTest(String sql, String expected) throws TranslatorException {
        Command command = translationUtility.parseCommand(sql);
        
        PrestoDBExecutionFactory ef= new PrestoDBExecutionFactory();
        ef.start();
        
        SQLConversionVisitor vistor = ef.getSQLConversionVisitor();
        vistor.append(command);

//        System.out.println(vistor.toString());
        assertEquals(expected, vistor.toString());
    }
    
    private void helpTestMod(Literal c, String format, String expectedStr) throws Exception {
        Function func = LANG_FACTORY.createFunction(format,  
                Arrays.asList(c),
                String.class);
        
        PrestoDBExecutionFactory ef= new PrestoDBExecutionFactory();
        ef.start();
        
        SQLConversionVisitor sqlVisitor = ef.getSQLConversionVisitor(); 
        sqlVisitor.append(func);  
        
        assertEquals(expectedStr, sqlVisitor.toString());
    }

    @Test
    public void testDayOfMonth() throws Exception {       
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(117, 0, 13, 10, 5, 0, 10000000), Timestamp.class);
        helpTestMod(arg1, SourceSystemFunctions.DAYOFMONTH, "day_of_month(timestamp '2017-01-13 10:05:00.01')"); //$NON-NLS-1$      
    }
    
    @Test
    public void testDayOfWeek() throws Exception {       
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(117, 0, 13, 10, 5, 0, 10000000), Timestamp.class);
        helpTestMod(arg1, SourceSystemFunctions.DAYOFWEEK, "day_of_week(timestamp '2017-01-13 10:05:00.01')"); //$NON-NLS-1$      
    }
    
    @Test
    public void testDayOfYear() throws Exception {       
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(117, 0, 13, 10, 5, 0, 10000000), Timestamp.class);
        helpTestMod(arg1, SourceSystemFunctions.DAYOFYEAR, "day_of_year(timestamp '2017-01-13 10:05:00.01')"); //$NON-NLS-1$      
    }
    
    @Test
    public void testDateTimeLiterals() throws Exception {
        
        String sql = "SELECT intkey FROM prestodbModel.smalla WHERE datevalue = cast('2017-01-13' as date)";
        String expected = "SELECT smalla.intKey FROM smalla WHERE smalla.dateValue = date '2017-01-13 00:00:00.0'";
        helpTest(sql, expected);
        
        sql = "SELECT intkey FROM prestodbModel.smalla WHERE smalla.timeValue = cast('15:50:02' as time)";
        expected = "SELECT smalla.intKey FROM smalla WHERE smalla.timeValue = time '1970-01-01 15:50:02.0'";
        helpTest(sql, expected);
        
        sql = "SELECT intkey FROM prestodbModel.smalla WHERE smalla.timestampValue = cast('2017-01-13 15:50:02.0' as timestamp)";
        expected = "SELECT smalla.intKey FROM smalla WHERE smalla.timestampValue = timestamp '2017-01-13 15:50:02.0'";
        helpTest(sql, expected);
    }
    
    @Test
    public void testFormatDateTime() throws TranslatorException {
        
        String sql = "SELECT FORMATDATE(datevalue, 'MM-dd-yy') FROM prestodbModel.smalla";
        String expected = "SELECT format_datetime(cast(smalla.dateValue AS timestamp with timezone), 'MM-dd-yy') FROM smalla";
        helpTest(sql, expected);
        
        sql = "SELECT FORMATTIME(timeValue, 'HH:MI:SS') FROM prestodbModel.smalla";
        expected = "SELECT format_datetime(cast(smalla.timeValue AS timestamp with timezone), 'HH:MI:SS') FROM smalla";
        helpTest(sql, expected);
        
        sql = "SELECT FORMATTIMESTAMP(timestampValue, 'YYYY-MM-DD HH:MI:SS') FROM prestodbModel.smalla";
        expected = "SELECT format_datetime(smalla.timestampValue, 'YYYY-MM-DD HH:MI:SS') FROM smalla";
        helpTest(sql, expected);
        
    }
    
    
}
