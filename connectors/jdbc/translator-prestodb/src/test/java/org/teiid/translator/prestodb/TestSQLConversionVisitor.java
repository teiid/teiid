package org.teiid.translator.prestodb;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.util.Arrays;

import org.junit.Test;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class TestSQLConversionVisitor {
    
    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();
    
    private void helpTestMod(Literal c, String format, String expectedStr) throws Exception {
        Function func = LANG_FACTORY.createFunction(format,  
                Arrays.asList(c),
                String.class);
        
        PrestoDBExecutionFactory ef= new PrestoDBExecutionFactory();
        ef.start();
        
        SQLConversionVisitor sqlVisitor = ef.getSQLConversionVisitor(); 
        sqlVisitor.append(func);  
        
//        System.out.println(sqlVisitor.toString());
        assertEquals(expectedStr, sqlVisitor.toString());
    }

    @Test
    public void testDayOfMonth() throws Exception {       
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(117, 0, 13, 10, 5, 0, 10000000), Timestamp.class);
        helpTestMod(arg1, SourceSystemFunctions.DAYOFMONTH, "day_of_month({ts '2017-01-13 10:05:00.01'})"); //$NON-NLS-1$      
    }
    
    @Test
    public void testDayOfWeek() throws Exception {       
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(117, 0, 13, 10, 5, 0, 10000000), Timestamp.class);
        helpTestMod(arg1, SourceSystemFunctions.DAYOFWEEK, "day_of_week({ts '2017-01-13 10:05:00.01'})"); //$NON-NLS-1$      
    }
    
    @Test
    public void testDayOfYear() throws Exception {       
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(117, 0, 13, 10, 5, 0, 10000000), Timestamp.class);
        helpTestMod(arg1, SourceSystemFunctions.DAYOFYEAR, "day_of_year({ts '2017-01-13 10:05:00.01'})"); //$NON-NLS-1$      
    }
}
