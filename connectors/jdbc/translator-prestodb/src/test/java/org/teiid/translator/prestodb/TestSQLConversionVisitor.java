package org.teiid.translator.prestodb;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.TimeZone;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.CoreConstants;
import org.teiid.core.util.TimestampWithTimezone;
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

@SuppressWarnings("nls")
public class TestSQLConversionVisitor {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    private static PrestoDBExecutionFactory TRANSLATOR;

    private static TranslationUtility translationUtility = new TranslationUtility(queryMetadataInterface());

    @BeforeClass
    public static void init() throws TranslatorException {
        init("0.92");
    }

    public static void init(String version) throws TranslatorException {
        TRANSLATOR = new PrestoDBExecutionFactory();
        TRANSLATOR.setDatabaseVersion(version);
        TRANSLATOR.start();
        translationUtility.addUDF(CoreConstants.SYSTEM_MODEL, TRANSLATOR.getPushDownFunctions());
    }

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

        SQLConversionVisitor vistor = TRANSLATOR.getSQLConversionVisitor();
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
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("UTC")); //$NON-NLS-1$
        try {
            String sql = "SELECT intkey FROM prestodbModel.smalla WHERE datevalue = cast('2017-01-13' as date)"; //$NON-NLS-1$
            String expected = "SELECT smalla.intKey FROM smalla WHERE smalla.dateValue = date '2017-01-13'"; //$NON-NLS-1$
            helpTest(sql, expected);

            sql = "SELECT intkey FROM prestodbModel.smalla WHERE smalla.timeValue = cast('15:50:02' as time)"; //$NON-NLS-1$
            expected = "SELECT smalla.intKey FROM smalla WHERE smalla.timeValue = time '15:50:02'"; //$NON-NLS-1$
            helpTest(sql, expected);

            sql = "SELECT intkey FROM prestodbModel.smalla WHERE smalla.timestampValue = cast('2017-01-13 15:50:02.0' as timestamp)"; //$NON-NLS-1$
            expected = "SELECT smalla.intKey FROM smalla WHERE smalla.timestampValue = timestamp '2017-01-13 15:50:02.0'"; //$NON-NLS-1$
            helpTest(sql, expected);
        } finally {
            TimestampWithTimezone.resetCalendar(null);
        }
    }

    @Test
    public void testFormatDateTime() throws TranslatorException {

        String sql = "SELECT FORMATDATE(datevalue, 'MM-dd-yy') FROM prestodbModel.smalla"; //$NON-NLS-1$
        String expected = "SELECT format_datetime(cast(smalla.dateValue AS timestamp), 'MM-dd-yy') FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);

        sql = "SELECT FORMATTIME(timeValue, 'HH:MI:SS') FROM prestodbModel.smalla"; //$NON-NLS-1$
        expected = "SELECT format_datetime(cast(smalla.timeValue AS timestamp), 'HH:MI:SS') FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);

        sql = "SELECT FORMATTIMESTAMP(timestampValue, 'YYYY-MM-DD HH:MI:SS') FROM prestodbModel.smalla"; //$NON-NLS-1$
        expected = "SELECT format_datetime(smalla.timestampValue, 'YYYY-MM-DD HH:MI:SS') FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);
    }

    @Test
    public void testConvertCast() throws TranslatorException {
        String sql = "SELECT convert(dateValue, timestamp) FROM prestodbModel.smalla"; //$NON-NLS-1$
        String expected = "SELECT cast(smalla.dateValue AS timestamp) FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);

        sql = "SELECT convert(timeValue, timestamp) FROM prestodbModel.smalla"; //$NON-NLS-1$
        expected = "SELECT cast(smalla.timeValue AS timestamp) FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);
    }

    @Test
    public void testConvertCastExpandedTypes() throws TranslatorException {
        init("0.190");
        String sql = "SELECT convert(stringnum, integer), convert(stringnum, byte) FROM prestodbModel.smalla"; //$NON-NLS-1$
        String expected = "SELECT cast(smalla.stringnum AS integer), cast(smalla.stringnum AS tinyint) FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);

        sql = "SELECT booleanValue, (booleanValue + 1) AS BooleanValuePlus2 FROM prestodbModel.SmallA"; //$NON-NLS-1$
        expected = "SELECT smalla.booleanValue, (cast(smalla.booleanValue AS integer) + 1) AS BooleanValuePlus2 FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);

        sql = "SELECT convert(stringnum, float), convert(stringnum, short) FROM prestodbModel.smalla"; //$NON-NLS-1$
        expected = "SELECT cast(smalla.stringnum AS real), cast(smalla.stringnum AS smallint) FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);
    }

    @Test
    public void testLogarithmFunctions() throws TranslatorException {

        // natural logarithm
        String sql = "SELECT log(doublenum) FROM prestodbModel.smalla"; //$NON-NLS-1$
        String expected = "SELECT ln(smalla.doublenum) FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);

        // base 10 logarithm
        sql = "SELECT log10(doublenum) FROM prestodbModel.smalla"; //$NON-NLS-1$
        expected = "SELECT log10(smalla.doublenum) FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);

        // base 2 logarithm
        sql = "SELECT prestodb.log2(doublenum) FROM prestodbModel.smalla"; //$NON-NLS-1$
        expected = "SELECT log2(smalla.doublenum) FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);

        // base b logarithm
        sql = "SELECT prestodb.log(doublenum, 2) FROM prestodbModel.smalla"; //$NON-NLS-1$
        expected = "SELECT log(smalla.doublenum, 2) FROM smalla"; //$NON-NLS-1$
        helpTest(sql, expected);
    }

    @Test
    public void testCorelatedSubquery() throws TranslatorException {
        String sql = "SELECT intkey, bytenum, (SELECT bytenum FROM prestodbModel.smalla AS b WHERE (bytenum = a.longnum) AND (intkey = '10')) AS longnum FROM prestodbModel.smalla AS a"; //$NON-NLS-1$
        String expected = "SELECT a.intKey, a.bytenum, (SELECT b.bytenum FROM smalla AS b WHERE cast(b.bytenum AS bigint) = a.longnum AND b.intKey = '10' LIMIT 2) AS longnum FROM smalla AS a"; //$NON-NLS-1$
        helpTest(sql, expected);
    }


}
