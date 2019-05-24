package org.teiid.resource.adapter.google.integration;
import java.util.List;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.resource.adapter.google.auth.OAuth2HeaderFactory;
import org.teiid.resource.adapter.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.resource.adapter.google.gdata.GDataClientLoginAPI;
import org.teiid.translator.google.api.result.PartialResultExecutor;
import org.teiid.translator.google.api.result.SheetRow;


/**
 *
 * This test tests data retrieval by Google Visualization protocol and GData protocol.
 * GData protocol has fairly simple API so querying through that shouldn't be hard to test/verify.
 *
 * On the other hand Google Visualization Data Protocol has complex query language (https://developers.google.com/chart/interactive/docs/querylanguage)
 * we verify that most constructs work.
 *
 * It is necessary to supply AUTH key for GoogleDataProtocol and Spreadsheet key (supplying this information reduces tested logic).
 *
 * This test expects following two column table in sheet "Sheet2" to be present:
 *
    A   B
      1    a
    2    a
    3    a
    4    a
    5    a
    6    a
    7    a
    8    a
    9    b
    10    b
    11    b
    12    b
 *
 * @author fnguyen
 *
 */
@Ignore
@SuppressWarnings("nls")
public class SpreadsheetDataRetrievalTest extends IntegrationTest {
    private String SPREADSHEET_KEY = "0Ajbs6-5EEwQqdFBSelpZT1FuZ2EwSFZaTTJVbGZVeGc";

    private static GoogleDataProtocolAPI dataProtocol = null;
    private static GDataClientLoginAPI gdata = null;

    @BeforeClass
    public static void prepareGoogleData(){
        dataProtocol = new GoogleDataProtocolAPI();
        OAuth2HeaderFactory headerFactory = new OAuth2HeaderFactory(refreshToken);
        //headerFactory.login();
        dataProtocol.setHeaderFactory(headerFactory);

        gdata = new GDataClientLoginAPI();
        gdata.setHeaderFactory(headerFactory);
    }


    @Test
    public void vSimple(){
        PartialResultExecutor dpqs = dataProtocol.new DataProtocolQueryStrategy(SPREADSHEET_KEY,"s1","");
        assertSimpleResultDataProtorol(dpqs);
    }


    private void assertSimpleResultDataProtorol(PartialResultExecutor partialExecutor) {
        List<SheetRow> result = partialExecutor.getResultsBatch(0, 4);
        Assert.assertEquals(new SheetRow(new String[]{"0","Michal","Abaffy", "$26,000", "Brno","01-17-1987"}),result.get(0));
        Assert.assertEquals(new SheetRow(new String[]{"2","Filip","Eliáš", "$50,000", "Brno", "02-18-1974"}),result.get(1));
        Assert.assertEquals(4, result.size());
        result = partialExecutor.getResultsBatch(1, 13);
        Assert.assertEquals(11, result.size());
        result = partialExecutor.getResultsBatch(10, 3);
        Assert.assertEquals(new SheetRow(new String[]{"11","Pavel","Macik", "$28,000", "Bratislava", "04-08-1954"}),result.get(1));
        Assert.assertEquals(2, result.size());
        result = partialExecutor.getResultsBatch(13, 2);
        Assert.assertEquals(0, result.size());
    }

    private List<SheetRow> query(String string, String string2, int i, int j) {
        PartialResultExecutor dpqs = dataProtocol.new DataProtocolQueryStrategy(SPREADSHEET_KEY,string,string2);
        return dpqs.getResultsBatch(i, j);
    }


    @Test
    public void vSelect(){
        List<SheetRow> result = query("s1","SELECT A",0,12);
        Assert.assertEquals(12, result.size());
        //only 1 column retrieved
        Assert.assertEquals(1, result.get(0).getRow().size());
        //assert some data
        Assert.assertEquals("0", result.get(0).getRow().get(0));
        Assert.assertEquals("2", result.get(1).getRow().get(0));
        Assert.assertEquals("3", result.get(2).getRow().get(0));
    }



    @Test
    public void vWhere(){
        List<SheetRow> result = query("s1","WHERE B='Filip'",0,12);
        Assert.assertEquals(2, result.size());
        result = query("s1","WHERE B='Filip' and C='Eliáš'",0,12);
        Assert.assertEquals(1, result.size());
        result = query("s1","WHERE C starts with 'E'",0,12);
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void vGroupBy(){
        List<SheetRow> result = query("s1","SELECT E,max(D) GROUP BY E",0,12);
        Assert.assertEquals(3, result.size());
        //TODO commas!? WTF!
        Assert.assertEquals(new SheetRow(new String[]{"Brno","50,000"}),result.get(1));
        Assert.assertEquals(new SheetRow(new String[]{"Praha","66,000"	}),result.get(2));
        Assert.assertEquals(new SheetRow(new String[]{"Bratislava","60,000"}),result.get(0));
    }

    /**
     * ICU formatting rules are obeyed http://userguide.icu-project.org/formatparse/numbers
     */
    @Test
    public void vFormat(){
        List<SheetRow> result = query("s1","SELECT D where A = 2 format D '#,##0.00'",0,12);
        Assert.assertEquals(1, result.size());
    }

//    @Test
//    public void vLimit(){
//
//    }
//    @Test
//    public void vOffset(){
//
//    }
//    @Test
//    public void vLabel(){
//
//    }

//    @Test
//    public void vOptions(){
//
//    }
//    @Test
//    public void vAvg(){
//
//    }
//    @Test
//    public void vCount(){
//
//    }
//    @Test
//    public void vMax(){
//
//    }
//    @Test
//    public void vMin(){
//
//    }
//    @Test
//    public void vScalars(){
//
//    }
//
//    @Test
//    public void vArithmetic(){
//
//    }
}
