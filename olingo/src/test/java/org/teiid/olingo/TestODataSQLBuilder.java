/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.olingo;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.TimeZone;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.core.Encoder;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.edmx.EdmxReference;
import org.apache.olingo.server.core.OData4Impl;
import org.apache.olingo.server.core.SchemaBasedEdmProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.metadata.MetadataStore;
import org.teiid.odata.api.Client;
import org.teiid.odata.api.CountResponse;
import org.teiid.odata.api.OperationResponse;
import org.teiid.odata.api.SQLParameter;
import org.teiid.olingo.service.EntityCollectionResponse;
import org.teiid.olingo.service.ODataSchemaBuilder;
import org.teiid.olingo.service.TeiidServiceHandler;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.RealMetadataFactory.DDLHolder;

@SuppressWarnings("nls")
public class TestODataSQLBuilder {
    static String DEFAULT_DDL =
            "CREATE FOREIGN TABLE G1 (\n" +
            "    e1 string, \n" +
            "    e2 integer PRIMARY KEY, \n" +
            "    e3 double\n" +
            ");\n" +
            "\n" +
            "CREATE FOREIGN TABLE G2 (\n" +
            "    e1 string, \n" +
            "    e2 integer PRIMARY KEY, \n" +
            "    CONSTRAINT FK0 FOREIGN KEY (e2) REFERENCES G1 (e2)\n" +
            ") OPTIONS (UPDATABLE 'true');\n" +
            "\n"+
            "CREATE FOREIGN TABLE G3 (\n" +
            "    e1 string, \n" +
            "    e2 integer,\n" +
            "   e3 string[],\n"+
            "    CONSTRAINT PK PRIMARY KEY (e1,e2)\n" +
            ") OPTIONS (UPDATABLE 'true')" +
            "\n" +
            "CREATE FOREIGN TABLE G4 (\n" +
            "    e1 string PRIMARY KEY, \n" +
            "    e2 integer,\n" +
            "    CONSTRAINT FKX FOREIGN KEY (e2) REFERENCES G1(e2)\n" +
            ") OPTIONS (UPDATABLE 'true');" +
            "\n"+
            "CREATE FOREIGN TABLE G5 (\n" +
            "    e1 string OPTIONS (SELECTABLE 'FALSE'), \n" +
            "    e2 integer PRIMARY KEY, \n" +
            "    e3 double\n" +
            ");\n" +            
            "CREATE FOREIGN PROCEDURE getCustomers("
            + "IN p2 timestamp, "
            + "IN p3 decimal"
            + ") RETURNS integer;";    
    
    static class QueryState extends BaseState {
        List<SQLParameter> parameters;
        ArgumentCaptor<Query> arg1;
        ArgumentCaptor<EntityCollectionResponse> arg6;        
    }
    
    static class UpdateState extends BaseState {
        ArgumentCaptor<Command> commandArg;
    }
    
    static class BaseState {
        Client client;
        String response;
		Integer status;
    }
    
    static class ProcedureState extends BaseState {
        String arg1;
        List arg2;
        Integer arg3;
        OperationResponse arg4;        
    }    
    
    private QueryState setup(String url) throws Exception {    
        return (QueryState)setup(DEFAULT_DDL, url, "GET", null, new QueryState());
    }
    
    private BaseState setup(String ddl, String url, String method, ServletInputStream stream, BaseState state) throws Exception {
        Client client = Mockito.mock(Client.class);

        DDLHolder model = new DDLHolder("PM1", ddl);
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("vdb", model);
        MetadataStore store = metadata.getMetadataStore();
        //TranslationUtility utility = new TranslationUtility(metadata);

        OData odata = OData4Impl.newInstance();
        org.teiid.metadata.Schema teiidSchema = store.getSchema("PM1");
        CsdlSchema schema = ODataSchemaBuilder.buildMetadata("vdb", teiidSchema);
        SchemaBasedEdmProvider edmProvider = new SchemaBasedEdmProvider();
        edmProvider.addSchema(schema);
        
        ServiceMetadata serviceMetadata = odata.createServiceMetadata(edmProvider, Collections.<EdmxReference> emptyList());
        ODataHttpHandler handler = odata.createHandler(serviceMetadata);

        Hashtable<String, String> headers = new Hashtable<String, String>();
        headers.put("Content-Type", "application/json");

        Mockito.stub(client.getMetadataStore()).toReturn(store);        
        Mockito.stub(client.executeCount(Mockito.any(Query.class), Mockito.anyListOf(SQLParameter.class))).toReturn(new CountResponse() {
            @Override
            public int getCount() {
                return 10;
            }
        });

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.stub(request.getHeaderNames()).toReturn(headers.keys());
        Mockito.stub(request.getHeaders("Content-Type")).toReturn(headers.elements());
        Mockito.stub(request.getMethod()).toReturn(method);
        
        String requestURL = url;
        String queryString = "";
        int idx = url.indexOf("?");
        if (idx != -1) {
            requestURL = url.substring(0, idx);
            queryString = url.substring(idx+1);
        }
        Mockito.stub(request.getRequestURL()).toReturn(new StringBuffer(requestURL));
        Mockito.stub(request.getQueryString()).toReturn(queryString);
        Mockito.stub(request.getServletPath()).toReturn("");
        Mockito.stub(request.getContextPath()).toReturn("/odata4/vdb/PM1");
        Mockito.stub(request.getInputStream()).toReturn(stream);

        final StringBuffer sb = new StringBuffer();
        ServletOutputStream out = new ServletOutputStream() {
            @Override
            public void write(int b) throws IOException {
                sb.append((char)b);
            }
            @Override
            public boolean isReady() {
                return true;
            }
            @Override
            public void setWriteListener(WriteListener writeListener) {
            }
        };
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.stub(response.getOutputStream()).toReturn(out);

        try {
            TeiidServiceHandler tsh = new TeiidServiceHandler("PM1");
            tsh.setPrepared(false);
            TeiidServiceHandler.setClient(client);
            handler.register(tsh);
            handler.process(request, response);
        } finally {
            TeiidServiceHandler.setClient(null);
        }
        ArgumentCaptor<Integer> statusCapture = ArgumentCaptor.forClass(Integer.class);
        Mockito.verify(response).setStatus(statusCapture.capture());
        state.client = client;        
        state.response = sb.toString();
        state.status = statusCapture.getValue();
        return state;
    }
    
    public QueryState helpTest(String url, String sqlExpected) throws Exception {
        QueryState state = setup(url);
        Client client = state.client;
        
        ArgumentCaptor<Query> arg1 = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<EntityCollectionResponse> arg6 = ArgumentCaptor.forClass(EntityCollectionResponse.class);
        List<SQLParameter> parameters = new ArrayList<SQLParameter>();
        
        if (sqlExpected != null) {
            Query actualCommand = (Query) QueryParser.getQueryParser().parseCommand(sqlExpected, new ParseInfo());
            Mockito.verify(client).executeSQL(arg1.capture(),
                    Mockito.eq(parameters), Mockito.eq(false),
                    (Integer) Mockito.eq(null), (Integer) Mockito.eq(null),
                    (String) Mockito.eq(null), Mockito.anyInt(),
                    arg6.capture());
            Assert.assertEquals(actualCommand.toString(), arg1.getValue().toString());
        }
        
        state.parameters = parameters;
        state.arg1 = arg1;
        state.arg6 = arg6;
        
        return state;
    }

    @Test
    public void testSimpleEntitySet() throws Exception {
        helpTest("/odata4/vdb/PM1/G1", "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testSimpleEntitySetWithKey() throws Exception {
        helpTest("/odata4/vdb/PM1/G1(1)", "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e2 = 1 ORDER BY g0.e2");
    }

    @Test
    public void testSimpleEntityID() throws Exception {
        helpTest("/odata4/vdb/PM1/$entity?$id=http://host/odata4/vdb/PM1/G1(1)", 
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e2 = 1 ORDER BY g0.e2");
    }

    @Test
    public void testEntitySet$Select() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$select=e1", "SELECT g0.e2, g0.e1 FROM PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testEntitySet$SelectBad() throws Exception {
        QueryState state = helpTest("/odata4/vdb/PM1/G1?$select=e1,x", null);
        Assert.assertEquals("{\"error\":{\"code\":null,\"message\":\"The property 'x', used in a query expression, is not defined in type 'G1'.\"}}", state.response);
    }

    @Test
    public void testEntitySet$OrderBy() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$orderby=e1 desc, e2",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY g0.e1 DESC, g0.e2");
        helpTest("/odata4/vdb/PM1/G1?$orderby=e1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY g0.e1");
    }

    @Test
    public void testEntitySet$OrderByNotIn$Select() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$orderby=e2&$select=e1",
                "SELECT g0.e2, g0.e1 FROM PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testEntitySet$filter() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq 1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 = 1 ORDER BY g0.e2");
    }

    @Test
    public void test_$it_OverPrimitiveProperty() throws Exception {
        helpTest("/odata4/vdb/PM1/G3(e1='e1',e2=2)/e3?$filter=endswith($it, '.com')",
                "SELECT g0.e1, g0.e2, CAST(g1.col AS string) AS e3 "
                + "FROM PM1.G3 AS g0, "
                + "TABLE(EXEC arrayiterate(g0.e3)) AS g1 "
                + "WHERE ((g0.e1 = 'e1') AND (g0.e2 = 2)) "
                + "AND (ENDSWITH('.com', CAST(g1.col AS string)) = TRUE) "
                + "ORDER BY g0.e1, g0.e2");
    }
        
    @Test
    public void test$CountIsTrueEntitySet() throws Exception {
        String expected = "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY g0.e2";
        QueryState state = helpTest("/odata4/vdb/PM1/G1?$count=true", null);

        Mockito.verify(state.client).executeSQL(state.arg1.capture(),
                Mockito.eq(state.parameters), Mockito.eq(true), (Integer)Mockito.eq(null),
                (Integer)Mockito.eq(null), 
                (String) Mockito.eq(null), Mockito.anyInt(),
                state.arg6.capture());
        Assert.assertEquals(expected, state.arg1.getValue().toString());
    }

    @Test
    public void test$CountInEntitySet() throws Exception {
        String expected = "SELECT COUNT(*) FROM PM1.G1 AS g0";
        QueryState state = helpTest("/odata4/vdb/PM1/G1/$count", null);

        Mockito.verify(state.client).executeCount(state.arg1.capture(), Mockito.eq(state.parameters));
        Assert.assertEquals(expected, state.arg1.getValue().toString());
    }
    
    @Test
    public void test$CountAnd$Filter() throws Exception {
        String expected = "SELECT COUNT(*) FROM PM1.G1 AS g0 WHERE g0.e3 < 10";
        QueryState state = helpTest("/odata4/vdb/PM1/G1/$count?$filter="+Encoder.encode("e3 lt 10"), null);

        Mockito.verify(state.client).executeCount(state.arg1.capture(), Mockito.eq(state.parameters));
        Assert.assertEquals(expected, state.arg1.getValue().toString());
        
    }    

    @Test
    public void test$CountInNavigation() throws Exception {
        String expected = "SELECT COUNT(*) FROM PM1.G1 AS g0 INNER JOIN PM1.G4 AS g1 ON g0.e2 = g1.e2 WHERE g0.e2 = 1";
        QueryState state = helpTest("/odata4/vdb/PM1/G1(1)/G4_FKX/$count", null);
        Mockito.verify(state.client).executeCount(state.arg1.capture(), Mockito.eq(state.parameters));
        Assert.assertEquals(expected, state.arg1.getValue().toString());
    }
    
    @Test
    public void test$CountIn$FilterWithCount() throws Exception {
        String expected = "SELECT g0.e2, g0.e1 FROM PM1.G1 AS g0 WHERE (SELECT COUNT(*) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) = 2 ORDER BY g0.e2";
        helpTest("/odata4/vdb/PM1/G1?$filter=G4_FKX/$count eq 2&$select=e1", expected);
    }
    
    @Test
    public void test$CountIn$FilterOnExpression() throws Exception {
        String expected = "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE (SELECT COUNT(*) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) = 2 ORDER BY g0.e2";
        helpTest("/odata4/vdb/PM1/G1?$filter="+Encoder.encode("G4_FKX/$count eq 2"), expected);
    }
    
    @Test
    public void test$CountIn$orderby() throws Exception {
        String expected = "SELECT (SELECT COUNT(*) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) AS \"_orderByAlias\", g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY \"_orderByAlias\"";
        helpTest("/odata4/vdb/PM1/G1?$orderby=G4_FKX/$count", expected);
    }    
        
    @Test
    public void test$CountIn$OrderBy() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$orderby=G4_FKX/$count", 
                "SELECT "
                + "(SELECT COUNT(*) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) AS \"_orderByAlias\", "
                + "g0.e1, g0.e2, g0.e3 "
                + "FROM PM1.G1 AS g0 "
                + "ORDER BY \"_orderByAlias\"");
    }

    @Test
    public void testCanonicalQuery() throws Exception {
        // this should be same as "/odata4/vdb/PM1/G1('1')"
        helpTest("/odata4/vdb/PM1/G2(1)/FK0(1)", "SELECT g1.e1, g1.e2, g1.e3 FROM PM1.G1 AS g1 WHERE g1.e2 = 1 ORDER BY g1.e2");
    }

    @Test
    public void testAlias() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e2 eq @p1&@p1=1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e2 = 1 ORDER BY g0.e2");
    }
    
    @Test
    public void testAlias2() throws Exception {
        QueryState state = helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq @p1&@p1=1",
                null);
        //should really be 400
        assertEquals(Integer.valueOf(500), state.status);
    }    

    @Test
    public void testAlias3() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq @p1&@p1='1'",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 = '1' ORDER BY g0.e2");
    }
    
    @Test
    public void testAliasWithExpression() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq @p1&@p1=$root/G2(1)/e1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 = (SELECT g1.e1 FROM PM1.G2 AS g1 WHERE g1.e2 = 1) ORDER BY g0.e2");
    }    
    
    @Test
    public void testMultiEntitykey() throws Exception {
        helpTest("/odata4/vdb/PM1/G3(e1='1',e2=2)",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G3 AS g0 WHERE g0.e1 = '1' AND g0.e2 = 2 ORDER BY g0.e1, g0.e2");
    }

    private void te(String in, String expected) throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter="+in,
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE "+expected+" ORDER BY g0.e2");
    }

    @Test
    public void testAnd() throws Exception {
        te("e1 eq 1 and e1 eq 2", "(g0.e1 = 1) AND (g0.e1 = 2)");
        te("(e1 add 4) eq 3", "(g0.e1 + 4) = 3");
        te("((e1 add e2) sub e3) mod e1 eq 0", "MOD(((g0.e1 + g0.e2) - g0.e3), g0.e1) = 0");
    }

    @Test
    public void testEq() throws Exception {
        te("e1 eq 1", "g0.e1 = 1");
        te("e1 eq 4.5", "g0.e1 = 4.5");
        te("e1 eq -4.5", "g0.e1 = -4.5");
        te("e1 eq null", "g0.e1 IS NULL");
        te("e1 eq 'foo'", "g0.e1 = 'foo'");
        te("e1 eq true", "g0.e1 = TRUE");
        te("e1 eq false", "g0.e1 = FALSE");
        
        Calendar gmt = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        gmt.clear();
        gmt.set(Calendar.HOUR, 8);
        gmt.set(Calendar.MINUTE, 20);
        gmt.set(Calendar.SECOND, 02);
        
        SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
        sdf.setTimeZone(TimeZone.getDefault());
        String time = sdf.format(new Time(gmt.getTimeInMillis()));
        String expected = "g0.e1 = {t'"+time+"'}";
        te("e1 eq 08:20:02", expected);
        
        
        gmt = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        gmt.clear();
        gmt.set(Calendar.YEAR, 2008);
        gmt.set(Calendar.MONTH, Calendar.OCTOBER);
        gmt.set(Calendar.DAY_OF_MONTH, 12);
        gmt.set(Calendar.HOUR, 8);
        gmt.set(Calendar.MINUTE, 20);
        gmt.set(Calendar.SECOND, 02);
        
        sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getDefault());
        time = sdf.format(new Time(gmt.getTimeInMillis()));
        expected = "g0.e1 = {ts '"+time+"'}";        
        te("e1 eq 2008-10-12T08:20:02Z", expected);
        
        gmt = Calendar.getInstance(TimeZone.getTimeZone("GMT-04:00"));
        gmt.clear();
        gmt.set(Calendar.YEAR, 2008);
        gmt.set(Calendar.MONTH, Calendar.OCTOBER);
        gmt.set(Calendar.DAY_OF_MONTH, 12);
        gmt.set(Calendar.HOUR, 8);
        gmt.set(Calendar.MINUTE, 20);
        gmt.set(Calendar.SECOND, 02);
        gmt.set(Calendar.MILLISECOND, 235);
        
        sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getDefault());
        time = sdf.format(new Time(gmt.getTimeInMillis()));
        expected = "g0.e1 = {ts '"+time+"'}";        
        te("e1 eq 2008-10-12T08:20:02.235-04:00", expected);        
    }

    @Test
    public void testCast() throws Exception {
        te("e1 eq cast('foo', Edm.String)", "g0.e1 = CONVERT('foo', string)");
        te("e1 eq cast('foo', Edm.Int32)", "g0.e1 = CONVERT('foo', integer)");
    }

    @Test
    public void testConcat() throws Exception {
        te("e1 eq concat('foo', 'bar')", "g0.e1 = CONCAT2('foo', 'bar')");
    }

    @Test
    public void testEndsWith() throws Exception {
        te("endswith(e1, 'foo')", "ENDSWITH('foo', g0.e1) = TRUE");
    }

    @Test
    public void testIndexOf() throws Exception {
        te("indexof(e1, 'foo') eq 1", "LOCATE('foo', g0.e1) = 1");
    }

    @Test
    public void testLength() throws Exception {
        te("length(e1) eq 2", "LENGTH(g0.e1) = 2");
    }

    @Test
    public void testOperator() throws Exception {
        te("not (e1)", "NOT (g0.e1)");
        te("(e1 mul e2) gt 5", "(g0.e1 * g0.e2) > 5");
        te("(e1 div 5) gt 5", "(g0.e1 / 5) > 5");
        te("(e1 add 5) lt 5", "(g0.e1 + 5) < 5");
        te("(e1 sub 5) ne 0", "(g0.e1 - 5) != 0");
        te("(e1 mod 5) eq 0", "MOD(g0.e1, 5) = 0");
        te("(e1 mul -1) eq 0", "(g0.e1 * -1) = 0");
    }

    @Test
    public void testComparisions() throws Exception {
        te("e1 gt e2", "g0.e1 > g0.e2");
        te("e1 lt e2", "g0.e1 < g0.e2");
        te("e1 ge e2", "g0.e1 >= g0.e2");
        te("e1 le e2", "g0.e1 <= g0.e2");
        te("e1 eq e2", "g0.e1 = g0.e2");
        te("e1 ne e2", "g0.e1 <> g0.e2");
        te("e1 eq null", "g0.e1 IS NULL");
        te("e1 ne null", "g0.e1 IS NOT NULL");
    }

    @Test
    public void testStringMethods() throws Exception {
        //te("replace(x, y, z)", "REPLACE(x, y, z)");

        te("substring('foo', 1) eq 'f'", "SUBSTRING('foo', 1) = 'f'");
        te("substring('foo', 1, 2) eq 'f'", "SUBSTRING('foo', 1, 2) = 'f'");
        te("tolower(e1) eq 'foo'", "LCASE(g0.e1) = 'foo'");
        te("toupper(e1) eq 'FOO'", "UCASE(g0.e1) = 'FOO'");
        te("trim('x') eq e1", "TRIM(' ' FROM 'x') = g0.e1");
        te("trim(e1) ne 'foo' and toupper(e1) eq 'bar'", "(TRIM(' ' FROM g0.e1) <> 'foo') AND (UCASE(g0.e1) = 'bar')");
        te("contains(e1, 'foo')", "LOCATE('foo', g0.e1, 1) >= 1");
    }


    @Test
    public void testStartsWith() throws Exception {
        te("startswith(e1, 'foo')", "LOCATE('foo', g0.e1, 1) = 1");
    }

    @Test
    public void testTimeMethods() throws Exception {
        te("year(e1) eq 2000", "YEAR(g0.e1) = 2000");
        
        Calendar gmt = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        gmt.clear();
        gmt.set(Calendar.YEAR, 2008);
        gmt.set(Calendar.MONTH, Calendar.OCTOBER);
        gmt.set(Calendar.DAY_OF_MONTH, 13);
        gmt.set(Calendar.HOUR, 8);
        gmt.set(Calendar.MINUTE, 20);
        gmt.set(Calendar.SECOND, 2);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getDefault());
        String time = sdf.format(new Time(gmt.getTimeInMillis()));
        String expected = "YEAR({ts'"+time+"'}) = 2008"; 
        
        te("year(2008-10-13T08:20:02Z) eq 2008", expected);
        te("month(e1) gt 1", "MONTH(g0.e1) > 1");
        te("day(e1) ne 1", "DAYOFMONTH(g0.e1) != 1");
        te("hour(e1) eq 12", "HOUR(g0.e1) = 12");
        te("minute(e1) lt 5", "MINUTE(g0.e1) < 5");
        te("second(e1) eq 3", "SECOND(g0.e1) = 3");
    }

    @Test
    public void testRoundMethods() throws Exception {
        te("round(e1) eq 0", "ROUND(g0.e1, 0) = 0");
        te("floor(e1) eq 0", "FLOOR(g0.e1) = 0");
        te("ceiling(e1) eq 1", "CEILING(g0.e1) = 1");
    }

    @Test
    public void testNavigationQuery() throws Exception {
        helpTest(
                "/odata4/vdb/PM1/G2(1)/FK0",
                "SELECT g1.e1, g1.e2, g1.e3 FROM PM1.G2 as g0 INNER JOIN PM1.G1 as g1 "
                + "ON g1.e2 = g0.e2 WHERE g0.e2 = 1 ORDER BY g1.e2");
    }

    @Test
    public void testNavigationQuery$Select() throws Exception {
        helpTest(
                "/odata4/vdb/PM1/G2(1)/FK0?$select=e1",
                "SELECT g1.e2, g1.e1 FROM PM1.G2 as g0 INNER JOIN PM1.G1 as g1 "
                + "ON g1.e2 = g0.e2 WHERE g0.e2 = 1 ORDER BY g1.e2");
    }

    @Test
    public void test$refCollection() throws Exception {
        helpTest("/odata4/vdb/PM1/G1(0)/G4_FKX/$ref",
                "SELECT g1.e1 FROM PM1.G1 AS g0 INNER JOIN PM1.G4 AS g1 ON g0.e2 = g1.e2 WHERE g0.e2 = 0 ORDER BY g1.e1");
    }

    @Test
    public void test$refEntity() throws Exception {
        helpTest(
                "/odata4/vdb/PM1/G2(1)/FK0/$ref",
                "SELECT g1.e2 FROM PM1.G2 as g0 INNER JOIN PM1.G1 as g1 "
                + "ON g1.e2 = g0.e2 WHERE g0.e2 = 1 ORDER BY g1.e2");
    }

    @Test
    public void testAddressingProperty() throws Exception {
        helpTest("/odata4/vdb/PM1/G2(1)/e1","SELECT g0.e1, g0.e2 FROM PM1.G2 AS g0 WHERE g0.e2 = 1 ORDER BY g0.e2");
    }

    @Test
    public void testAddressingPropertyValue() throws Exception {
        helpTest("/odata4/vdb/PM1/G1(1)/e1/$value","SELECT g0.e1, g0.e2 FROM PM1.G1 AS g0 WHERE g0.e2 = 1 ORDER BY g0.e2");
    }

    @Test
    public void test$filter() throws Exception {
        helpTest("/odata4/vdb/PM1/G2?$filter=e1 eq 1","SELECT g0.e1, g0.e2 FROM PM1.G2 AS g0 WHERE g0.e1 = 1 ORDER BY g0.e2");
        helpTest("/odata4/vdb/PM1/G2?$filter=contains(e1, 'foo')","SELECT g0.e1, g0.e2 FROM PM1.G2 AS g0 WHERE LOCATE('foo', g0.e1, 1) >= 1 ORDER BY g0.e2");
        helpTest("/odata4/vdb/PM1/G2?$filter=(4 add 5) mod (4 sub 1) eq 0","SELECT g0.e1, g0.e2 FROM PM1.G2 AS g0 WHERE MOD((4 + 5), (4 - 1)) = 0 ORDER BY g0.e2");
    }
    
    @Test
    public void testInsert() throws Exception {
        String payload = "{\n" +
                "  \"e1\":\"teiid\",\n" +
                "  \"e2\":1,\n" +
                "  \"e3\":2.0" +
                "}";
        helpInsert("/odata4/vdb/PM1/G1",
                "insert into PM1.G1 (e1, e2, e3) values('teiid', 1, 2.0)", 
                new StringServletInputStream(payload), "POST");
    }    
    
    @Test
    public void testUpdate() throws Exception {
        String payload = "{ \"e1\":\"teiid\", \"e3\":3.0}";
        helpInsert("/odata4/vdb/PM1/G1(1)",
                "INSERT INTO PM1.G1 (e1, e3) VALUES ('teiid', 3.0)",  
                new StringServletInputStream(payload),"PATCH");
    }    
    
    @Test
    public void testUpdateProperty() throws Exception {
        String payload = "{\"value\":4.0}";
        helpInsert("/odata4/vdb/PM1/G1(1)/e3",
                "UPDATE PM1.G1 SET e3 = 4.0 WHERE PM1.G1.e2 = 1",  
                new StringServletInputStream(payload),"PUT");
    }
    
    @Test
    public void testDeleteProperty() throws Exception {
        String payload = "{\"value\":4.0}";
        helpInsert("/odata4/vdb/PM1/G1(1)/e3",
                "UPDATE PM1.G1 SET e3 = null WHERE PM1.G1.e2 = 1",  
                new StringServletInputStream(payload),"DELETE");
    }    
    
    @Test
    public void testDelete() throws Exception {
        String payload = "";
        helpInsert("/odata4/vdb/PM1/G1(1)",
                "DELETE FROM PM1.G1 WHERE PM1.G1.e2 = 1",  
                new StringServletInputStream(payload),"DELETE");
    }    

    @Test
    public void testUpdateReference() throws Exception {
        String payload = "{\n" +
                "\"@odata.id\": \"/odata4/vdb/PM1/G1(9)\"\n" +
                "}";            

        helpInsert("/odata4/vdb/PM1/G2(1)/FK0/$ref",
                "UPDATE PM1.G2 SET PM1.G2.e2 = 9 WHERE PM1.G2.e2 = 1",  
                new StringServletInputStream(payload),"PUT");
    }
    
    @Test
    public void testAddReference() throws Exception {
        String payload = "{\n" +
                "\"@odata.id\": \"/odata4/vdb/PM1/G4(9)\"\n" +
                "}";            

        helpInsert("/odata4/vdb/PM1/G1(1)/G4_FKX/$ref",
                "UPDATE PM1.G4 SET e2 = 1 WHERE PM1.G4.e1 = 9",  
                new StringServletInputStream(payload),"POST");
    }    

    @Test
    public void testDeleteReferenceNonCollection() throws Exception {
        String payload = "";            
        helpInsert("/odata4/vdb/PM1/G2(1)/FK0/$ref",
                "UPDATE PM1.G2 SET PM1.G2.e2 = null WHERE PM1.G2.e2 = 1",  
                new StringServletInputStream(payload),"DELETE");
    }
    
    @Test
    public void testDeleteReferenceCollectionValued() throws Exception {
        String payload = "";            

        String id = "$id=/odata4/vdb/PM1/G4(9)";
        helpInsert("/odata4/vdb/PM1/G1(1)/G4_FKX/$ref?"+id,
                "UPDATE PM1.G4 SET e2 = null WHERE PM1.G4.e1 = 9",  
                new StringServletInputStream(payload),"DELETE");
    }    
    
    private UpdateState helpInsert(String url, String sqlExpected, 
            StringServletInputStream stream, String method) throws Exception{
        UpdateState state = (UpdateState)setup(DEFAULT_DDL, url, method, stream, new UpdateState());
        
        Client client = state.client;
        
        ArgumentCaptor<Command> arg1 = ArgumentCaptor.forClass(Command.class);
        ArgumentCaptor<List> arg2 = ArgumentCaptor.forClass(List.class);
        
        if (sqlExpected != null) {
            Command actualCommand = (Command) QueryParser.getQueryParser().parseCommand(sqlExpected, new ParseInfo());
            Mockito.verify(client).executeUpdate(arg1.capture(), arg2.capture());
            Assert.assertEquals(actualCommand.toString(), arg1.getValue().toString());
        }
        
        state.commandArg = arg1;
        
        return state;
    }

    static class StringServletInputStream extends ServletInputStream{
        ByteArrayInputStream stream;
        
        public StringServletInputStream(String content) {
            this.stream = new ByteArrayInputStream(content.getBytes());
        }
        @Override
        public int read() throws IOException {
            return this.stream.read();
        }
        @Override
        public boolean isFinished() {
            return false;
        }
        @Override
        public boolean isReady() {
            return true;
        }
        @Override
        public void setReadListener(ReadListener readListener) {
        }
    }
            
    @Test
    public void testNavigationalQuery() throws Exception {
        helpTest("/odata4/vdb/PM1/G2(0)/FK0", "SELECT g1.e1, g1.e2, g1.e3 FROM PM1.G2 AS g0 "
                + "INNER JOIN PM1.G1 AS g1 ON g1.e2 = g0.e2 WHERE g0.e2 = 0 ORDER BY g1.e2");

        //Canonical query
        helpTest("/odata4/vdb/PM1/G2(0)/FK0(0)/e1", "SELECT g1.e1, g1.e2 FROM PM1.G1 AS g1 WHERE g1.e2 = 0 ORDER BY g1.e2");

        helpTest("/odata4/vdb/PM1/G1(0)/G4_FKX", "SELECT g1.e1, g1.e2 FROM PM1.G1 AS g0 "
                + "INNER JOIN PM1.G4 AS g1 ON g0.e2 = g1.e2 WHERE g0.e2 = 0 ORDER BY g1.e1");
    }
    
    @Test
    public void test$RootOverPath() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq $root/G1(1)/e1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 = (SELECT g1.e1 FROM PM1.G1 AS g1 WHERE g1.e2 = 1) ORDER BY g0.e2");
    } 
    
    @Test
    public void testAny() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter="+Encoder.encode("G4_FKX/any(ol: ol/e2 gt 10)"), 
                "SELECT DISTINCT ol.e1, ol.e2 FROM PM1.G1 AS g0 "
                + "INNER JOIN PM1.G4 AS ol ON g0.e2 = ol.e2 "
                + "WHERE ol.e2 > 10 ORDER BY ol.e1");
    }

    @Test
    public void testAll() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter="+Encoder.encode("G4_FKX/all(ol: ol/e2 gt 10)"),
                "SELECT g0.e1, g0.e2, g0.e3 "
                + "FROM PM1.G1 AS g0 WHERE 10 < ALL (SELECT ol.e2 FROM PM1.G4 AS ol "
                + "WHERE g0.e2 = ol.e2) ORDER BY g0.e2");        
    }
    
    @Test
    public void testExpandSimple() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX",
                "SELECT g0.e1, g0.e2, g0.e3, g1.e1, g1.e2 "
                + "FROM PM1.G1 AS g0 "
                + "LEFT OUTER JOIN PM1.G4 AS g1 "
                + "ON g0.e2 = g1.e2 "
                + "ORDER BY g0.e2");        
    }
    
    @Test
    public void testExpandSimple_OneToOne() throws Exception {
        helpTest("/odata4/vdb/PM1/G2?$expand=FK0",
                "SELECT g0.e1, g0.e2, g1.e1, g1.e2, g1.e3 "
                + "FROM PM1.G2 AS g0 "
                + "LEFT OUTER JOIN PM1.G1 AS g1 "
                + "ON g1.e2 = g0.e2 "
                + "ORDER BY g0.e2");   
    }    
    
    @Test
    public void testExpandSimpleWithSelect() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX&$select=e3",
                "SELECT g0.e2, g0.e3, g1.e1, g1.e2 "
                + "FROM PM1.G1 AS g0 "
                + "LEFT OUTER JOIN PM1.G4 AS g1 "
                + "ON g0.e2 = g1.e2 "
                + "ORDER BY g0.e2");        
    }
    
    @Test
    public void testExpandWithNestedSelect() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($select=e2)&$select=e3",
                "SELECT g0.e2, g0.e3, g1.e2 "
                + "FROM PM1.G1 AS g0 "
                + "LEFT OUTER JOIN PM1.G4 AS g1 "
                + "ON g0.e2 = g1.e2 "
                + "ORDER BY g0.e2");        
    }
    
    @Test
    public void testExpandFilter() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($filter=e2 eq 100)&$select=e3",
                "SELECT g0.e2, g0.e3, g1.e1, g1.e2 "
                + "FROM PM1.G1 AS g0 "
                + "LEFT OUTER JOIN PM1.G4 AS g1 "
                + "ON g0.e2 = g1.e2 "
                + "WHERE g1.e2 = 100 "
                + "ORDER BY g0.e2");        
    }
    
    @Test
    public void testExpandFilter2() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($filter=e2 eq e1)&$select=e3",
                "SELECT g0.e2, g0.e3, g1.e1, g1.e2 "
                + "FROM PM1.G1 AS g0 "
                + "LEFT OUTER JOIN PM1.G4 AS g1 "
                + "ON g0.e2 = g1.e2 "
                + "WHERE g1.e2 = g1.e1 "
                + "ORDER BY g0.e2");        
    }    
    
    @Test
    public void testExpandCompoundFilter() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($filter=e2 eq 100)&$select=e3&$filter=e2 ne 100",
                "SELECT g0.e2, g0.e3, g1.e1, g1.e2 "
                + "FROM PM1.G1 AS g0 "
                + "LEFT OUTER JOIN PM1.G4 AS g1 "
                + "ON g0.e2 = g1.e2 "
                + "WHERE g0.e2 != 100 AND g1.e2 = 100 "
                + "ORDER BY g0.e2");        
    }
    
    @Test
    public void testExpandOrderby() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($orderby=e1 desc)",
                "SELECT g0.e1, g0.e2, g0.e3, g1.e1, g1.e2 "
                + "FROM PM1.G1 AS g0 "
                + "LEFT OUTER JOIN PM1.G4 AS g1 "
                + "ON g0.e2 = g1.e2 "
                + "ORDER BY g0.e2, g1.e1 DESC");        
    }
    
    @Test
    public void testSimpleCrossJoin() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1, G2)",
                "SELECT g0.e2, g1.e2 "
                + "FROM PM1.G1 AS g0, "
                + "PM1.G2 AS g1 "
                + "ORDER BY g0.e2, g1.e2");        
    } 
    
    @Test
    public void testSimpleCrossJoinWithFilter() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1, G2)?$filter="+Encoder.encode("G1/e1 eq G2/e2"),
                "SELECT g0.e2, g1.e2 "
                + "FROM PM1.G1 AS g0, "
                + "PM1.G2 AS g1 "
                + "WHERE g0.e1 = g1.e2 "
                + "ORDER BY g0.e2, g1.e2");        
    }
    
    @Test
    public void testSimpleCrossJoinWith$Orderby() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1, G2)?$orderby=G1/e1,G2/e2",
                "SELECT g0.e2, g0.e1, g1.e2 "
                + "FROM PM1.G1 AS g0, "
                + "PM1.G2 AS g1 "
                + "ORDER BY g0.e1, g1.e2");        
    }
    
    @Test
    public void testSimpleCrossJoinWith$expand() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1, G2)?$expand=G1",
                "SELECT g0.e1, g0.e3, g0.e2, g1.e2 "
                + "FROM PM1.G1 AS g0, "
                + "PM1.G2 AS g1 "
                + "ORDER BY g0.e2, g1.e2");        
    }    
    
    @Test
    public void testSelectStarWithNonSelectableColumn() throws Exception {
        helpTest("/odata4/vdb/PM1/G5",
                "SELECT g0.e2, g0.e3 FROM PM1.G5 AS g0 ORDER BY g0.e2");        
    }
    
    @Test
    public void testSelectStarWithNonSelectableColumn2() throws Exception {
        helpTest("/odata4/vdb/PM1/G5?$select=*",
                "SELECT g0.e2, g0.e3 FROM PM1.G5 AS g0 ORDER BY g0.e2");        
    }
    
    @Test
    public void testSelectStarWithNonSelectableColumn3() throws Exception {
        helpTest("/odata4/vdb/PM1/G5?$filter=e1 eq 1",
                "SELECT g0.e2, g0.e3 FROM PM1.G5 AS g0 WHERE g0.e1 = 1 ORDER BY g0.e2");        
    }   
    
    @BeforeClass public static void oneTimeSetup() {
    	TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT"));
    }
    
    @AfterClass public static void oneTimeTearDown() {
    	TimestampWithTimezone.resetCalendar(null);
    }
    
}
