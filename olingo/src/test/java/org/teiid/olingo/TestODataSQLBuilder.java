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

import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.commons.core.Encoder;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.core.OData4Impl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
import org.teiid.olingo.service.TeiidEdmProvider;
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
            "    e3 string[],\n"+
            "    CONSTRAINT PK PRIMARY KEY (e1,e2)\n" +
            ") OPTIONS (UPDATABLE 'true')" +
            "\n" +
            "CREATE FOREIGN TABLE G4 (\n" +
            "    e1 string PRIMARY KEY, \n" +
            "    e2 integer,\n" +
            "    CONSTRAINT FKX FOREIGN KEY (e2) REFERENCES G1(e2)\n" +
            ") OPTIONS (UPDATABLE 'true');" +
            "\n"+
            "CREATE FOREIGN TABLE G4a(\n" +
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
            "CREATE FOREIGN PROCEDURE getCustomers(" +
            "   IN p2 timestamp, "+
            "   IN p3 decimal"+
            "   ) RETURNS integer;\n" +
            "CREATE FOREIGN TABLE SimpleTable(\n" +
            "    intkey integer PRIMARY KEY,\n" +
            "    intnum integer,\n" +
            "    stringkey varchar(20),\n" +
            "    stringval varchar(20),\n" +
            "    booleanval boolean,\n" +
            "    decimalval decimal(20, 10),\n" +
            "    timeval time,\n" +
            "    dateval date,\n" +
            "    timestampval timestamp,\n" +
            "    clobval clob);"+
            "CREATE FOREIGN TABLE Customers("+
            "   id integer PRIMARY KEY, " +
            "   name varchar(10));\n" +
            "CREATE FOREIGN TABLE Orders("+
            "   id integer PRIMARY KEY, " +
            "   customerid integer, " +
            "   place varchar(10), "+
            "   FOREIGN KEY (customerid) REFERENCES Customers(id));" +
            "CREATE FOREIGN TABLE EmployeeEntity (\n" +
            "  EmployeeID integer primary key,\n" +
            "  Delegate integer," +
            "  DeputyDelegate integer," +
            " CONSTRAINT delegates FOREIGN KEY (Delegate) REFERENCES EmployeeEntity(EmployeeID)," +
            " CONSTRAINT deputyDelegates FOREIGN KEY (DeputyDelegate) REFERENCES EmployeeEntity(EmployeeID)"
            + ");\n"
            + "CREATE FOREIGN table geo (id integer primary key,"
            + "location geometry options (\"teiid_spatial:coord_dimension\" 2, \"teiid_spatial:srid\" 4326, \"teiid_spatial:type\" 'POINT')"
            + ",line geometry options (\"teiid_spatial:coord_dimension\" 2, \"teiid_spatial:srid\" 4326, \"teiid_spatial:type\" 'LINESTRING')"
            + ",polygon geometry options (\"teiid_spatial:coord_dimension\" 2, \"teiid_spatial:srid\" 4326, \"teiid_spatial:type\" 'POLYGON')"
            + ")";

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
        TeiidEdmProvider edmProvider = new TeiidEdmProvider("baseuri", schema, "x");

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
        return helpTest(url, sqlExpected, null, null, false);
    }
    public QueryState helpTest(String url, String sqlExpected, Integer skip, Integer top, Boolean count) throws Exception {
        QueryState state = setup(url);
        Client client = state.client;

        ArgumentCaptor<Query> arg1 = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<EntityCollectionResponse> arg6 = ArgumentCaptor.forClass(EntityCollectionResponse.class);
        List<SQLParameter> parameters = new ArrayList<SQLParameter>();

        if (sqlExpected != null) {
            Query actualCommand = (Query) QueryParser.getQueryParser().parseCommand(sqlExpected, new ParseInfo());
            Mockito.verify(client).executeSQL(arg1.capture(),
                    Mockito.eq(parameters), Mockito.eq(count),
                    Mockito.eq(skip), Mockito.eq(top),
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
    public void testSimpleEntitySetWithKeyWithAlias() throws Exception {
        helpTest("/odata4/vdb/PM1/G1(@e2)?@e2=1", "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e2 = 1 ORDER BY g0.e2");
    }

    @Test
    public void testSimpleEntityID() throws Exception {
        helpTest("/odata4/vdb/PM1/$entity?$id="+Encoder.encode("http://host/odata4/vdb/PM1/G1(1)"),
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e2 = 1 ORDER BY g0.e2");
    }

    @Test
    public void testEntitySet$Select() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$select=e1", "SELECT g0.e1, g0.e2 FROM PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testEntitySet$SelectBad() throws Exception {
        QueryState state = helpTest("/odata4/vdb/PM1/G1?$select=e1,x", null);
        Assert.assertEquals("{\"error\":{\"code\":null,\"message\":\"The property 'x', used in a query expression, is not defined in type 'G1'.\"}}", state.response);
    }

    @Test
    public void testEntitySet$OrderBy() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$orderby=e1 desc,e2",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY g0.e1 DESC NULLS LAST, g0.e2 NULLS FIRST");
        helpTest("/odata4/vdb/PM1/G1?$orderby=e1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY g0.e1 NULLS FIRST");
    }

    @Test
    public void testEntitySet$OrderByNotIn$Select() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$orderby=e2&$select=e1",
                "SELECT g0.e1, g0.e2 FROM PM1.G1 AS g0 ORDER BY g0.e2 NULLS FIRST");
    }

    @Test
    public void testEntitySet$filter() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq '1'",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 = '1' ORDER BY g0.e2");
    }

    @Test
    public void test_$it_OverPrimitiveProperty() throws Exception {
        helpTest("/odata4/vdb/PM1/G3(e1='e1',e2=2)/e3?$filter=endswith($it,'.com')",
                "SELECT g0.e1, g0.e2, ARRAY_AGG(CAST(g1.col AS string) ORDER BY g0.e1, g0.e2) AS e3 FROM PM1.G3 AS g0, LATERAL(EXEC arrayiterate(g0.e3)) AS g1 WHERE (g0.e1 = 'e1') AND (g0.e2 = 2) AND (ENDSWITH('.com', CAST(g1.col AS string)) = TRUE) GROUP BY g0.e1, g0.e2");
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
        String expected = "SELECT g0.e1, g0.e2 FROM PM1.G1 AS g0 WHERE "
                + "(SELECT COUNT(*) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) = 2 ORDER BY g0.e2";
        helpTest("/odata4/vdb/PM1/G1?$filter=G4_FKX/$count eq 2&$select=e1", expected);
    }

    @Test
    public void test$CountIn$FilterOnExpression() throws Exception {
        String expected = "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 "
                + "WHERE (SELECT COUNT(*) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) = 2 "
                + "ORDER BY g0.e2";
        helpTest("/odata4/vdb/PM1/G1?$filter="+Encoder.encode("G4_FKX/$count eq 2"), expected);
    }

    @Test
    public void test$CountIn$orderby() throws Exception {
        String expected = "SELECT g0.e1, g0.e2, g0.e3, (SELECT COUNT(*) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) "
                + "AS \"_orderByAlias_1\" FROM PM1.G1 AS g0 "
                + "ORDER BY \"_orderByAlias_1\" NULLS FIRST";
        helpTest("/odata4/vdb/PM1/G1?$orderby=G4_FKX/$count", expected);
    }

    @Test
    public void test$CountIn$OrderBy() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$orderby=G4_FKX/$count",
                "SELECT g0.e1, g0.e2, g0.e3, "
                + "(SELECT COUNT(*) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) AS \"_orderByAlias_1\" "
                + "FROM PM1.G1 AS g0 "
                + "ORDER BY \"_orderByAlias_1\" NULLS FIRST");

        helpTest("/odata4/vdb/PM1/G1?$orderby=G4_FKX/$count,G4a_FKX/$count",
                "SELECT g0.e1, g0.e2, g0.e3, "
                + "(SELECT COUNT(*) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) AS \"_orderByAlias_1\", "
                + "(SELECT COUNT(*) FROM PM1.G4a AS g2 WHERE g0.e2 = g2.e2) AS \"_orderByAlias_2\" "
                + "FROM PM1.G1 AS g0 "
                + "ORDER BY \"_orderByAlias_1\" NULLS FIRST, \"_orderByAlias_2\" NULLS FIRST");
    }

    @Test
    public void testCanonicalQuery() throws Exception {
        // this should be same as "/odata4/vdb/PM1/G1('1')"
        helpTest("/odata4/vdb/PM1/G2(1)/FK0",
                "SELECT g1.e1, g1.e2, g1.e3 FROM PM1.G2 AS g0 INNER JOIN PM1.G1 AS g1 "
                + "ON g1.e2 = g0.e2 WHERE g0.e2 = 1 ORDER BY g1.e2");

        helpTest("/odata4/vdb/PM1/G1(1)/G4_FKX('1')",
                "SELECT g1.e1, g1.e2 FROM PM1.G4 AS g1 WHERE g1.e1 = '1' ORDER BY g1.e1");
    }

    @Test
    public void testAlias() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e2 eq @p1&@p1=1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e2 = 1 ORDER BY g0.e2");
    }

    @Test
    public void testAlias2InvalidType() throws Exception {
        QueryState state = helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq @p1&@p1=1", null);
        assertEquals(Integer.valueOf(400), state.status);
    }

    @Test
    public void testAlias3() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq @p1&@p1='1'",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 = '1' ORDER BY g0.e2");
    }

    @Test
    public void testNoAlias() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq @p1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 is NULL ORDER BY g0.e2");
    }

    @Test
    public void testNoAlias2() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e2 eq @p1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e2 is NULL ORDER BY g0.e2");
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

    private void tea(String in, String expected) throws Exception {
        helpTest("/odata4/vdb/PM1/SimpleTable?$select=intkey&$filter="+in,
                "SELECT g0.intkey FROM PM1.SimpleTable AS g0 WHERE "+expected+" ORDER BY g0.intkey");
    }

    @Test
    public void testAnd() throws Exception {
        te("e1 eq '1' and e1 eq '2'", "(g0.e1 = '1') AND (g0.e1 = '2')");
        te("(e2 add 4) eq 3", "(g0.e2 + 4) = 3");
        tea("((intkey add intnum) sub decimalval) mod intkey eq 0",
                "MOD(((g0.intkey + g0.intnum) - g0.decimalval), g0.intkey) = 0");
    }

    @Test
    public void testEq() throws Exception {
        te("e2 eq 1", "g0.e2 = 1");
        te("e3 eq 4.5", "g0.e3 = 4.5");
        te("e3 eq -4.5", "g0.e3 = -4.5");
        te("e1 eq null", "g0.e1 IS NULL");
        te("e1 eq 'foo'", "g0.e1 = 'foo'");
        tea("booleanval eq true", "g0.booleanval = TRUE");
        tea("booleanval eq false", "g0.booleanval = FALSE");

        Calendar gmt = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        gmt.clear();
        gmt.set(Calendar.HOUR, 8);
        gmt.set(Calendar.MINUTE, 20);
        gmt.set(Calendar.SECOND, 02);

        SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
        sdf.setTimeZone(TimeZone.getDefault());
        String time = sdf.format(new Time(gmt.getTimeInMillis()));
        String expected = "g0.timeval = {t'"+time+"'}";
        tea("timeval eq 08:20:02", expected);


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
        expected = "g0.timestampval = {ts '"+time+"'}";
        tea("timestampval eq 2008-10-12T08:20:02Z", expected);

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
        expected = "g0.timestampval = {ts '"+time+"'}";
        tea("timestampval eq 2008-10-12T08:20:02.235-04:00", expected);
    }

    @Test
    public void testCast() throws Exception {
        te("e1 eq cast('foo',Edm.String)", "g0.e1 = CONVERT('foo', string)");
        te("e2 eq cast('32',Edm.Int32)", "g0.e2 = CONVERT('32', integer)");
    }

    @Test
    public void testConcat() throws Exception {
        te("e1 eq concat('foo','bar')", "g0.e1 = CONCAT('foo', 'bar')");
    }

    @Test
    public void testEndsWith() throws Exception {
        te("endswith(e1,'foo')", "ENDSWITH('foo', g0.e1) = TRUE");
    }

    @Test
    public void testIndexOf() throws Exception {
        te("indexof(e1,'foo') eq 1", "(LOCATE('foo', g0.e1) - 1) = 1");
    }

    @Test
    public void testSubString() throws Exception {
        te("substring(e1,1) eq 'x'", "SUBSTRING(g0.e1, CASE WHEN 1 < 0 THEN 1 ELSE (1 + 1) END) = 'x'");
        te("substring(e1,-1) eq 'x'", "SUBSTRING(g0.e1, CASE WHEN -1 < 0 THEN -1 ELSE (-1 + 1) END) = 'x'");
        te("substring(e1,1,2) eq 'x'", "SUBSTRING(g0.e1, CASE WHEN 1 < 0 THEN 1 ELSE (1 + 1) END, 2) = 'x'");
    }

    @Test
    public void testLength() throws Exception {
        te("length(e1) eq 2", "LENGTH(g0.e1) = 2");
    }

    @Test
    public void testOperator() throws Exception {
        tea("not (booleanval)", "NOT (g0.booleanval)");
        tea("(intkey mul intkey) gt 5", "(g0.intkey * g0.intkey) > 5");
        tea("(intkey div 5) gt 5", "(g0.intkey / 5) > 5");
        tea("(intkey add 5) lt 5", "(g0.intkey + 5) < 5");
        tea("(intkey sub 5) ne 0", "(g0.intkey - 5) != 0");
        tea("(intkey mod 5) eq 0", "MOD(g0.intkey, 5) = 0");
        tea("(intkey mul -1) eq 0", "(g0.intkey * -1) = 0");
    }

    @Test
    public void testComparisions() throws Exception {
        tea("intkey gt intnum", "g0.intkey > g0.intnum");
        tea("intkey lt intnum", "g0.intkey < g0.intnum");
        tea("intkey ge intnum", "g0.intkey >= g0.intnum");
        tea("intkey le intnum", "g0.intkey <= g0.intnum");
        tea("intkey eq intnum", "g0.intkey = g0.intnum");
        tea("intkey ne intnum", "g0.intkey <> g0.intnum");
        te("e1 eq null", "g0.e1 IS NULL");
        te("e1 ne null", "g0.e1 IS NOT NULL");
    }

    @Test
    public void testStringMethods() throws Exception {
        //te("replace(x, y, z)", "REPLACE(x, y, z)");

        te("substring('foo',1) eq 'f'", "SUBSTRING('foo', CASE WHEN 1 < 0 THEN 1 ELSE (1 + 1) END) = 'f'");
        te("substring('foo',1,2) eq 'f'", "SUBSTRING('foo', CASE WHEN 1 < 0 THEN 1 ELSE (1 + 1) END, 2) = 'f'");
        te("tolower(e1) eq 'foo'", "LCASE(g0.e1) = 'foo'");
        te("toupper(e1) eq 'FOO'", "UCASE(g0.e1) = 'FOO'");
        te("trim('x') eq e1", "TRIM(' ' FROM 'x') = g0.e1");
        te("trim(e1) ne 'foo' and toupper(e1) eq 'bar'", "(TRIM(' ' FROM g0.e1) <> 'foo') AND (UCASE(g0.e1) = 'bar')");
        te("contains(e1,'foo')", "LOCATE('foo', g0.e1, 1) >= 1");
    }


    @Test
    public void testStartsWith() throws Exception {
        te("startswith(e1,'foo')", "LOCATE('foo', g0.e1, 1) = 1");
    }

    @Test
    public void testTimeMethods() throws Exception {
        tea("year(dateval) eq 2000", "YEAR(g0.dateval) = 2000");

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
        tea("month(dateval) gt 1", "MONTH(g0.dateval) > 1");
        tea("day(dateval) ne 1", "DAYOFMONTH(g0.dateval) != 1");
        tea("hour(timeval) eq 12", "HOUR(g0.timeval) = 12");
        tea("minute(timeval) lt 5", "MINUTE(g0.timeval) < 5");
        tea("second(timeval) eq 3", "SECOND(g0.timeval) = 3");
    }

    @Test
    public void testRoundMethods() throws Exception {
        tea("round(decimalval) eq 0", "ROUND(g0.decimalval, 0) = 0");
        tea("floor(decimalval) eq 0", "FLOOR(g0.decimalval) = 0");
        tea("ceiling(decimalval) eq 1", "CEILING(g0.decimalval) = 1");
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
                "SELECT g1.e1, g1.e2 FROM PM1.G2 as g0 INNER JOIN PM1.G1 as g1 "
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
        helpTest("/odata4/vdb/PM1/G2?$filter=e1 eq '1'",
                "SELECT g0.e1, g0.e2 FROM PM1.G2 AS g0 WHERE g0.e1 = '1' ORDER BY g0.e2");
        helpTest("/odata4/vdb/PM1/G2?$filter=contains(e1,'foo')",
                "SELECT g0.e1, g0.e2 FROM PM1.G2 AS g0 WHERE LOCATE('foo', g0.e1, 1) >= 1 ORDER BY g0.e2");
        helpTest("/odata4/vdb/PM1/G2?$filter=(4 add 5) mod (4 sub 1) eq 0",
                "SELECT g0.e1, g0.e2 FROM PM1.G2 AS g0 WHERE MOD((4 + 5), (4 - 1)) = 0 ORDER BY g0.e2");
    }

    @Test
    public void test$filterWithNavigation() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=G4_FKX/$count lt 2",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 "
                + "WHERE (SELECT COUNT(*) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) < 2 ORDER BY g0.e2");

        helpTest("/odata4/vdb/PM1/Customers?$filter=Orders_FK0/$count lt 2",
                "SELECT g0.id, g0.name FROM PM1.Customers AS g0 "
                + "WHERE (SELECT COUNT(*) FROM PM1.Orders AS g1 "
                + "WHERE g0.id = g1.customerid) < 2 ORDER BY g0.id");
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
                "\"@odata.id\": \"/odata4/vdb/PM1/G4('9')\"\n" +
                "}";

        helpInsert("/odata4/vdb/PM1/G1(1)/G4_FKX/$ref",
                "UPDATE PM1.G4 SET e2 = 1 WHERE PM1.G4.e1 = '9'",
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

        String id = "$id=/odata4/vdb/PM1/G4('9')";
        helpInsert("/odata4/vdb/PM1/G1(1)/G4_FKX/$ref?"+id,
                "UPDATE PM1.G4 SET e2 = null WHERE PM1.G4.e1 = '9'",
                new StringServletInputStream(payload),"DELETE");
    }

    private UpdateState helpInsert(String url, String sqlExpected,
            StringServletInputStream stream, String method) throws Exception{
        UpdateState state = (UpdateState)setup(DEFAULT_DDL, url, method, stream, new UpdateState());

        Client client = state.client;

        ArgumentCaptor<Command> arg1 = ArgumentCaptor.forClass(Command.class);
        ArgumentCaptor<List> arg2 = ArgumentCaptor.forClass(List.class);

        if (sqlExpected != null) {
            Command actualCommand = QueryParser.getQueryParser().parseCommand(sqlExpected, new ParseInfo());
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
        helpTest("/odata4/vdb/PM1/G1(0)/G4_FKX('0')/e1", "SELECT g1.e1 FROM PM1.G4 AS g1 WHERE g1.e1 = '0' ORDER BY g1.e1");

        helpTest("/odata4/vdb/PM1/G1(0)/G4_FKX", "SELECT g1.e1, g1.e2 FROM PM1.G1 AS g0 "
                + "INNER JOIN PM1.G4 AS g1 ON g0.e2 = g1.e2 WHERE g0.e2 = 0 ORDER BY g1.e1");
    }

    @Test
    public void testSelfNavigation() throws Exception {
        helpTest("/odata4/vdb/PM1/EmployeeEntity(3)/deputyDelegates", "SELECT g1.EmployeeID, g1.Delegate, g1.DeputyDelegate "
                + "FROM PM1.EmployeeEntity AS g0 INNER JOIN PM1.EmployeeEntity AS g1 ON g1.EmployeeID = g0.DeputyDelegate WHERE g0.EmployeeID = 3 ORDER BY g1.EmployeeID");

        helpTest("/odata4/vdb/PM1/EmployeeEntity(3)/delegates", "SELECT g1.EmployeeID, g1.Delegate, g1.DeputyDelegate "
                + "FROM PM1.EmployeeEntity AS g0 INNER JOIN PM1.EmployeeEntity AS g1 ON g1.EmployeeID = g0.Delegate WHERE g0.EmployeeID = 3 ORDER BY g1.EmployeeID");
    }

    @Test
    public void test$RootOverPath() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq $root/G1(1)/e1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 = (SELECT g1.e1 FROM PM1.G1 AS g1 WHERE g1.e2 = 1) ORDER BY g0.e2");
    }

    @Test
    public void test$RootOverPath1() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=$root/G1(1)/e1 eq e1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE (SELECT g1.e1 FROM PM1.G1 AS g1 WHERE g1.e2 = 1) = g0.e1 ORDER BY g0.e2");
    }

    @Test
    public void testAny() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter="+Encoder.encode("G4_FKX/any(ol:ol/e2 gt 10)"),
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE EXISTS (SELECT 1 FROM PM1.G4 AS ol WHERE (g0.e2 = ol.e2) AND (ol.e2 > 10)) ORDER BY g0.e2");
    }

    @Test
    public void testAnyNoLambdaVariable() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter="+Encoder.encode("G4_FKX/any(ol:1 gt 10)"),
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE 1 > 10 ORDER BY g0.e2");
    }

    @Test
    public void testAll() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter="+Encoder.encode("G4_FKX/all(ol:ol/e2 gt 10)"),
                "SELECT g0.e1, g0.e2, g0.e3 "
                + "FROM PM1.G1 AS g0 WHERE TRUE = ALL (SELECT ol.e2 > 10 FROM PM1.G4 AS ol "
                + "WHERE g0.e2 = ol.e2) ORDER BY g0.e2");
    }

    @Test
    public void testAllRoot() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter="+Encoder.encode("G4_FKX/all(ol:ol/e2 gt length($root/G1(1)/e1))"),
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE TRUE = ALL (SELECT ol.e2 > LENGTH((SELECT g2.e1 FROM PM1.G1 AS g2 WHERE g2.e2 = 1)) FROM PM1.G4 AS ol WHERE g0.e2 = ol.e2) ORDER BY g0.e2");
    }

    @Test
    public void testAllAnd() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter="+Encoder.encode("G4_FKX/all(ol:ol/e2 gt 10 and ol/e1 eq 'b')"),
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE TRUE = ALL (SELECT (ol.e2 > 10) AND (ol.e1 = 'b') FROM PM1.G4 AS ol WHERE g0.e2 = ol.e2) ORDER BY g0.e2");
    }

    @Test
    public void testExpandSimple() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX",
                "SELECT g0.e1, g0.e2, g0.e3, /*+ MJ */ (SELECT ARRAY_AGG((g1.e1, g1.e2) ORDER BY g1.e1) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) FROM /*+ MAKEIND */ PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testExpandSimple_OneToOne() throws Exception {
        helpTest("/odata4/vdb/PM1/G2?$expand=FK0",
                "SELECT g0.e1, g0.e2, /*+ MJ */ (SELECT ARRAY_AGG((g1.e1, g1.e2, g1.e3) ORDER BY g1.e2) FROM PM1.G1 AS g1 WHERE g1.e2 = g0.e2) FROM /*+ MAKEIND */ PM1.G2 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testExpandSimpleWithSelect() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX&$select=e3",
                "SELECT g0.e2, g0.e3, /*+ MJ */ (SELECT ARRAY_AGG((g1.e1, g1.e2) ORDER BY g1.e1) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) FROM /*+ MAKEIND */ PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testExpandWithNestedSelect() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($select=e2)&$select=e3",
                "SELECT g0.e2, g0.e3, /*+ MJ */ (SELECT ARRAY_AGG((g1.e1, g1.e2) ORDER BY g1.e1) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) FROM /*+ MAKEIND */ PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testExpandFilter() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($filter=e2 eq 100)&$select=e3",
                "SELECT g0.e2, g0.e3, /*+ MJ */ (SELECT ARRAY_AGG((g1.e1, g1.e2) ORDER BY g1.e1) FROM PM1.G4 AS g1 WHERE (g0.e2 = g1.e2) AND (g1.e2 = 100)) FROM /*+ MAKEIND */ PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testExpandFilter2() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($filter=e2 eq 1)&$select=e3",
                "SELECT g0.e2, g0.e3, /*+ MJ */ (SELECT ARRAY_AGG((g1.e1, g1.e2) ORDER BY g1.e1) FROM PM1.G4 AS g1 WHERE (g0.e2 = g1.e2) AND (g1.e2 = 1)) FROM /*+ MAKEIND */ PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testExpandCompoundFilter() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($filter=e2 eq 100)&$select=e3&$filter=e2 ne 100",
                "SELECT g0.e2, g0.e3, /*+ MJ */ (SELECT ARRAY_AGG((g1.e1, g1.e2) ORDER BY g1.e1) FROM PM1.G4 AS g1 WHERE (g0.e2 = g1.e2) AND (g1.e2 = 100)) FROM /*+ MAKEIND */ PM1.G1 AS g0 WHERE g0.e2 <> 100 ORDER BY g0.e2");
    }

    @Test
    public void testExpandFilter$it() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($filter=$it/e2 eq e2)",
                "SELECT g0.e1, g0.e2, g0.e3, /*+ MJ */ (SELECT ARRAY_AGG((g1.e1, g1.e2) ORDER BY g1.e1) FROM PM1.G4 AS g1 WHERE (g0.e2 = g1.e2) AND (g0.e2 = g1.e2)) FROM /*+ MAKEIND */ PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testExpandFilter$itNested() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($expand=FKX($filter=$it/e3 eq e2))",
                "SELECT g0.e1, g0.e2, g0.e3, /*+ MJ */ (SELECT ARRAY_AGG((g1.e1, g1.e2, /*+ MJ */ (SELECT ARRAY_AGG((g2.e1, g2.e2, g2.e3) ORDER BY g2.e2) FROM PM1.G1 AS g2 WHERE (g2.e2 = g1.e2) AND (g0.e3 = g2.e2))) ORDER BY g1.e1) FROM /*+ MAKEIND */ PM1.G4 AS g1 WHERE g0.e2 = g1.e2) FROM /*+ MAKEIND */ PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testExpandOrderby() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$expand=G4_FKX($orderby=e1 desc)",
                "SELECT g0.e1, g0.e2, g0.e3, /*+ MJ */ (SELECT ARRAY_AGG((g1.e1, g1.e2) ORDER BY g1.e1 DESC NULLS LAST) FROM PM1.G4 AS g1 WHERE g0.e2 = g1.e2) FROM /*+ MAKEIND */ PM1.G1 AS g0 ORDER BY g0.e2");
    }

    @Test
    public void testSimpleCrossJoin() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1,G2)",
                "SELECT g0.e2, g1.e2 "
                + "FROM PM1.G1 AS g0, "
                + "PM1.G2 AS g1 "
                + "ORDER BY g0.e2, g1.e2");
    }

    @Test
    public void testSimpleCrossJoinWithSkip() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1,G2)?$skip=1",
                "SELECT g0.e2, g1.e2 "
                + "FROM PM1.G1 AS g0, "
                + "PM1.G2 AS g1 "
                + "ORDER BY g0.e2, g1.e2", 1, null, false);
    }

    @Test
    public void testSimpleCrossJoinWithTop() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1,G2)?$top=1",
                "SELECT g0.e2, g1.e2 "
                + "FROM PM1.G1 AS g0, "
                + "PM1.G2 AS g1 "
                + "ORDER BY g0.e2, g1.e2", null, 1, false);
    }

    @Test
    public void testSimpleCrossJoinWithCount() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1,G2)?$count=true",
                "SELECT g0.e2, g1.e2 "
                + "FROM PM1.G1 AS g0, "
                + "PM1.G2 AS g1 "
                + "ORDER BY g0.e2, g1.e2", null, null, true);
    }

    @Test
    public void testSimpleCrossJoinWithFilter() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1,G2)?$filter="+Encoder.encode("G1/e1 eq G2/e1"),
                "SELECT g0.e2, g1.e2 "
                + "FROM PM1.G1 AS g0, "
                + "PM1.G2 AS g1 "
                + "WHERE g0.e1 = g1.e1 "
                + "ORDER BY g0.e2, g1.e2");
    }

    @Test
    public void testSimpleCrossJoinWith$Orderby() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1,G2)?$orderby=G1/e1,G2/e2",
                "SELECT g0.e2, g1.e2 "
                + "FROM PM1.G1 AS g0, "
                + "PM1.G2 AS g1 "
                + "ORDER BY g0.e1 NULLS FIRST, g1.e2 NULLS FIRST");
    }

    @Ignore //OLINGO-904
    @Test
    public void testSimpleCrossJoinWith$expand() throws Exception {
        helpTest("/odata4/vdb/PM1/$crossjoin(G1,G2)?$expand=G1",
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
        helpTest("/odata4/vdb/PM1/G5?$filter=e1 eq '1'",
                "SELECT g0.e2, g0.e3 FROM PM1.G5 AS g0 WHERE g0.e1 = '1' ORDER BY g0.e2");
    }

    @Test
    public void testFilterOnNull() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq null",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 IS NULL ORDER BY g0.e2");
    }

    @Test
    public void testMultipleAirthamatic() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e2 eq 1 add 1 add 1",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e2 = ((1 + 1) + 1) ORDER BY g0.e2");
    }

    @Test
    public void testFloor() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$filter=e2 eq floor(4.2)",
                "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e2 = FLOOR(4.2) ORDER BY g0.e2");
    }

    @Test
    public void testMonthWithDatetimeoffser() throws Exception {
        helpTest("/odata4/vdb/PM1/SimpleTable?$filter="+Encoder.encode("month(2001-01-01T00:01:01.01Z) eq intkey"),
                "SELECT g0.intkey, g0.intnum, g0.stringkey, g0.stringval, g0.booleanval, g0.decimalval, "
                + "g0.timeval, g0.dateval, g0.timestampval, g0.clobval FROM "
                + "PM1.SimpleTable AS g0 WHERE MONTH({ts'2001-01-01 00:01:01.01'}) = g0.intkey ORDER BY g0.intkey");
    }

    @Test
    public void testDate() throws Exception {
        helpTest("/odata4/vdb/PM1/SimpleTable?$select=intkey&$filter="+Encoder.encode("date(now()) eq dateval"),
                "SELECT g0.intkey FROM PM1.SimpleTable AS g0 "
                + "WHERE CONVERT(NOW(), date) = g0.dateval ORDER BY g0.intkey");
    }

    @Test
    public void testTime() throws Exception {
        helpTest("/odata4/vdb/PM1/SimpleTable?$select=intkey&$filter="+
                Encoder.encode("timeval gt time(2001-01-01T00:01:01.01Z)"),
                "SELECT g0.intkey FROM PM1.SimpleTable AS g0 "
                + "WHERE g0.timeval > CONVERT({ts'2001-01-01 00:01:01.01'}, time) ORDER BY g0.intkey");
    }

    @BeforeClass public static void oneTimeSetup() {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT"));
    }

    @AfterClass public static void oneTimeTearDown() {
        TimestampWithTimezone.resetCalendar(null);
    }

    @Test
    public void testGeoDistance() throws Exception {
        helpTest("/odata4/vdb/PM1/geo?$filter="+
                Encoder.encode("geo.distance(location, geometry'SRID=4326;POINT(-127.89734578345 45.234534534)') gt 10"),
                "SELECT g0.id, g0.location, g0.line, g0.polygon FROM PM1.geo AS g0 WHERE st_distance(g0.location, st_geomfromewkt('SRID=4326;Point(-127.89734578345 45.234534534)')) > 10 ORDER BY g0.id");
    }

    @Test
    public void testGeoLength() throws Exception {
        helpTest("/odata4/vdb/PM1/geo?$filter="+
                Encoder.encode("geo.length(line) lt 10"),
                "SELECT g0.id, g0.location, g0.line, g0.polygon FROM PM1.geo AS g0 WHERE st_length(g0.line) < 10 ORDER BY g0.id");
    }

    @Test
    public void testGeoIntersects() throws Exception {
        helpTest("/odata4/vdb/PM1/geo?$filter="+
                Encoder.encode("geo.intersects(location, polygon)"),
                "SELECT g0.id, g0.location, g0.line, g0.polygon FROM PM1.geo AS g0 WHERE st_intersects(g0.location, g0.polygon) ORDER BY g0.id");
    }

    @Test
    public void testFilteredAggregateSum() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$apply=filter(e2%20gt%200)/aggregate(e3%20with%20sum%20as%20Total)",
                "SELECT * FROM (SELECT SUM(g0.e3) AS Total FROM PM1.G1 AS g0 WHERE g0.e2 > 0) AS g1");
    }

    @Test
    public void testTimestampPrecision() throws Exception {
        String ddl = "CREATE FOREIGN TABLE G1(\n" +
                "e1 timestamp primary key,\n" +
                "e3 timestamp(0),\n" +
                "e4 timestamp(1));";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "vdb", "PM1");
        MetadataStore store = metadata.getMetadataStore();
        org.teiid.metadata.Schema teiidSchema = store.getSchema("PM1");
        CsdlSchema schema = ODataSchemaBuilder.buildMetadata("vdb", teiidSchema);
        CsdlProperty property = schema.getEntityType("G1").getProperty("e1");
        assertNull(property.getPrecision());
        property = schema.getEntityType("G1").getProperty("e3");
        assertEquals(0, property.getPrecision().intValue());
        property = schema.getEntityType("G1").getProperty("e4");
        assertEquals(1, property.getPrecision().intValue());
    }

    @Test
    public void testSumAggregateFiltered() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$apply=aggregate(e3%20with%20sum%20as%20Total)/filter(Total%20gt%200)",
                "SELECT * FROM (SELECT SUM(g0.e3) AS Total FROM PM1.G1 AS g0) AS g1 WHERE g1.Total > 0");
    }

    @Test
    public void testGroupBy() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$apply=groupby((e1))",
                "SELECT * FROM (SELECT g0.e1 AS e1 FROM PM1.G1 AS g0 GROUP BY g0.e1) AS g1");
    }

    @Test
    public void testGroupBySumAggregate() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$apply=groupby((e1),aggregate(e3%20with%20sum%20as%20Total))",
                "SELECT * FROM (SELECT g0.e1 AS e1, SUM(g0.e3) AS Total FROM PM1.G1 AS g0 GROUP BY g0.e1) AS g1");
    }

    @Test
    public void testGroupBySelect() throws Exception {
        helpTest("/odata4/vdb/PM1/G1?$apply=groupby((e1,e2))&$select=e1",
                "SELECT g1.e1 FROM (SELECT g0.e1 AS e1, g0.e2 AS e2 FROM PM1.G1 AS g0 GROUP BY g0.e1, g0.e2) AS g1");
    }
}
