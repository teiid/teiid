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
package org.teiid.translator.solr;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URLDecoder;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestTeiidLanguageToSolr {

    private TransformationMetadata metadata;
    private SolrExecutionFactory translator;
    private TranslationUtility utility;

    private QueryMetadataInterface setUp(String ddl, String vdbName,
            String modelName) throws Exception {

        this.translator = new SolrExecutionFactory();
        this.translator.start();

        metadata = RealMetadataFactory.fromDDL(ddl, vdbName, modelName);
        this.utility = new TranslationUtility(metadata);

        return metadata;
    }

    private String getSolrTranslation(String sql) throws IOException, Exception {
        Select select = (Select) getCommand(sql);
        SolrSQLHierarchyVistor visitor = new SolrSQLHierarchyVistor(this.utility.createRuntimeMetadata(), this.translator);
        visitor.visit(select);
        String cmd =  visitor.getSolrQuery().toString();
        return URLDecoder.decode(cmd, "UTF-8");
    }

    public Command getCommand(String sql) throws IOException, Exception {
        CommandBuilder builder = new CommandBuilder(setUp(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("exampleTBL.ddl")), "exampleVDB","exampleModel"));
        return builder.getCommand(sql);

    }

    @Test
    public void testSelectStar() throws Exception {
        // column test, all columns translates to price, weight and popularity
        assertEquals("fl=price,weight,popularity,name,purchasets,purchasetime,purchasedate,nis&q=*:*", getSolrTranslation("select * from example"));
    }

    @Test
    public void testSelectColumn() throws Exception {
        assertEquals("fl=price,weight&q=*:*", getSolrTranslation("select price,weight from example"));
    }

    @Test
    public void testSelectWhereEQ() throws Exception {
        assertEquals("fl=price,weight,popularity&q=price:1.0",
                getSolrTranslation("select price,weight,popularity from example where price=1"));
    }

    @Test
    public void testSelectWhereEQNegitive() throws Exception {
        assertEquals("fl=price,weight,popularity&q=price:\\-1.0",
                getSolrTranslation("select price,weight,popularity from example where price=-1"));
    }

    @Test
    public void testSelectWhereNE() throws Exception {
        assertEquals("fl=price,weight,popularity&q=NOT price:1.0",
                getSolrTranslation("select price,weight,popularity from example where price!=1"));
    }

    @Test
    public void testSelectWhereNEString() throws Exception {
        assertEquals("fl=price,weight,popularity&q=NOT name:test",
                getSolrTranslation("select price,weight,popularity from example where name!='test'"));
    }

    // only need to preform LT bc SOLR does not handle strict <,> only <=,>=
    @Test
    public void testSelectWhereGT() throws Exception {
        assertEquals("fl=price,weight,popularity&q=price:[1.0 TO *] AND NOT price:1.0",
                getSolrTranslation("select price,weight,popularity from example where price>1"));
    }

    // only need to preform LT bc SOLR does not handle strict <,> only <=,>=
    @Test
    public void testSelectWhereGE() throws Exception {
        assertEquals("fl=price,weight,popularity&q=price:[1.0 TO *]",
                getSolrTranslation("select price,weight,popularity from example where price>=1"));
    }

    // only need to preform LT bc SOLR does not handle strict <,> only <=,>=
    @Test
    public void testSelectWhereLT() throws Exception {
        assertEquals("fl=price,weight,popularity&q=price:[* TO 1.0] AND NOT price:1.0",
                getSolrTranslation("select price,weight,popularity from example where price<1"));
    }

    // only need to preform LT bc SOLR does not handle strict <,> only <=,>=
    @Test
    public void testSelectWhereLE() throws Exception {
        assertEquals("fl=price,weight,popularity&q=price:[* TO 1.0]",
                getSolrTranslation("select price,weight,popularity from example where price<=1"));
    }

    @Test
    public void testSelectWhereNEQ() throws Exception {
        assertEquals("fl=price,weight,popularity&q=NOT price:1.0",
                getSolrTranslation("select price,weight,popularity from example where price!=1"));
    }

    @Test
    public void testSelectWhenOr() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((price:1.0) OR (weight:[5.0 TO *]))",
                getSolrTranslation("select price,weight,popularity from example where price=1 or weight >=5"));
    }

    @Test
    public void testSelectWhenNotOr() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((NOT price:1.0) AND (weight:[* TO 5.0]))",
                getSolrTranslation("select price,weight,popularity from example where Not (price=1 or weight >5)"));
    }

    @Test
    public void testSelectWhenNotOrString() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((NOT name:*sung) AND (weight:[* TO 5.0]))",
                getSolrTranslation("select price,weight,popularity from example where Not (name like '%sung' or weight >5)"));
    }

    @Test
    public void testSelectWhenAnd() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((price:1.0) AND (weight:[5.0 TO *]))",
                getSolrTranslation("select price,weight,popularity from example where price=1 AND weight >=5"));
    }

    @Test
    public void testSelectWhenAndOr() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((((weight:[5.0 TO *] AND NOT weight:5.0) AND (price:1.0))) OR (weight:[5.0 TO *] AND NOT weight:5.0))",
                getSolrTranslation("select price,weight,popularity from example where weight > 5 AND price=1 or weight > 5"));
    }

    @Test
    public void testSelectWhenAndOrOr() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((((weight:[5.0 TO *] AND NOT weight:5.0) AND (price:1.0))) OR (((weight:[5.0 TO *] AND NOT weight:5.0) OR (popularity:[* TO 1] AND NOT popularity:1))))",
                getSolrTranslation("select price,weight,popularity from example where weight > 5 AND price=1 or weight > 5 or popularity < 1"));
    }

    @Test
    public void testSelectWhenLike() throws Exception {
        assertEquals("fl=price,weight,popularity&q=name:*sung",
                getSolrTranslation("select price,weight,popularity from example where name like '%sung'"));
    }

    @Test
    public void testSelectWhenLikeAnd() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((name:*sung) AND (price:1.0))",
                getSolrTranslation("select price,weight,popularity from example where name like '%sung' and price=1"));
    }

    @Test
    public void testSelectWhenOrLikeAnd() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((popularity:[* TO 1]) OR (((name:*sung) AND (price:1.0))))",
                getSolrTranslation("select price,weight,popularity from example where popularity <= 1 Or name like '%sung' and price=1"));
    }

    @Test
    public void testSelectWhenOrNotLikeAnd() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((popularity:[* TO 1]) OR (((NOT name:*sung) AND (price:1.0))))",
                getSolrTranslation("select price,weight,popularity from example where popularity <= 1 Or name not like '%sung' and price=1"));
    }

    @Test
    public void testSelectWhenInString() throws Exception {
        assertEquals("fl=price,weight,popularity&q=name:(3 OR 2 OR 1)",
                getSolrTranslation("select price,weight,popularity from example where name in ('1','2','3')"));
    }

    @Test
    public void testSelectWhenIn() throws Exception {
        assertEquals("fl=price,weight,popularity&q=popularity:(3 OR 2 OR 1)",
                getSolrTranslation("select price,weight,popularity from example where popularity in (1,2,3)"));
    }

    @Test
    public void testSelectWhenOrInAnd() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((weight:1.0) OR (((popularity:(3 OR 2 OR 1)) AND (price:1.0))))",
                getSolrTranslation("select price,weight,popularity from example where weight = 1 or popularity in (1,2,3) and price = 1"));
    }

    @Test
    public void testSelectWhenNotIn() throws Exception {
        assertEquals("fl=price,weight,popularity&q=NOT popularity:(3 OR 2 OR 1)",
                getSolrTranslation("select price,weight,popularity from example where  popularity not in (1,2,3)"));
    }

    @Test
    public void testSelectWhenNotAnd() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((weight:1.0) AND (NOT popularity:(3 OR 2 OR 1)))",
                getSolrTranslation("select price,weight,popularity from example where weight = 1 AND not popularity in (1,2,3)"));
    }

    @Test
    public void testSelectWhenAndNotAndAndLike() throws Exception {
        assertEquals("fl=price,weight,popularity&q=((weight:1.0) AND (((NOT popularity:(3 OR 2 OR 1)) OR (NOT name:*sung))))",
                getSolrTranslation("select price,weight,popularity from example where weight = 1 AND not (popularity in (1,2,3) and name like '%sung')"));
    }

    @Test
    public void testLimit() throws Exception {
        assertEquals("fl=name,popularity&rows=21&start=2&q=*:*",
                getSolrTranslation("select name,popularity from example limit 2, 21"));
    }

    @Test
    public void testOrderBy() throws Exception {
        assertEquals("fl=name,popularity&sort=popularity asc&q=*:*",
                getSolrTranslation("select name,popularity from example order by popularity ASC"));
    }

    @Test
    public void testOrderByWithAlias() throws Exception {
        assertEquals("fl=name,popularity&sort=popularity asc&q=*:*",
                getSolrTranslation("select name as c_0,popularity c_1 from example order by c_1 ASC"));
    }


    @Test
    public void testTimestampField() throws Exception {
        Date d = getTestDate();
        assertEquals("fl=name,purchasedate&q=purchasets:2014\\-02\\-06T19\\:52\\:07\\:000Z",
                getSolrTranslation("select name,purchasedate from example where purchasets = {ts '"+new Timestamp(d.getTime())+"'}"));
    }

    @Test
    public void testDateField() throws Exception {
        Date d = getTestDate();
        assertEquals("fl=name,purchasedate&q=purchasedate:2014\\-02\\-06T08\\:00\\:00\\:000Z",
                getSolrTranslation("select name,purchasedate from example where purchasedate = {d '"+new java.sql.Date(d.getTime())+"'}"));
    }

    @Test
    public void testTimeField() throws Exception {
        Date d = getTestDate();
        assertEquals("fl=name,purchasedate&q=purchasetime:1970\\-01\\-01T19\\:52\\:07\\:000Z",
                getSolrTranslation("select name,purchasedate from example where purchasetime = {t '"+new Time(d.getTime())+"'}"));
    }

    private Date getTestDate() {
        Calendar c = TimestampWithTimezone.getCalendar();
        c.setTimeInMillis(0);
        c.set(2014, 1, 6, 11, 52, 07);
        return c.getTime();
    }

    @Test
    public void testFunction() throws Exception {
        assertEquals("fl=name,sum(popularity,1)&sort=popularity asc&q=*:*",
                getSolrTranslation("select name,popularity+1 from example order by popularity ASC"));
    }

    @Test
    public void testNestedFunction() throws Exception {
        assertEquals("fl=name,div(sum(popularity,1),2)&sort=popularity asc&q=*:*",
                getSolrTranslation("select name,(popularity+1)/2 as x from example order by popularity ASC"));
    }

    @Before public void setUp() {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("PST")); //$NON-NLS-1$
    }

    @After public void tearDown() {
        TimestampWithTimezone.resetCalendar(null);
    }

}
