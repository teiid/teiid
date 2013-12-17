package org.jboss.teiid.translator.solr;

import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.solr.SolrExecutionFactory;
import org.teiid.translator.solr.execution.SolrQueryExecution;
import org.teiid.translator.solr.execution.SolrSQLHierarchyVistor;
import org.teiid.cdk.api.TranslationUtility;

/**
 * @author student
 *
 */
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
		SolrSQLHierarchyVistor visitor = new SolrSQLHierarchyVistor();
		visitor.visit(select);
		return visitor.getTranslatedSQL();

	}

	public Command getCommand(String sql) throws IOException, Exception {

		CommandBuilder builder = new CommandBuilder(setUp(
				ObjectConverterUtil.convertFileToString(UnitTestUtil
						.getTestDataFile("exampleTBL.ddl")), "exampleVDB",
				"exampleModel"));
		return builder.getCommand(sql);

	}

	@Test
	public void testSelectStar() throws Exception {

		// column test, all columns translates to price, weight and popularity
		Assert.assertEquals(getSolrTranslation("select * from example"), "*:*");

	}

	@Test
	public void testSelectColumn() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example"),
				"*:*");
	}

	@Test
	public void testSelectFrom() throws Exception {
	}

	@Test
	public void testSelectFromJoin() throws Exception {
	}

	@Test
	public void testSelectWhereEQ() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where price=1"),
				"price:1.0");
	}

	@Test
	public void testSelectWhereNE() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where price!=1"),
				"NOT price:1.0");
	}
	
	@Test
	public void testSelectWhereNEString() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where name!='test'"),
				"NOT name:'test'");
	}
	
	// only need to preform LT bc SOLR does not handle strict <,> only <=,>=
	@Test
	public void testSelectWhereGT() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where price>1"),
				"price:[1.0 TO *]");
	}

	// only need to preform LT bc SOLR does not handle strict <,> only <=,>=
	@Test
	public void testSelectWhereGE() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where price>=1"),
				"price:[1.0 TO *]");
	}

	// only need to preform LT bc SOLR does not handle strict <,> only <=,>=
	@Test
	public void testSelectWhereLT() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where price<1"),
				"price:[* TO 1.0]");
	}

	// only need to preform LT bc SOLR does not handle strict <,> only <=,>=
	@Test
	public void testSelectWhereLE() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where price<=1"),
				"price:[* TO 1.0]");
	}

	@Test
	public void testSelectWhereNEQ() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where price!=1"),
				"NOT price:1.0");
	}

	@Test
	public void testSelectWhenOr() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where price=1 or weight >5"),
				"((price:1.0) OR (weight:[5.0 TO *]))");
	}
@Test
	public void testSelectWhenNotOr() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where Not (price=1 or weight >5)"),
				"((NOT price:1.0) AND (weight:[* TO 5.0]))");
	}	
@Test
public void testSelectWhenNotOrString() throws Exception {
	Assert.assertEquals(
			getSolrTranslation("select price,weight,popularity from example where Not (name like '%sung' or weight >5)"),
			"((NOT name:*sung) AND (weight:[* TO 5.0]))");
}
	@Test
	public void testSelectWhenAnd() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where price=1 AND weight >5"),
				"((price:1.0) AND (weight:[5.0 TO *]))");
	}

	@Test
	public void testSelectWhenAndOr() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where weight > 5 AND price=1 or weight > 5"),
				"((((weight:[5.0 TO *]) AND (price:1.0))) OR (weight:[5.0 TO *]))");
	}

	@Test
	public void testSelectWhenAndOrOr() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where weight > 5 AND price=1 or weight > 5 or popularity < 1"),
				"((((weight:[5.0 TO *]) AND (price:1.0))) OR (((weight:[5.0 TO *]) OR (popularity:[* TO 1]))))");
	}

	@Test
	public void testSelectWhenLike() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where name like '%sung'"),
				"name:*sung");
	}

	@Test
	public void testSelectWhenLikeAnd() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where name like '%sung' and price=1"),
				"((name:*sung) AND (price:1.0))");
	}

	@Test
	public void testSelectWhenOrLikeAnd() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where popularity < 1 Or name like '%sung' and price=1"),
				"((popularity:[* TO 1]) OR (((name:*sung) AND (price:1.0))))");
	}
	
	@Test
	public void testSelectWhenOrNotLikeAnd() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where popularity < 1 Or name not like '%sung' and price=1"),
				"((popularity:[* TO 1]) OR (((NOT name:*sung) AND (price:1.0))))");
	}
	
	@Test
	public void testSelectWhenInString() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where name in ('1','2','3')"),
				"name:('1' OR '2' OR '3')");
	}
	
	@Test
	public void testSelectWhenIn() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where popularity in (1,2,3)"),
				"popularity:(1 OR 2 OR 3)");
	}
	
	@Test
	public void testSelectWhenOrInAnd() throws Exception {
		Assert.assertEquals(
				getSolrTranslation("select price,weight,popularity from example where weight = 1 or popularity in (1,2,3) and price = 1"),
				"((weight:1.0) OR (((popularity:(1 OR 2 OR 3)) AND (price:1.0))))");
	}
		
	 @Test
	 public void testSelectWhenNotIn() throws Exception {
			Assert.assertEquals(
					getSolrTranslation("select price,weight,popularity from example where  popularity not in (1,2,3)"),
					"NOT popularity:(1 OR 2 OR 3)");
	 }
	 @Test
	 public void testSelectWhenNotAnd() throws Exception {
			Assert.assertEquals(
					getSolrTranslation("select price,weight,popularity from example where weight = 1 AND not popularity in (1,2,3)"),
					"((weight:1.0) AND (NOT popularity:(1 OR 2 OR 3)))");
	 }
	 @Test
	 public void testSelectWhenAndNotAndAndLike() throws Exception {
			Assert.assertEquals(
					getSolrTranslation("select price,weight,popularity from example where weight = 1 AND not (popularity in (1,2,3) and name like '%sung')"),
					"((weight:1.0) AND (((NOT popularity:(1 OR 2 OR 3)) OR (NOT name:*sung))))");
	 }

	/* TODO testSelectGroupBy
	 * @Test
	 public void testSelectGroupBy() throws Exception {
	 }*/
	
	/* TODO testSelectWhenOrderBy
	 * @Test
	 public void testSelectWhenOrderBy() throws Exception {
	 }*/
	

}
