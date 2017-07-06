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
package org.teiid.translator.salesforce.execution.visitors;

import static org.junit.Assert.*;

import java.io.FileReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesForceMetadataProcessor;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.Util;
import org.teiid.translator.salesforce.execution.QueryExecutionImpl;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

@SuppressWarnings("nls")
public class TestVisitors {

    public static QueryMetadataInterface exampleSalesforce() { 
    	
    	try {
    		ModelMetaData mmd = new ModelMetaData();
    		mmd.setName("SalesforceModel");
    		MetadataFactory mf = new MetadataFactory("sf", 1, SystemMetadata.getInstance().getRuntimeTypeMap(), mmd);
    		mf.setParser(new QueryParser());
    		
    		//load the metadata as captured from 8.9 on 9/3/2014
    		mf.parse(new FileReader(UnitTestUtil.getTestDataFile("sf.ddl")));

    		SalesForceExecutionFactory factory = new SalesForceExecutionFactory();
    		factory.start();
    		for (FunctionMethod func : factory.getPushDownFunctions()) {
    			mf.addFunction(func);
    		}
    		
    		SalesForceMetadataProcessor.addProcedrues(mf);

            // Create Contacts group - which has different name in sources
            Table contactTable = RealMetadataFactory.createPhysicalGroup("Contacts", mf.getSchema()); //$NON-NLS-1$
            contactTable.setNameInSource("Contact"); //$NON-NLS-1$
            contactTable.setProperty("Supports Query", Boolean.TRUE.toString()); //$NON-NLS-1$
            // Create Contact Columns
            String[] elemNames = new String[] {
                "ContactID", "Name", "AccountId", "InitialContact", "LastTime"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            };
            String[] elemTypes = new String[] {  
                DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.DefaultDataTypes.TIME
            };
            
            List<Column> contactCols = RealMetadataFactory.createElements(contactTable, elemNames, elemTypes);
            // Set name in source on each column
            String[] contactNameInSource = new String[] {
               "id", "ContactName", "accountid", "InitialContact", "LastTime"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            };
            for(int i=0; i<2; i++) {
                Column obj = contactCols.get(i);
                obj.setNameInSource(contactNameInSource[i]);
            }
    		
            //add a procedure with a native query property
            List<ProcedureParameter> params = new LinkedList<ProcedureParameter>();
            params.add(RealMetadataFactory.createParameter("x", SPParameter.IN, TypeFacility.RUNTIME_NAMES.STRING));
    		Procedure nativeProc = RealMetadataFactory.createStoredProcedure("foo", mf.getSchema(), params);
            nativeProc.setProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, "search;select accountname from account where accountid = $1");
            nativeProc.setResultSet(RealMetadataFactory.createResultSet("rs", new String[] {"accountname"}, new String[] {TypeFacility.RUNTIME_NAMES.STRING}));
    		
            TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x");
            ValidatorReport report = new MetadataValidator().validate(tm.getVdbMetaData(), tm.getMetadataStore());
        	if (report.hasItems()) {
        		throw new RuntimeException(report.getFailureMessage());
        	}
        	return tm;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }    

	private static TranslationUtility translationUtility = new TranslationUtility(exampleSalesforce());

	@Test public void testOr() throws Exception {
		String sql = "select id from Account where Name = 'foo' or BillingStreet = 'bar'";
		helpTest(sql, "SELECT Account.Id FROM Account WHERE (Account.Name = 'foo') OR (Account.BillingStreet = 'bar')");
	}
	
	@Test public void testNot() throws Exception {
		String sql = "select Account.id, Account.Name, Account.Industry from Account where not (Name = 'foo' and BillingStreet = 'bar')"; //$NON-NLS-1$
		helpTest(sql, "SELECT Account.Id, Account.Name, Account.Industry FROM Account WHERE (Account.Name != 'foo') OR (Account.BillingStreet != 'bar')");
	}
	
	@Test public void testCountStar() throws Exception {
		String sql = "select count(*) from Account";
		helpTest(sql,"SELECT COUNT(Id) FROM Account");
	}
	
	@Test public void testNotLike() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select id from Account where Name not like '%foo' or BillingCity = 'bar'"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Account.Id FROM Account WHERE (NOT (Account.Name LIKE '%foo')) OR (Account.BillingCity = 'bar')", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}

	@Test public void testIN() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select id from Account where Industry IN (1,2,3)"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertFalse(visitor.hasOnlyIDCriteria());
		assertEquals("SELECT Account.Id FROM Account WHERE Account.Industry IN('1','2','3')", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}

	@Test public void testOnlyIDsIN() throws Exception {
		// this can resolve to a better performing retrieve call
		Select command = (Select)translationUtility.parseCommand("select id, name from Account where ID IN (1,2,3)"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertTrue(visitor.hasOnlyIdInCriteria());
		assertEquals("Account", visitor.getTableName());
		assertEquals("Account.Id, Account.Name", visitor.getRetrieveFieldList());
		assertEquals(Arrays.asList(new String[]{"1", "2", "3"}), visitor.getIdInCriteria());	
	}
	
	@Test public void testJoin() throws Exception {
		Select command = (Select)translationUtility.parseCommand("SELECT Account.Name, Contact.Name FROM Contact LEFT OUTER JOIN Account ON Account.Id = Contact.AccountId"); //$NON-NLS-1$
		SelectVisitor visitor = new JoinQueryVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Account.Name, Contact.Name FROM Contact", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testJoin2() throws Exception {
		Select command = (Select)translationUtility.parseCommand("SELECT Account.Name, Contact.Name FROM Account LEFT OUTER JOIN Contact ON Account.Id = Contact.AccountId"); //$NON-NLS-1$
		SelectVisitor visitor = new JoinQueryVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Account.Name, (SELECT Contact.Name FROM Contacts) FROM Account", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testJoin3() throws Exception {
		Select command = (Select)translationUtility.parseCommand("SELECT Contact.Name FROM Account LEFT OUTER JOIN Contact ON Account.Id = Contact.AccountId"); //$NON-NLS-1$
		SelectVisitor visitor = new JoinQueryVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT (SELECT Contact.Name FROM Contacts) FROM Account", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
    @Test public void testJoin4() throws Exception {
        Select command = (Select)translationUtility.parseCommand("SELECT Contact.Name FROM Account INNER JOIN Contact ON Account.Id = Contact.AccountId WHERE Contact.Name='foo' AND Account.Id=5"); //$NON-NLS-1$
        SelectVisitor visitor = new JoinQueryVisitor(translationUtility.createRuntimeMetadata());
        visitor.visit(command);
        assertEquals("SELECT Contact.Name FROM Contact WHERE ((Contact.Name = 'foo') AND (Account.Id = '5')) AND (Contact.AccountId != NULL)", visitor.getQuery().toString().trim()); //$NON-NLS-1$
    }
    
	@Test public void testInnerJoin() throws Exception {
		Select command = (Select)translationUtility.parseCommand("SELECT Account.Phone, Account.Name, Account.Type, Contact.LastName FROM Account inner join Contact on Account.Id = Contact.AccountId"); //$NON-NLS-1$
		SelectVisitor visitor = new JoinQueryVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Account.Phone, Account.Name, Account.Type, Contact.LastName FROM Contact WHERE Contact.AccountId != NULL", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testInnerJoin1() throws Exception {
		Select command = (Select)translationUtility.parseCommand("SELECT Account.Phone, Account.Name, Account.Type, Contact.LastName FROM Contact inner join Account on Account.Id = Contact.AccountId"); //$NON-NLS-1$
		SelectVisitor visitor = new JoinQueryVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Account.Phone, Account.Name, Account.Type, Contact.LastName FROM Contact WHERE Contact.AccountId != NULL", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testInnerJoin2() throws Exception {
		Select command = (Select)translationUtility.parseCommand("SELECT Account.Phone, Account.Name, Account.Type, Contact.LastName FROM Contact inner join Account on Contact.AccountId = Account.Id"); //$NON-NLS-1$
		SelectVisitor visitor = new JoinQueryVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Account.Phone, Account.Name, Account.Type, Contact.LastName FROM Contact WHERE Contact.AccountId != NULL", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testInWithNameInSourceDifferent() throws Exception {
		Select command = (Select)translationUtility.parseCommand("SELECT Contacts.Name FROM Contacts WHERE Contacts.Name in ('x', 'y')"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Contact.ContactName FROM Contact WHERE Contact.ContactName IN('x','y')", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testEqualsElement() throws Exception {
		Select command = (Select)translationUtility.parseCommand("SELECT Contact.Name FROM Contact WHERE Contact.Name = Contact.AccountId"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Contact.Name FROM Contact WHERE Contact.Name = Contact.AccountId", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testIsNull() throws Exception {
		Select command = (Select)translationUtility.parseCommand("SELECT Contact.Name FROM Contact WHERE Contact.Name is not null"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Contact.Name FROM Contact WHERE Contact.Name != NULL", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testIDCriteria() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select id, name from Account where id = 'bar'"); //$NON-NLS-1$
		SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
		Mockito.stub(sfc.retrieve("Account.Id, Account.Name", "Account", Arrays.asList("bar"))).toReturn(new SObject[] {null});
		QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, translationUtility.createRuntimeMetadata(), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
		qei.execute();
		Mockito.verify(sfc).retrieve("Account.Id, Account.Name", "Account", Arrays.asList("bar"));
	}
	
	@BeforeClass static public void oneTimeSetup() {
	    Util.resetTimeZone();
		TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-06:00"));
	}
	
	@AfterClass static public void oneTimeTearDown() {
	    Util.resetTimeZone();
	    TimestampWithTimezone.resetCalendar(null);
	}
	
	@Test public void testDateTimeFormating() throws Exception {
		String sql = "select id from clientbrowser where LastUpdate = {ts'2003-03-11 11:42:10.5'}";
		String source = "SELECT ClientBrowser.Id FROM ClientBrowser WHERE ClientBrowser.LastUpdate = 2003-03-11T11:42:10.500-06:00";
		helpTest(sql, source);
	}

	@Test public void testDateTimeFormating1() throws Exception {
		String sql = "select id from clientbrowser where LastUpdate in ({ts'2003-03-11 11:42:10.506'}, {ts'2003-03-11 11:42:10.8088'})";
		String source = "SELECT ClientBrowser.Id FROM ClientBrowser WHERE ClientBrowser.LastUpdate IN(2003-03-11T11:42:10.506-06:00,2003-03-11T11:42:10.80-06:00)";
		helpTest(sql, source);
	}
	
	@Test public void testTimeFormatting() throws Exception {
		String sql = "select name from BusinessHours where SundayStartTime = {t'11:42:10'}";
		String source = "SELECT BusinessHours.Name FROM BusinessHours WHERE BusinessHours.SundayStartTime = 11:42:10.000-06:00";
		helpTest(sql, source);
	}
	
	@Test public void testAggregateSelect() throws Exception {
		String sql = "select max(name), count(1) from contact";
		String source = "SELECT MAX(Contact.Name), COUNT(Id) FROM Contact";
		helpTest(sql, source);
	}
	
	@Test public void testAggregateGroupByHaving() throws Exception {
		String sql = "select max(name), EmailBouncedDate from contact group by EmailBouncedDate having min(LastCUUpdateDate) in ({ts'2003-03-11 11:42:10.506'}, {ts'2003-03-11 11:42:10.8088'})";
		String source = "SELECT MAX(Contact.Name), Contact.EmailBouncedDate FROM Contact GROUP BY Contact.EmailBouncedDate HAVING MIN(Contact.LastCUUpdateDate) IN(2003-03-11T11:42:10.506-06:00,2003-03-11T11:42:10.80-06:00)";
		helpTest(sql, source);
	}
	
	@Test public void testPluralChild() throws Exception {
		String sql = "select Opportunity.CloseDate as Opportunity_CloseDate1 from Campaign Campaign LEFT OUTER JOIN Opportunity Opportunity ON Campaign.Id = Opportunity.CampaignId";
		String source = "SELECT (SELECT Opportunity.CloseDate FROM Opportunities) FROM Campaign";
		helpTest(sql, source);
	}
	
	@Test public void testParentName() throws Exception {
		String sql = "select Product2Feed.ParentId as Product2Feed_ParentId, Product2.Description as Product2_Description, Product2Feed.Title as Product2Feed_Title from SalesForceModel.Product2Feed Product2Feed LEFT OUTER JOIN SalesForceModel.Product2 Product2 ON Product2Feed.ParentId = Product2.Id";
		String source = "SELECT Product2Feed.ParentId, Parent.Description, Product2Feed.Title FROM Product2Feed";
		helpTest(sql, source);
	}

	private void helpTest(String sql, String expected) throws Exception {
		Command command = translationUtility.parseCommand(sql); 
		SalesForceExecutionFactory factory = new SalesForceExecutionFactory();
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);
		
        ArgumentCaptor<String> queryArgument = ArgumentCaptor.forClass(String.class);
        QueryResult qr = Mockito.mock(QueryResult.class);
        Mockito.stub(connection.query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean())).toReturn(qr);
        
		Execution execution = factory.createExecution(command, ec, rm, connection);
		execution.execute();

		Mockito.verify(connection, Mockito.times(1)).query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean());
		
		assertEquals(expected, queryArgument.getValue().trim());
	}
	
	@Test public void testNativeProc() throws Exception {
		String sql = "exec foo('1')";
		String source = "select accountname from account where accountid = '1'";
		helpTest(sql, source);
	}
	
	@Test public void testPluralNameFromKey() throws Exception {
		String sql = "SELECT CaseSolution.SolutionId, Case_.Origin FROM Case_ LEFT OUTER JOIN CaseSolution ON Case_.Id = CaseSolution.CaseId";
		String source = "SELECT Case.Origin, (SELECT CaseSolution.SolutionId FROM CaseSolutions) FROM Case";
		helpTest(sql, source);
		
	}
	
	@Test public void testInMulti() throws Exception {
		String sql = "select id from idea where categories in ('a', 'b')";
		String source = "SELECT Idea.Id FROM Idea WHERE Categories includes('a','b')";
		helpTest(sql, source);
		
		sql = "select id from idea where categories not in ('a', 'b')";
		source = "SELECT Idea.Id FROM Idea WHERE Categories EXCLUDES('a','b')";
		helpTest(sql, source);	
	}
	
	@Test public void testIncludExclude() throws Exception {
		String sql = "select id from idea where includes(categories, 'a,b')";
		String source = "SELECT Idea.Id FROM Idea WHERE Categories includes('a','b')";
		helpTest(sql, source);
		
		sql = "select id from idea where excludes(categories, 'a')";
		source = "SELECT Idea.Id FROM Idea WHERE Categories EXCLUDES('a')";
		helpTest(sql, source);	
	}
	
	@Test public void testFloatingLiteral() throws Exception {
		String sql = "SELECT COUNT(*) FROM Opportunity where amount > 100000000";
		String source = "SELECT COUNT(Id) FROM Opportunity WHERE Opportunity.Amount > 100000000";
		helpTest(sql, source);
	}
	
	@Test public void testCustomJoin() throws Exception {
        String sql = "select a.id, a.name, b.Order_Recipe_Steps__c, b.name from Media_Prep_Order_Recipe_Step__c a "
                + "inner join Recipe_Step_Detail__c b on (a.id = b.Order_Recipe_Steps__c)"
                + " where b.name = 'abc'";
        String source = "SELECT Order_Recipe_Steps__r.Id, Order_Recipe_Steps__r.Name, Recipe_Step_Detail__c.Order_Recipe_Steps__c, Recipe_Step_Detail__c.Name FROM Recipe_Step_Detail__c WHERE (Recipe_Step_Detail__c.Name = 'abc') AND (Recipe_Step_Detail__c.Order_Recipe_Steps__c != NULL)";
        helpTest(sql, source);
    }   
	
}
