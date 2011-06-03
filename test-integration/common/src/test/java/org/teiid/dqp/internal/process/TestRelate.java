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
package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.jdbc.FakeServer;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.h2.H2ExecutionFactory;
@SuppressWarnings("nls")
public class TestRelate {

	private static boolean writeResults = false;
	private static boolean DEBUG = false;
	private static FakeServer server;
    
    @BeforeClass public static void oneTimeSetUp() throws Exception {
    	//DQPConfiguration config = new DQPConfiguration();
    	//config.setUserRequestSourceConcurrency(1);
    	server = new FakeServer();
    	JdbcDataSource h2ds = new JdbcDataSource();
    	h2ds.setURL("jdbc:h2:zip:src/test/resources/relate/test.zip!/test");
    	final DataSource ds = JdbcConnectionPool.create(h2ds);
		ExecutionFactory h2 = new H2ExecutionFactory();
		h2.start();
		ConnectorManagerRepository cmr = new ConnectorManagerRepository();
		ConnectorManager cm = new ConnectorManager("source", "bar") {
			@Override
			protected Object getConnectionFactory() throws TranslatorException {
				return ds;
			}
		};
		cm.setExecutionFactory(h2);
		cmr.addConnectorManager("source", cm);
		server.setConnectorManagerRepository(cmr);
		server.deployVDB("VehicleRentalsVDB", UnitTestUtil.getTestDataPath()+"/relate/VehicleRentalsVDB.vdb");
		if (DEBUG) {
	    	Logger logger = Logger.getLogger("org.teiid");
	    	logger.setLevel(Level.FINER);
	    	ConsoleHandler handler = new ConsoleHandler();
	    	handler.setLevel(Level.FINER);
	    	logger.addHandler(handler);
    	}
    }
    
	private void compareResults(SQLXML[] docs)
	throws SQLException, IOException {
		StackTraceElement ste = new Exception().getStackTrace()[1];
    	String testName = ste.getMethodName();
    	testName = "relate/" + testName; //$NON-NLS-1$
        File actual = new File(UnitTestUtil.getTestDataPath() + "/" +testName+".expected"); //$NON-NLS-1$ //$NON-NLS-2$
        if (writeResults) {
	        PrintWriter writer = new PrintWriter(actual);
			for (SQLXML xml : docs) {
				writer.write(xml.getString());
				writer.write('\n');
			}
			writer.close();
			return;
        }
        BufferedReader br = new BufferedReader(new FileReader(actual));
		for (int i = 0; i < docs.length; i++) {
			assertEquals(br.readLine(), docs[i].getString());
		}
	}

    @Test public void testCase3365() throws Exception{
    	SQLXML[] docs = relate(false, null, null, null, 
    			"rentals.rentsVehicle", "rentals.company", "rentals.vehicle", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.rentsVehicle.companyId", 
    			"rentals.rentsVehicle.vehicleId", "'*:rentals/*:rentsVehicle/@vehicleID'", "STRING", "rentals.vehicle.vehicleId", 
    			"VehicleRentalsDoc.rentalsDocumentWithLocation",
    			null, 
    			null, 
    			null, 
    			null, 
    			null, null, null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    @Test public void testCase3365_crit() throws Exception{
    	SQLXML[] docs = relate(false, null, null, null, 
    			"rentals.rentsVehicle", "rentals.company", "rentals.vehicle", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.rentsVehicle.companyId", 
    			"rentals.rentsVehicle.vehicleId", "'*:rentals/*:rentsVehicle/@vehicleID'", "STRING", "rentals.vehicle.vehicleId", 
    			"VehicleRentalsDoc.rentalsDocumentWithLocation",
    			null, 
    			"company.companyid = 'CID1'", 
    			null, 
    			null, 
    			null, null, null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    @Test public void testCase3365_critNestedSrc() throws Exception {
    	SQLXML[] docs = relate(false, null, null, null, 
    			"rentals.rentsVehicle", "rentals.company", "rentals.vehicle", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.rentsVehicle.companyId", 
    			"rentals.rentsVehicle.vehicleId", "'*:rentals/*:rentsVehicle/@vehicleID'", "STRING", "rentals.vehicle.vehicleId", 
    			"VehicleRentalsDoc.rentalsDocumentWithLocation",
    			null, 
    			"location = 'Pittsburgh'", 
    			null, 
    			null, 
    			null, null, null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    @Test public void testCase3365_critNestedSrcContext() throws Exception {
    	SQLXML[] docs = relate(false, null, null, null, 
    			"rentals.rentsVehicle", "rentals.company", "rentals.vehicle", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.rentsVehicle.companyId", 
    			"rentals.rentsVehicle.vehicleId", "'*:rentals/*:rentsVehicle/@vehicleID'", "STRING", "rentals.vehicle.vehicleId", 
    			"VehicleRentalsDoc.rentalsDocumentWithLocation",
    			null, 
    			null, 
    			null, 
    			null, 
    			null, "context(location, location) = 'Pittsburgh'", null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    @Test public void testCase3365_critNestedSrcCombinationContext() throws Exception {
    	SQLXML[] docs = relate(false, null, null, null, 
    			"rentals.rentsVehicle", "rentals.company", "rentals.vehicle", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.rentsVehicle.companyId", 
    			"rentals.rentsVehicle.vehicleId", "'*:rentals/*:rentsVehicle/@vehicleID'", "STRING", "rentals.vehicle.vehicleId", 
    			"VehicleRentalsDoc.rentalsDocumentWithLocation",
    			null, 
    			"location = 'Pittsburgh'", 
    			null, 
    			null, 
    			null, "context(location, location) = 'Pittsburgh'", null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    @Test public void testCase3365_critNestedTgt() throws Exception {
    	SQLXML[] docs = relate(false, null, null, null, 
    			"rentals.rentsVehicle", "rentals.company", "rentals.vehicle", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.rentsVehicle.companyId", 
    			"rentals.rentsVehicle.vehicleId", "'*:rentals/*:rentsVehicle/@vehicleID'", "STRING", "rentals.vehicle.vehicleId", 
    			"VehicleRentalsDoc.rentalsDocumentWithLocation",
    			null, 
    			null, 
    			"color = 'Black'", 
    			null, 
    			null, null, null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    @Test public void testCase3365_compoundCritNestedTgt() throws Exception {
    	SQLXML[] docs = relate(false, null, null, null, 
    			"rentals.rentsVehicle", "rentals.company", "rentals.vehicle", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.rentsVehicle.companyId", 
    			"rentals.rentsVehicle.vehicleId", "'*:rentals/*:rentsVehicle/@vehicleID'", "STRING", "rentals.vehicle.vehicleId", 
    			"VehicleRentalsDoc.rentalsDocumentWithLocation",
    			null, 
    			null, 
    			"color='Black' or color='Puce'", 
    			null, 
    			null, null, null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    @Test public void testSharesDoc() throws Exception {
    	SQLXML[] docs = relate(true, null, null, null, 
    			"rentals.hasSharingAgreement", "rentals.company", "rentals.company", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.hasSharingAgreement.primaryPartyID", 
    			"rentals.hasSharingAgreement.secondaryPartyID", "'*:rentals/*:hasSharingAgreement/@secondaryPartyID'", "STRING", "rentals.company.companyId", 
    			"VehicleRentalsDoc.companiesDocument",
    			null, 
    			null, 
    			null, 
    			null, 
    			null, null, null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    @Test public void testSharesDocWithCritTgt() throws Exception {
    	SQLXML[] docs = relate(true, null, null, null, 
    			"rentals.hasSharingAgreement", "rentals.company", "rentals.company", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.hasSharingAgreement.primaryPartyID", 
    			"rentals.hasSharingAgreement.secondaryPartyID", "'*:rentals/*:hasSharingAgreement/@secondaryPartyID'", "STRING", "rentals.company.companyId", 
    			"VehicleRentalsDoc.companiesDocument",
    			null, 
    			null, 
    			"rentals.company.name like 'B%'", 
    			null, 
    			null, null, null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    @Test public void testSharesDocWithCritTgtContext() throws Exception {
    	SQLXML[] docs = relate(true, null, null, null, 
    			"rentals.hasSharingAgreement", "rentals.company", "rentals.company", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.hasSharingAgreement.primaryPartyID", 
    			"rentals.hasSharingAgreement.secondaryPartyID", "'*:rentals/*:hasSharingAgreement/@secondaryPartyID'", "STRING", "rentals.company.companyId", 
    			"VehicleRentalsDoc.companiesDocument",
    			null, 
    			null, 
    			"rentals.company.name like 'B%'", 
    			"rentals.company.name like 'B%'", 
    			null, null, null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    @Test public void testSharesDocEspaceQuestion3() throws Exception {
    	SQLXML[] docs = relate(true, null, null, null, 
    			"rentals.hasSharingAgreement", "rentals.company", "rentals.company", 
    			"rentals.company.companyId", "'*:rentals/*:company/@companyID'", "STRING", "rentals.hasSharingAgreement.primaryPartyID", 
    			"rentals.hasSharingAgreement.secondaryPartyID", "'*:rentals/*:hasSharingAgreement/@secondaryPartyID'", "STRING", "rentals.company.companyId", 
    			"VehicleRentalsDoc.companiesDocument",
    			null, 
    			"company.locations.location = 'Pittsburgh'", 
    			null, 
    			null, 
    			null, null, null, 
    			null, null, null);
    	compareResults(docs);
    }
    
    public SQLXML[] relate(
    		//distinct is only meaningful for self-relationships
    		boolean distinct,
    		//select args to limit the xml projection
    		//there is an assumption that the source/relationship select must project key values
    		String relationshipSelect, String sourceSelect, String targetSelect,  
    		//relevant contexts, the same as expected by the legacy relate function
    		String relationshipContext, String sourceContext, String targetContext, 
    		//break down of the relationship predicates
    		String sourceKey, 
     		  String sourceKeyPath,
    		  String sourceKeyType,
    		  String sourceFKey,
    		String targetFKey, 
    		String targetFKeyPath,
    		  String targetFKeyType,
    		  String targetKey,
    		//target document
    		String xmlDocument,
    		//explicit/implicit relationship context criteria - should not use the context function 
    		String relationshipCriteria,
    		//relateSource/implicit/explicit source context criteria - should not use the context function
    		String relateSourceCriteria, 
    		//relateTarget criteria - should not use the context function
    		String relateTargetCriteria, 
    		//relateTarget context criteria - should not use the context function
    		String relateTargetContextCriteria,
    		//subcontext criteria, logically applied after the relate operation - should use context function(s), and should not be specified against a root context
    		String relationshipContextCriteria,
    		String sourceContextCriteria,
    		String targetContextCriteria,
    		//order bys
    		String relationshipOrderBy,
    		String sourceOrderBy,
    		String targetOrderBy
    		) throws Exception {
    	if (sourceSelect == null) {
    		sourceSelect = sourceContext + ".*";
    	}
    	if (relationshipSelect == null) {
    		relationshipSelect = relationshipContext + ".*";
    	}
    	if (targetSelect == null) {
    		targetSelect = targetContext + ".*";
    	}
    	Connection conn = server.createConnection("jdbc:teiid:VehicleRentalsVDB"); //$NON-NLS-1$
    	if (DEBUG) {
    		conn.createStatement().execute("SET SHOWPLAN DEBUG");
    	}
    	SQLXML[] result = new SQLXML[3];
    	//source query
    	String query = String.format("SELECT %s FROM %s WHERE CONTEXT(%s, %s) IN (SELECT %s FROM %s WHERE %s IN (SELECT %s FROM %s", sourceSelect, xmlDocument, sourceContext, sourceKey, sourceFKey, relationshipContext, targetFKey, targetKey, targetContext);
    	if (relateTargetCriteria != null) {
    		query += (" WHERE " + relateTargetCriteria);
    	}
    	query += "))";
    	if (relateSourceCriteria != null) {
    		query += String.format(" AND (CONTEXT(%s, %s) = null OR %s)", sourceContext, sourceKey, relateSourceCriteria);
    	}
    	if (relationshipCriteria != null) {
    		query += String.format(" AND CONTEXT(%s, %s) IN (SELECT %s FROM %s WHERE AND %s)", sourceContext, sourceKey, sourceFKey, relationshipContext, relationshipCriteria);
    	}
    	if (sourceContextCriteria != null) {
    		query += (" AND " + sourceContextCriteria);
    	}
    	if (sourceOrderBy != null) {
    		query += (" ORDER BY " + sourceOrderBy);
    	}
    	PreparedStatement sourcePs = conn.prepareStatement(query);
    	ResultSet sourceRs = sourcePs.executeQuery();
    	sourceRs.next();
    	SQLXML sourceXml = sourceRs.getSQLXML(1);
    	result[0] = sourceXml;
    	
    	Statement ddlStmt = conn.createStatement();
    	
    	ddlStmt.execute("CREATE LOCAL TEMPORARY TABLE #st_source (source_key STRING, PRIMARY KEY (source_key))");
    	//source key extraction
    	String sourceStagingQuery = String.format("INSERT INTO #st_source (source_key) SELECT DISTINCT source_key FROM XMLTABLE(%s PASSING cast(? AS xml) COLUMNS source_key %s PATH '.') x", sourceKeyPath, sourceKeyType);
    	PreparedStatement ps = conn.prepareStatement(sourceStagingQuery);
    	ps.setSQLXML(1, sourceXml);
    	ps.execute();
    	ps.close();
    	
    	//relationship query
    	String relQuery = String.format("SELECT %s FROM %s WHERE CONTEXT(%s, %s) IN /*+ DJ */ (SELECT source_key from #st_source) AND CONTEXT(%s, %s) IN (SELECT %s FROM %s", relationshipSelect, xmlDocument, relationshipContext, sourceFKey, relationshipContext, targetFKey, targetKey, targetContext);
    	if (relateTargetContextCriteria != null) {
    		relQuery += (" WHERE " + relateTargetContextCriteria);
    	}
    	relQuery += ")";
    	if (relationshipCriteria != null) {
    		relQuery += String.format(" AND (CONTEXT(%s, %s) = null OR %s)", relationshipContext, sourceFKey, relationshipCriteria);
    	}
    	if (relationshipContextCriteria != null) {
    		relQuery += (" AND " + relationshipContextCriteria);
    	}
    	if (relationshipOrderBy != null) {
    		relQuery += (" ORDER BY " + relationshipOrderBy);
    	}
    	PreparedStatement relStmt = conn.prepareStatement(relQuery);
    	ResultSet relRs = relStmt.executeQuery();
    	relRs.next();
    	SQLXML relXml = relRs.getSQLXML(1);
    	result[1] = relXml;
    	
    	ddlStmt.execute("CREATE LOCAL TEMPORARY TABLE #st_rel (target_key STRING, PRIMARY KEY (target_key))");
    	//target key extraction
    	String relStagingQuery = String.format("INSERT INTO #st_rel (target_key) SELECT DISTINCT target_key FROM XMLTABLE(%s PASSING cast(? AS xml) COLUMNS target_key %s PATH '.') x", targetFKeyPath, targetFKeyType);
    	PreparedStatement psRel = conn.prepareStatement(relStagingQuery);
    	psRel.setSQLXML(1, relXml);
    	psRel.execute();
    	psRel.close();
    	
    	//target query
    	String targetQuery = String.format("SELECT %s FROM %s WHERE CONTEXT(%s, %s) IN /*+ DJ */ (SELECT target_key FROM #st_rel", targetSelect, xmlDocument, targetContext, targetKey);
    	if (distinct && targetContext.equalsIgnoreCase(sourceContext)) {
    		targetQuery += " EXCEPT SELECT source_key FROM #st_source";
    	}
    	targetQuery += ")";
    	if (targetContextCriteria != null) {
    		targetQuery += (" AND " + targetContextCriteria);
    	}
    	if (targetOrderBy != null) {
    		targetQuery += (" ORDER BY " + targetOrderBy);
    	}
    	PreparedStatement targetStmt = conn.prepareStatement(targetQuery);
    	ResultSet taretRs = targetStmt.executeQuery();
    	taretRs.next();
    	SQLXML targetXml = taretRs.getSQLXML(1);
    	result[2] = targetXml;
    	
    	ddlStmt.execute("drop table #st_source");
    	ddlStmt.execute("drop table #st_rel");
    	
    	return result;
    }
    
}
