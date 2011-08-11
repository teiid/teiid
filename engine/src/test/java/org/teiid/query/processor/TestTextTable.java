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

package org.teiid.query.processor;

import static org.teiid.query.optimizer.TestOptimizer.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.serial.SerialClob;

import org.junit.Test;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings({"unchecked", "nls"})
public class TestTextTable {
    
	@Test public void testCorrelatedTextTable() throws Exception {
    	String sql = "select x.* from pm1.g1, texttable(e1 || ',' || e2 COLUMNS x string, y integer) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a", 0),
        		Arrays.asList("a", 3),
        		Arrays.asList("c", 1),
        		Arrays.asList("b", 2),
        		Arrays.asList("a", 0),
        };    

        process(sql, expected);
    }
	
	@Test public void testTextTableFixed() throws Exception {
    	String sql = "select max(compkey), max(cdm_id), max(currency), max(\"start\"), max(maturity), max(amount), count(*) from texttable(? COLUMNS compkey string width 76, CDM_ID string width 14, CURRENCY string width 9, \"START\" string width 31, MATURITY string width 31, AMOUNT double width 21, RECORDSOURCE string width 13, SUMMIT_ID string width 15, RATE double width 20, SPREAD double width 20, DESK string width 14) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("000369USD05/20/200405/20/2007", "000369", "USD", "12/18/2000", "12/19/2005", 6.7209685146E8, 52),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/cdm_dos.txt")));
    }
	
	@Test public void testTextTableFixedWin() throws Exception {
    	String sql = "select max(compkey), max(cdm_id), max(currency), max(\"start\"), max(maturity), max(amount), count(*) from texttable(? COLUMNS compkey string width 76, CDM_ID string width 14, CURRENCY string width 9, \"START\" string width 31, MATURITY string width 31, AMOUNT double width 21, RECORDSOURCE string width 13, SUMMIT_ID string width 15, RATE double width 20, SPREAD double width 20, DESK string width 14) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("000369USD05/20/200405/20/2007", "000369", "USD", "12/18/2000", "12/19/2005", 6.7209685146E8, 52),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/cdm_dos_win.txt")));
    }
	
	@Test public void testTextTableFixedPartial() throws Exception {
    	String sql = "select max(length(compkey)) from texttable(? COLUMNS compkey string width 76) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(30),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/cdm_dos.txt")));
    }
	
	@Test public void testNamedMultilineHeader() throws Exception {
    	String sql = "SELECT * from texttable(? COLUMNS Col3Head string HEADER) x";
    	
        List[] expected = new List[] {
        	Arrays.asList("VAL2"),
        	Arrays.asList("VAL4"),
        	Arrays.asList("abc"),
        	Arrays.asList("VAL9"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/test-file.txt.csv")));
    }
	
	@Test public void testHeaderWithSkip() throws Exception {
    	String sql = "select count(*) from texttable(? COLUMNS PARTNAME string HEADER 3 SKIP 5) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(21),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/TextParts_HeaderRow2.csv")));
    }
	
	@Test public void testEscape() throws Exception {
    	String sql = "select * from texttable('a\\,b,c\\\na\na,b\\\\' COLUMNS c1 string, c2 string ESCAPE '\\') x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a,b", "c\na"),
        		Arrays.asList("a", "b\\"),
        };    
    
        process(sql, expected);
    }
	
	@Test(expected=TeiidProcessingException.class) public void testEscapeError() throws Exception {
    	String sql = "select * from texttable('axq' COLUMNS c1 string ESCAPE 'x') x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {};    
        
        process(sql, expected);
    }

	@Test public void testDelimiter() throws Exception {
    	String sql = "select * from texttable('\na\\,b,c' COLUMNS c1 string, c2 string DELIMITER 'b') x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a\\,", ",c"),
        };    
    
        process(sql, expected);
    }
	
	@Test public void testNoRowDelimiter() throws Exception {
    	String sql = "select * from texttable('abcdef' COLUMNS c1 string width 1, c2 string width 1 no row delimiter) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a","b"),
        		Arrays.asList("c","d"),
        		Arrays.asList("e","f"),
        };    
    
        process(sql, expected);
    }
	
	@Test public void testNoTrim() throws Exception {
    	String sql = "select * from texttable('a b \nc  d' COLUMNS c1 string width 2, c2 string width 2 no trim) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a","b "),
        		Arrays.asList("c"," d"),
        };    
    
        process(sql, expected);
    }
	
	@Test public void testNoRows() throws Exception {
    	String sql = "select * from texttable('' COLUMNS c1 string, c2 string SKIP 3) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {};
        
        process(sql, expected);
    }
	
	@Test public void testQuote() throws Exception {
    	String sql = "select * from texttable('  \" a\", \" \"\" \"' COLUMNS c1 string, c2 string) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(" a", " \" ")
        };    
    
        process(sql, expected);
    }
	
	@Test(expected=TeiidProcessingException.class) public void testUnclosedQuoteError() throws Exception {
    	String sql = "select * from texttable('  \" a\", \" \"\"' COLUMNS c1 string, c2 string) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {};    
    
        process(sql, expected);
    }
	
	@Test(expected=TeiidProcessingException.class) public void testQuoteError() throws Exception {
    	String sql = "select * from texttable('  \" a\", x\" \"\" \"' COLUMNS c1 string, c2 string) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {};    
    
        process(sql, expected);
    }
	
	@Test(expected=TeiidProcessingException.class) public void testHeaderError() throws Exception {
    	String sql = "select * from texttable('notc1,notc2' COLUMNS c1 string, c2 string HEADER) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {};    
    
        process(sql, expected);
    }
	
	@Test public void testTextTableCriteria() throws Exception {
    	String sql = "select x.* from texttable('a' || ',' || '1' COLUMNS x string, y integer) x where x.y = 1"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a", 1),
        };    

        process(sql, expected);
    }
	
	@Test public void testTextTableGroupBy() throws Exception {
    	String sql = "select max(x) from texttable('a' || ',' || '1' COLUMNS x string, y integer) x group by y"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a"),
        };    

        process(sql, expected);
    }
	
	@Test public void testTextTableSubquery() throws Exception {
		String sql = "select x.* from pm1.g1, texttable(e1 || ',' || e2 COLUMNS x string, y integer) x where y < (select avg(e2) from pm1.g1 as x)";
    	
        List[] expected = new List[] {
        		Arrays.asList("a", 0),
        		Arrays.asList("c", 1),
        		Arrays.asList("a", 0),
        };    

        process(sql, expected);
    }

	@Test public void testTextTableMultiBatch() throws Exception {
		String sql = "select x.* from (select * from pm1.g1 where e1 = 'c') y, texttable(e1 || '\n' || e2 || '\n' || e3 COLUMNS x string) x";
    	
        List[] expected = new List[] {
        		Arrays.asList("c"),
        		Arrays.asList("1"),
        		Arrays.asList("true"),
        };    

        process(sql, expected);
    }   
	
	public static void process(String sql, List[] expectedResults) throws Exception {    
    	FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
    	ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        helpProcess(plan, createCommandContext(), dataManager, expectedResults);
    }
	
	public static ClobType clobFromFile(final String file) {
		return new ClobType(new ClobImpl(new InputStreamFactory.FileInputStreamFactory(UnitTestUtil.getTestDataFile(file)), -1));
	}
	
    @Test public void testTextAgg() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$        
        
        ProcessorPlan plan = helpPlan("select convert(to_chars(textagg(for pm1.g1.e1, pm1.g1.e2 header order by e2), 'UTF-8'), string) as x from pm1.g1", metadata,  null, capFinder, //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", new List[] {Arrays.asList("z", 2), Arrays.asList("b", 1)});
        hdm.setBlockOnce(true);
                
        String nl = System.getProperty("line.separator");
        ArrayList list = new ArrayList();
        list.add("\"e1\",\"e2\""+nl+"\"b\",\"1\""+nl+"\"z\",\"2\""+nl);
        List[] expected = new List[] {
        		list,
        };    

        helpProcess(plan, hdm, expected);    	
    }
    
    @Test public void testTextAggOrderByUnrelated() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$        
        
        ProcessorPlan plan = helpPlan("select convert(to_chars(textagg(for pm1.g1.e1 header order by e2), 'UTF-8'), string) as x from pm1.g1", metadata,  null, capFinder, //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", new List[] {Arrays.asList("z", 2), Arrays.asList("b", 1)});
        hdm.setBlockOnce(true);
                
        String nl = System.getProperty("line.separator");
        ArrayList list = new ArrayList();
        list.add("\"e1\""+nl+"\"b\""+nl+"\"z\""+nl);
        List[] expected = new List[] {
        		list,
        };    

        helpProcess(plan, hdm, expected);    	
    }
    
	@Test(expected=TeiidProcessingException.class) public void testTextTableInvalidData() throws Exception {
    	String sql = "select count(*) from texttable(? COLUMNS PARTNAME string) x"; //$NON-NLS-1$
    	
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        char[] data = new char[5000];
        processPreparedStatement(sql, null, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(new ClobType(new SerialClob(data))));
    }
	
}
