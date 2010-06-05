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

import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.unittest.FakeMetadataFactory;

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
    	String sql = "select count(*) from texttable(? COLUMNS compkey string width 77, CDM_ID string width 14, CURRENCY string width 9, \"START\" string width 31, MATURITY string width 38, AMOUNT double width 13, RECORDSOURCE string width 13, SUMMIT_ID string width 25, RATE double width 21, SPREAD double width 9, DESK string width 13) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(52),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), FakeMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/cdm_dos.txt")));
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
        
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), FakeMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/test-file.txt.csv")));
    }
	
	@Test public void testHeaderWithSkip() throws Exception {
    	String sql = "select count(*) from texttable(? COLUMNS PARTNAME string HEADER 3 SKIP 5) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(21),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), FakeMetadataFactory.example1Cached(), Arrays.asList(clobFromFile("text/TextParts_HeaderRow2.csv")));
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

    public static void process(String sql, List[] expectedResults) throws Exception {    
    	FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
    	ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        helpProcess(plan, createCommandContext(), dataManager, expectedResults);
    }
	
	public ClobType clobFromFile(final String file) {
		return new ClobType(new ClobImpl(new InputStreamFactory.FileInputStreamFactory(UnitTestUtil.getTestDataFile(file)), -1));
	}
	
}
