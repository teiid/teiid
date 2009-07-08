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

package com.metamatrix.connector.metadata.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.language.ICommand;
import org.teiid.connector.metadata.ObjectQueryProcessor;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.dqp.internal.datamgr.metadata.RuntimeMetadataImpl;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.metadata.runtime.FakeMetadataService;
import com.metamatrix.metadata.runtime.FakeQueryMetadata;

public class TestObjectQueryProcessor extends TestCase {

    private CommandBuilder commandBuilder;
    private static BufferedReader testFile;

    private static PrintWriter actualResultsFile;

    public TestObjectQueryProcessor(String name) {
        super(name);
    }

    private void runNextTest() throws Exception {
        FakeMetadataService metaService = new FakeMetadataService(TestObjectQueryProcessor.class.getResource("/PartsSupplier.vdb")); //$NON-NLS-1$

        String queryText = testFile.readLine();
        actualResultsFile.append(queryText);
        actualResultsFile.append(StringUtil.LINE_SEPARATOR);

        ObjectQuery query = new ObjectQuery(getRuntimeMetadata(), getCommand(queryText));
        Iterator results = new ObjectQueryProcessor(metaService.getMetadataObjectSource("PartsSupplier", "1")).process(query); //$NON-NLS-1$ //$NON-NLS-2$

        String nextLine = testFile.readLine();
        StringBuffer sb = new StringBuffer();
        while(nextLine != null && nextLine.trim().length()>0){
            sb.append(nextLine).append(StringUtil.LINE_SEPARATOR);
            nextLine = testFile.readLine();
        }
        
        checkResults(sb.toString(), dumpResults(results));
    }

    public static RuntimeMetadata getRuntimeMetadata() {
        return new RuntimeMetadataImpl(FakeQueryMetadata.getQueryMetadata());
    }

    private void checkResults(String expected, String actual) {
        actualResultsFile.append(actual);
        actualResultsFile.append(StringUtil.LINE_SEPARATOR);
        assertEquals(expected, actual);
    }

    public void testNameField() throws Exception {
        runNextTest();
    }

    public void testCardinalityField() throws Exception {
        runNextTest();
    }

    public void testSupportsUpdateField() throws Exception {
        runNextTest();
    }

    public void testColumns() throws Exception {
        runNextTest();
    }

    public void testTwoFields() throws Exception {
        runNextTest();
    }

    public void testModels() throws Exception {
        runNextTest();
    }

    public void testPathModels() throws Exception {
        runNextTest();
    }

    public void testUUIDModels() throws Exception {
        runNextTest();
    }

    public void testCriteria() throws Exception {
        runNextTest();
    }

    public void testMultipleCriteria() throws Exception {
        runNextTest();
    }

    public void testFkKeys() throws Exception {
        runNextTest();
    }

    public void testPkKeys() throws Exception {
        runNextTest();
    }

    public void testCaseCriteria1() throws Exception {
        runNextTest();
    }

    public void testCaseCriteria2() throws Exception {
        runNextTest();
    }

    public void testCaseCriteria3() throws Exception {
        runNextTest();
    }

    public void testCaseCriteria4() throws Exception {
        runNextTest();
    }

    public void testCaseCriteria5() throws Exception {
        runNextTest();
    }

    public void testCaseCriteria6() throws Exception {
        runNextTest();
    }

    public void testCaseCriteria7() throws Exception {
        runNextTest();
    }

    public void testCaseCriteria8() throws Exception {
        runNextTest();
    }

    public void testCaseCriteria9() throws Exception {
        runNextTest();
    }

    public void testCaseCriteria10() throws Exception {
        runNextTest();
    }

    public void testWildCardCriteria() throws Exception {
        runNextTest();
    }

    public static String dumpResults(Iterator rows){
        StringBuffer result = new StringBuffer();
        while(rows.hasNext()){
            List row = (List) rows.next();
            Iterator elementIterator = row.iterator();
            boolean firstElement = true;
            while(elementIterator.hasNext()){
                Object element = elementIterator.next();
                if (!firstElement){
                    result.append('\t');
                }
                result.append(element);
                firstElement = false;
            }
            result.append( StringUtil.LINE_SEPARATOR );
        }
        return result.toString();
    }

    private ICommand getCommand(String sql){
        return commandBuilder.getCommand(sql);
    }


    /*
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        commandBuilder = new CommandBuilder(FakeQueryMetadata.getQueryMetadata());
        if (testFile == null) {
            testFile = new BufferedReader(new FileReader(UnitTestUtil.getTestDataPath() + File.separator+"tests.txt")); //$NON-NLS-1$
        }
        if (actualResultsFile == null) {
            actualResultsFile = new PrintWriter(UnitTestUtil.getTestScratchFile("results.txt")); //$NON-NLS-1$
            actualResultsFile.write(""); //$NON-NLS-1$
        }
    }
}
