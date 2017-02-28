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
package org.teiid.translator.couchbase;

import static org.teiid.translator.couchbase.TestCouchbaseMetadataProcessor.*;
import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.CoreConstants;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.Command;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestCouchbaseExecutionFactory {
    
    private static TransformationMetadata queryMetadataInterface() {
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("couchbase");

            CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
            MetadataFactory mf = new MetadataFactory("couchbase", 1, SystemMetadata.getInstance().getRuntimeTypeMap(), mmd);
            mp.addTable(mf, KEYSPACE, formCustomer(), null);
            mp.addTable(mf, KEYSPACE, formOder(), null);
            mp.addTable(mf, KEYSPACE, formSimpleJson(), null);
            mp.addTable(mf, KEYSPACE, formJson(), null);
            mp.addTable(mf, KEYSPACE, formArray(), null);
            mp.addTable(mf, KEYSPACE, layerJson(), null);
            mp.addTable(mf, KEYSPACE, layerArray(), null);

            TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x");
            ValidatorReport report = new MetadataValidator().validate(tm.getVdbMetaData(), tm.getMetadataStore());
            if (report.hasItems()) {
                throw new RuntimeException(report.getFailureMessage());
            }
            return tm;
        } catch (MetadataException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static TranslationUtility translationUtility = new TranslationUtility(queryMetadataInterface());
    private static RuntimeMetadata runtimeMetadata = new RuntimeMetadataImpl(queryMetadataInterface());
    
    private static CouchbaseExecutionFactory TRANSLATOR;
    
    @BeforeClass
    public static void init() throws TranslatorException {
        TRANSLATOR = new CouchbaseExecutionFactory();
        TRANSLATOR.start();
        translationUtility.addUDF(CoreConstants.SYSTEM_MODEL, TRANSLATOR.getPushDownFunctions());
    }
    
    private void helpTest(String sql, String expected) throws TranslatorException {

        Command command = translationUtility.parseCommand(sql);

        N1QLVisitor vistor = TRANSLATOR.getN1QLVisitor(runtimeMetadata);
        vistor.append(command);

        System.out.println(vistor.toString());
        assertEquals(expected, vistor.toString());
    }
    

    @Test
    public void testBasicSelect() throws TranslatorException {
        
        String sql = "SELECT * FROM test_CreditCard AS T";
        helpTest(sql, "SELECT T.CardNumber, T.Type, T.CVN, T.Expiry FROM `test`.`CreditCard` AS T");
        
        sql = "SELECT * FROM test_CreditCard";
        helpTest(sql, "SELECT CardNumber, Type, CVN, Expiry FROM `test`.`CreditCard`");
        
        sql = "SELECT * FROM test_SavedAddresses";
        helpTest(sql, "SELECT SavedAddresses FROM `test`.`SavedAddresses`");
        
        sql = "SELECT * FROM test_SavedAddresses AS T";
        helpTest(sql, "SELECT T FROM `test`.`SavedAddresses` AS T");
    }
    
    @Test
    public void testNumbericFunctions() throws TranslatorException {
        
        String sql = "SELECT CEILING(attr_number_float) FROM test";
        helpTest(sql, "SELECT CEIL(TONUMBER(`test`.attr_number_float)) FROM `test`"); 
    }
    
    @Test
    public void testConversionFunctions() throws TranslatorException {
        
        String sql = "SELECT convert(attr_number_float, double) FROM test";
        helpTest(sql, "SELECT TONUMBER(`test`.attr_number_float) FROM `test`"); 
    }


}
