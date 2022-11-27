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

package org.teiid.translator.jdbc;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.metadata.FunctionMethod;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TranslationHelper {

    public static final String PARTS_VDB = "/PartsSupplierJDBC.vdb"; //$NON-NLS-1$
    public static final String BQT_VDB = "/bqt.vdb"; //$NON-NLS-1$

    public static Command helpTranslate(String vdbFileName, String sql) {
        return helpTranslate(vdbFileName, null, sql);
    }

    public static TranslationUtility getTranslationUtility(String vdbFileName) {
        TranslationUtility util = null;
        if (PARTS_VDB.equals(vdbFileName)) {
            util = new TranslationUtility("PartsSupplierJDBC.vdb", TranslationHelper.class.getResource(vdbFileName)); //$NON-NLS-1$
        } else if (BQT_VDB.equals(vdbFileName)){
            util = FakeTranslationFactory.getInstance().getBQTTranslationUtility();
        } else {
            try {
                util = new TranslationUtility(RealMetadataFactory.fromDDL(vdbFileName, "vdb", "test"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return util;
    }

    public static Command helpTranslate(String vdbFileName, List<FunctionMethod> pushdowns, String sql) {
        TranslationUtility util =  getTranslationUtility(vdbFileName);

        if (pushdowns != null) {
            util.addUDF(CoreConstants.SYSTEM_MODEL, pushdowns);
        }
        return util.parseCommand(sql);
    }

    public static TranslatedCommand helpTestVisitor(String vdb, String input, String expectedOutput, JDBCExecutionFactory translator) throws TranslatorException {
        // Convert from sql to objects
        Command obj = helpTranslate(vdb, translator.getPushDownFunctions(), input);

        return helpTestVisitor(expectedOutput, translator, obj);
    }

    public static TranslatedCommand helpTestVisitor(String expectedOutput, JDBCExecutionFactory translator, Command obj) throws TranslatorException {
        TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), translator);
        tc.translateCommand(obj);
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
        return tc;
    }

    public static String helpTestTempTable(JDBCExecutionFactory transaltor, boolean transactional) throws QueryMetadataException, TeiidComponentException {
        List<ColumnReference> cols = new ArrayList<ColumnReference>();
        cols.add(new ColumnReference(null, "COL1", RealMetadataFactory.exampleBQTCached().getElementID("BQT1.SMALLA.INTKEY"), TypeFacility.RUNTIME_TYPES.INTEGER));
        cols.add(new ColumnReference(null, "COL2", RealMetadataFactory.exampleBQTCached().getElementID("BQT1.SMALLA.STRINGKEY"), TypeFacility.RUNTIME_TYPES.STRING));
        return transaltor.getCreateTempTableSQL("foo", cols, transactional);
    }

}
