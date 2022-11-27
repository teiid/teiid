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

package org.teiid.connector.metadata.runtime;

import java.sql.Timestamp;
import java.util.List;

import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.ProcedureParameter;

import junit.framework.TestCase;



/**
 * @since 4.3
 */
@SuppressWarnings("nls")
public class TestParams extends TestCase {

    private static TranslationUtility CONNECTOR_METADATA_UTILITY = createTranslationUtility(getTestVDBName());

    /**
     * Constructor for TestParams.
     * @param name
     */
    public TestParams(String name) {
        super(name);
    }

    private static String getTestVDBName() {
        return UnitTestUtil.getTestDataPath() + "/sptest/spvdb.vdb"; //$NON-NLS-1$
    }

    public static TranslationUtility createTranslationUtility(String vdbName) {
        return new TranslationUtility(vdbName);
    }

    public Call getProcedure(String procName, int inputArgs, TranslationUtility transUtil) throws Exception {
        StringBuffer sql = new StringBuffer("EXEC " + procName + "("); //$NON-NLS-1$ //$NON-NLS-2$
        if(inputArgs > 0) {
            sql.append("null"); //$NON-NLS-1$
            for(int i=1; i<inputArgs; i++) {
                sql.append(", null");                 //$NON-NLS-1$
            }
        }
        sql.append(")"); //$NON-NLS-1$
        Call proc = (Call) transUtil.parseCommand(sql.toString());
        return proc;
    }

    private void checkParameter(Argument param,
                                String name,
                                String fullName,
                                Direction direction,
                                String nameInSource,
                                String defaultValue,
                                NullType nullability,
                                Class<?> javaType,
                                int length,
                                int precision,
                                int scale) throws Exception {
        ProcedureParameter p = param.getMetadataObject();
        assertEquals(name, p.getName());
        assertEquals(fullName, p.getFullName());
        assertEquals(direction, param.getDirection());
        assertEquals(nameInSource, p.getNameInSource());
        assertEquals(defaultValue, p.getDefaultValue());
        assertEquals(nullability, p.getNullType());
        assertEquals(javaType, p.getJavaType());
        assertEquals(javaType, param.getType());
        assertEquals(length, p.getLength());
        assertEquals(precision, p.getPrecision());
        assertEquals(scale, p.getScale());
        assertEquals(null, param.getArgumentValue().getValue());

    }

    public void testProcedureWithResultSet() throws Exception {
        Call proc = getProcedure("sptest.proc1", 4, CONNECTOR_METADATA_UTILITY);      //$NON-NLS-1$
        List params = proc.getArguments();
        assertEquals(4, params.size());

        checkParameter((Argument)params.get(0),
                       "in1", //$NON-NLS-1$
                       "sptest.proc1.in1", //$NON-NLS-1$
                       Direction.IN,
                       null,
                       "sample default",
                       NullType.No_Nulls, //$NON-NLS-1$
                       String.class,
                       20,
                       10,
                       5); //$NON-NLS-1$

        checkParameter((Argument)params.get(1),
                       "in2", //$NON-NLS-1$
                       "sptest.proc1.in2", //$NON-NLS-1$
                       Direction.IN,
                       null,
                       "15",
                       NullType.Nullable, //$NON-NLS-1$
                       Integer.class,
                       0,
                       10,
                       0); //$NON-NLS-1$

        checkParameter((Argument)params.get(2),
                       "in3", //$NON-NLS-1$
                       "sptest.proc1.in3", //$NON-NLS-1$
                       Direction.IN,
                       null,
                       "2003-04-23 09:30:00",
                       NullType.Unknown, //$NON-NLS-1$
                       Timestamp.class,
                       22,
                       10,
                       0); //$NON-NLS-1$

        checkParameter((Argument)params.get(3),
                       "inOptional", //$NON-NLS-1$
                       "sptest.proc1.inOptional", //$NON-NLS-1$
                       Direction.IN,
                       "optionalName",
                       null, //$NON-NLS-1$
                       NullType.Nullable,
                       String.class,
                       DataTypeManager.MAX_STRING_LENGTH,
                       0,
                       0); //$NON-NLS-1$


    }

}
