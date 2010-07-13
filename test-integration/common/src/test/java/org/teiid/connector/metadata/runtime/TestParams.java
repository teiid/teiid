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

package org.teiid.connector.metadata.runtime;

import java.sql.Timestamp;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Argument.Direction;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.BaseColumn.NullType;



/** 
 * @since 4.3
 */
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
                                int index,
                                Direction direction,
                                String nameInSource,
                                String defaultValue,
                                NullType nullability,
                                Class javaType,
                                int length,
                                int precision,
                                int scale,
                                TranslationUtility transUtil, String modeledType, String modeledBaseType, String modeledPrimitiveType) throws Exception {
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

        //System.out.println("\n" + p.getModeledType() + "\n" + p.getModeledBaseType() + "\n" + p.getModeledPrimitiveType());
        
        assertEquals(modeledType, p.getDatatypeID());
        assertEquals(modeledBaseType, p.getBaseTypeID());
        assertEquals(modeledPrimitiveType, p.getPrimitiveTypeID());
        
    }

    public void testProcedureWithResultSet() throws Exception {
        Call proc = getProcedure("sptest.proc1", 4, CONNECTOR_METADATA_UTILITY);      //$NON-NLS-1$
        List params = proc.getArguments();
        assertEquals(4, params.size());
        
        checkParameter((Argument)params.get(0),
                       "in1", //$NON-NLS-1$
                       "sptest.proc1.in1", //$NON-NLS-1$
                       1,
                       Direction.IN,
                       null,
                       "sample default", //$NON-NLS-1$
                       NullType.No_Nulls,
                       String.class,
                       20,
                       10,
                       5,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.w3.org/2001/XMLSchema#string",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#anySimpleType",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#string"); //$NON-NLS-1$

        checkParameter((Argument)params.get(1),
                       "in2", //$NON-NLS-1$
                       "sptest.proc1.in2", //$NON-NLS-1$
                       2,
                       Direction.IN,
                       null,
                       "15", //$NON-NLS-1$
                       NullType.Nullable,
                       Integer.class,
                       0,
                       10,
                       0,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.w3.org/2001/XMLSchema#int",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#long",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#decimal"); //$NON-NLS-1$

        checkParameter((Argument)params.get(2),
                       "in3", //$NON-NLS-1$
                       "sptest.proc1.in3", //$NON-NLS-1$
                       3,
                       Direction.IN,
                       null,
                       "2003-04-23 09:30:00", //$NON-NLS-1$
                       NullType.Unknown,
                       Timestamp.class,
                       22,
                       10,
                       0,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.metamatrix.com/metamodels/SimpleDatatypes-instance#timestamp",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#string",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#string"); //$NON-NLS-1$

        checkParameter((Argument)params.get(3),
                       "inOptional", //$NON-NLS-1$
                       "sptest.proc1.inOptional", //$NON-NLS-1$
                       4,
                       Direction.IN,
                       "optionalName", //$NON-NLS-1$
                       null,
                       NullType.Nullable,
                       String.class,
                       0,
                       0,
                       0,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.w3.org/2001/XMLSchema#string",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#anySimpleType",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#string"); //$NON-NLS-1$

    
    }   

}
