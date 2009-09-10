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

import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IParameter.Direction;

import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.core.util.UnitTestUtil;


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

    public IProcedure getProcedure(String procName, int inputArgs, TranslationUtility transUtil) throws Exception {
        StringBuffer sql = new StringBuffer("EXEC " + procName + "("); //$NON-NLS-1$ //$NON-NLS-2$
        if(inputArgs > 0) {
            sql.append("null"); //$NON-NLS-1$
            for(int i=1; i<inputArgs; i++) {
                sql.append(", null");                 //$NON-NLS-1$
            }
        }
        sql.append(")"); //$NON-NLS-1$
        IProcedure proc = (IProcedure) transUtil.parseCommand(sql.toString());
        return proc;
    }

    private void checkParameter(IParameter param,
                                String name,
                                String fullName,
                                int index,
                                Direction direction,
                                String nameInSource,
                                String defaultValue,
                                int nullability,
                                Class javaType,
                                int length,
                                int precision,
                                int scale,
                                TranslationUtility transUtil, String modeledType, String modeledBaseType, String modeledPrimitiveType) throws Exception {
        Parameter p = param.getMetadataObject();
        assertEquals(name, p.getName());
        assertEquals(fullName, p.getFullName());
        assertEquals(index, param.getIndex());
        assertEquals(direction, param.getDirection());
        assertEquals(nameInSource, p.getNameInSource());
        assertEquals(defaultValue, p.getDefaultValue());
        assertEquals(nullability, p.getNullability());
        assertEquals(javaType, p.getJavaType());
        assertEquals(javaType, param.getType());
        assertEquals(length, p.getLength());
        assertEquals(precision, p.getPrecision());
        assertEquals(scale, p.getScale());
        assertEquals(null, param.getValue());
        assertEquals(false, param.getValueSpecified());        

        //System.out.println("\n" + p.getModeledType() + "\n" + p.getModeledBaseType() + "\n" + p.getModeledPrimitiveType());
        
        assertEquals(modeledType, p.getModeledType());
        assertEquals(modeledBaseType, p.getModeledBaseType());
        assertEquals(modeledPrimitiveType, p.getModeledPrimitiveType());
        
    }

    public void testProcedureWithResultSet() throws Exception {
        IProcedure proc = getProcedure("sptest.proc1", 4, CONNECTOR_METADATA_UTILITY);      //$NON-NLS-1$
        List params = proc.getParameters();
        assertEquals(4, params.size());
        
        checkParameter((IParameter)params.get(0),
                       "in1", //$NON-NLS-1$
                       "sptest.proc1.in1", //$NON-NLS-1$
                       1,
                       Direction.IN,
                       null,
                       "sample default", //$NON-NLS-1$
                       TypeModel.NOT_NULLABLE,
                       String.class,
                       20,
                       10,
                       5,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.w3.org/2001/XMLSchema#string",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#anySimpleType",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#string"); //$NON-NLS-1$

        checkParameter((IParameter)params.get(1),
                       "in2", //$NON-NLS-1$
                       "sptest.proc1.in2", //$NON-NLS-1$
                       2,
                       Direction.IN,
                       null,
                       "15", //$NON-NLS-1$
                       TypeModel.NULLABLE,
                       Integer.class,
                       0,
                       10,
                       0,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.w3.org/2001/XMLSchema#int",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#long",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#decimal"); //$NON-NLS-1$

        checkParameter((IParameter)params.get(2),
                       "in3", //$NON-NLS-1$
                       "sptest.proc1.in3", //$NON-NLS-1$
                       3,
                       Direction.IN,
                       null,
                       "2003-04-23 09:30:00", //$NON-NLS-1$
                       TypeModel.NULLABLE_UNKNOWN,
                       Timestamp.class,
                       22,
                       10,
                       0,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.metamatrix.com/metamodels/SimpleDatatypes-instance#timestamp",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#dateTime",  //$NON-NLS-1$
                       "http://www.w3.org/2001/XMLSchema#dateTime"); //$NON-NLS-1$

        checkParameter((IParameter)params.get(3),
                       "inOptional", //$NON-NLS-1$
                       "sptest.proc1.inOptional", //$NON-NLS-1$
                       4,
                       Direction.IN,
                       "optionalName", //$NON-NLS-1$
                       null,
                       TypeModel.NULLABLE,
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
