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

package com.metamatrix.data.metadata.runtime;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.connector.language.IParameter;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.metadata.runtime.Parameter;
import com.metamatrix.connector.metadata.runtime.TypeModel;
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
        return UnitTestUtil.getTestDataPath() + ""+File.separator+"sptest"+File.separator+"spvdb.vdb"; //$NON-NLS-1$
    }
    
    public static TranslationUtility createTranslationUtility(String vdbName) {
        return new TranslationUtility(vdbName);        
    }

    public IProcedure getProcedure(String procName, int inputArgs, TranslationUtility transUtil) throws Exception {
        StringBuffer sql = new StringBuffer("EXEC " + procName + "("); //$NON-NLS-1$ //$NON-NLS-2$
        if(inputArgs > 0) {
            sql.append("?"); //$NON-NLS-1$
            for(int i=1; i<inputArgs; i++) {
                sql.append(", ?");                 //$NON-NLS-1$
            }
        }
        sql.append(")"); //$NON-NLS-1$
        IProcedure proc = (IProcedure) transUtil.parseCommand(sql.toString()); //$NON-NLS-1$
        return proc;
    }

    private void checkParameter(IParameter param,
                                String name,
                                String fullName,
                                int index,
                                int direction,
                                String nameInSource,
                                String defaultValue,
                                int nullability,
                                Class javaType,
                                int length,
                                int precision,
                                int scale,
                                TranslationUtility transUtil, String modeledType, String modeledBaseType, String modeledPrimitiveType) throws Exception {
        MetadataID metadataID = param.getMetadataID();
        Parameter p = (Parameter)transUtil.createRuntimeMetadata().getObject(metadataID);
        assertEquals(name, metadataID.getName());
        assertEquals(fullName, metadataID.getFullName());
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
        assertEquals(MetadataID.TYPE_PARAMETER, metadataID.getType());

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
                       "in1",
                       "sptest.proc1.in1",
                       1,
                       IParameter.IN,
                       null,
                       "sample default",
                       TypeModel.NOT_NULLABLE,
                       String.class,
                       20,
                       10,
                       5,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.w3.org/2001/XMLSchema#string", 
                       "http://www.w3.org/2001/XMLSchema#anySimpleType", 
                       "http://www.w3.org/2001/XMLSchema#string");

        checkParameter((IParameter)params.get(1),
                       "in2",
                       "sptest.proc1.in2",
                       2,
                       IParameter.IN,
                       null,
                       "15",
                       TypeModel.NULLABLE,
                       Integer.class,
                       0,
                       10,
                       0,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.w3.org/2001/XMLSchema#int", 
                       "http://www.w3.org/2001/XMLSchema#long", 
                       "http://www.w3.org/2001/XMLSchema#decimal");

        checkParameter((IParameter)params.get(2),
                       "in3",
                       "sptest.proc1.in3",
                       3,
                       IParameter.IN,
                       null,
                       "2003-04-23 09:30:00",
                       TypeModel.NULLABLE_UNKNOWN,
                       Timestamp.class,
                       22,
                       10,
                       0,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.metamatrix.com/metamodels/SimpleDatatypes-instance#timestamp", 
                       "http://www.w3.org/2001/XMLSchema#dateTime", 
                       "http://www.w3.org/2001/XMLSchema#dateTime");

        checkParameter((IParameter)params.get(3),
                       "inOptional",
                       "sptest.proc1.inOptional",
                       4,
                       IParameter.IN,
                       "optionalName",
                       null,
                       TypeModel.NULLABLE,
                       String.class,
                       0,
                       0,
                       0,
                       CONNECTOR_METADATA_UTILITY, 
                       "http://www.w3.org/2001/XMLSchema#string", 
                       "http://www.w3.org/2001/XMLSchema#anySimpleType", 
                       "http://www.w3.org/2001/XMLSchema#string");

    
    }   

}
