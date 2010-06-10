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

import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Call;
import org.teiid.metadata.Column;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;


/**
 */
public class TestProcedure extends TestCase {

    private static TranslationUtility CONNECTOR_METADATA_UTILITY = createTranslationUtility(getTestVDBName());

    /**
     * Constructor for TestGroup.
     * @param name
     */
    public TestProcedure(String name) {
        super(name);
    }

    private static String getTestVDBName() {
        return UnitTestUtil.getTestDataPath() + "/ConnectorMetadata.vdb"; //$NON-NLS-1$
    }
    
    public static TranslationUtility createTranslationUtility(String vdbName) {
        return new TranslationUtility(vdbName);        
    }

    public Procedure getProcedure(String procName, int inputArgs, TranslationUtility transUtil) throws Exception {
        StringBuffer sql = new StringBuffer("EXEC " + procName + "("); //$NON-NLS-1$ //$NON-NLS-2$
        if(inputArgs > 0) {
            sql.append("null"); //$NON-NLS-1$
            for(int i=1; i<inputArgs; i++) {
                sql.append(", null");                 //$NON-NLS-1$
            }
        }
        sql.append(")"); //$NON-NLS-1$
        Call proc = (Call) transUtil.parseCommand(sql.toString()); 
        return proc.getMetadataObject();
    }

    public void testProcedure1() throws Exception {
        Procedure proc = getProcedure("ConnectorMetadata.TestProc1", 2, CONNECTOR_METADATA_UTILITY);      //$NON-NLS-1$
        assertEquals("Procedure name in source", proc.getNameInSource()); //$NON-NLS-1$
        
        String[] nameInSource = new String[] { "Param name in source", null, null, null }; //$NON-NLS-1$
        ProcedureParameter.Type[] direction = new ProcedureParameter.Type[] { ProcedureParameter.Type.In, ProcedureParameter.Type.Out, ProcedureParameter.Type.InOut, ProcedureParameter.Type.ReturnValue };
        int[] index = new int[] { 1, 2, 3, 4 };
        Class<?>[] type = new Class[] { Integer.class, Long.class, Short.class, java.sql.Date.class };        
        
        checkParams(proc, nameInSource, direction, index, type);
                
    }   
    
    public void testProcedureWithResultSet() throws Exception {
        Procedure proc = getProcedure("ConnectorMetadata.TestProc2", 1, CONNECTOR_METADATA_UTILITY);      //$NON-NLS-1$
        assertEquals(null, proc.getNameInSource());
        
        String[] nameInSource = new String[] { null };
        ProcedureParameter.Type[] direction = new ProcedureParameter.Type[] { ProcedureParameter.Type.In };
        int[] index = new int[] { 1 };
        Class<?>[] type = new Class[] { String.class };        
        
        checkParams(proc, nameInSource, direction, index, type);
        
        List<Column> rsCols = proc.getResultSet().getColumns();
        // Check first column of result set
        assertEquals(2, rsCols.size());
        Column elemID = rsCols.get(0);
        assertEquals("RSCol1", elemID.getName());         //$NON-NLS-1$
        assertEquals("ConnectorMetadata.TestProc2.RSParam.RSCol1", elemID.getFullName());         //$NON-NLS-1$
        assertEquals("Result set column name in source", elemID.getNameInSource());         //$NON-NLS-1$
        assertEquals(java.sql.Timestamp.class, elemID.getJavaType());
        assertEquals(1, elemID.getPosition());
        
        Column elemID2 = rsCols.get(1);        
        assertEquals("RSCol2", elemID2.getName());         //$NON-NLS-1$
        assertEquals("ConnectorMetadata.TestProc2.RSParam.RSCol2", elemID2.getFullName());         //$NON-NLS-1$
        assertEquals(null, elemID2.getNameInSource());         
        assertEquals(String.class, elemID2.getJavaType());
        assertEquals(2, elemID2.getPosition());
        Properties props = new Properties();
        props.put("ColProp", "defaultvalue"); //$NON-NLS-1$ //$NON-NLS-2$
        
        // failing because default extension properties aren't in the VDB file
        //assertEquals(props, e2.getProperties());

    }

	private List<ProcedureParameter> checkParams(Procedure proc,
			String[] nameInSource, ProcedureParameter.Type[] direction,
			int[] index, Class<?>[] type) {
		List<ProcedureParameter> params = proc.getParameters();
		assertEquals(type.length, params.size());
        for (int i = 0; i < params.size(); i++) {
        	ProcedureParameter param = params.get(i);
            assertEquals(nameInSource[i], param.getNameInSource());
            assertEquals(direction[i], param.getType());
            assertEquals(index[i], param.getPosition());
            assertEquals(type[i], param.getJavaType());
        }
		return params;
	}   
    
}
