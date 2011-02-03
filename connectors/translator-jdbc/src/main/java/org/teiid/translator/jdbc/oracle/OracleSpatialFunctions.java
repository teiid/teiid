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
package org.teiid.translator.jdbc.oracle;

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;

public class OracleSpatialFunctions {
	
	/*
	 * Spatial Functions
	 */
	public static final String RELATE = "sdo_relate"; //$NON-NLS-1$
	public static final String NEAREST_NEIGHBOR = "sdo_nn"; //$NON-NLS-1$
	public static final String FILTER = "sdo_filter"; //$NON-NLS-1$
	public static final String WITHIN_DISTANCE = "sdo_within_distance"; //$NON-NLS-1$
	public static final String NEAREST_NEIGHBOR_DISTANCE = "sdo_nn_distance"; //$NON-NLS-1$
	public static final String ORACLE_SDO = "Oracle-SDO"; //$NON-NLS-1$
	

    public static List<FunctionMethod> getOracleSpatialFunctions(){
    	    	
    	List<FunctionMethod> spatialFuncs = new ArrayList<FunctionMethod>();
    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + RELATE, RELATE, ORACLE_SDO,   
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$

    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + RELATE, RELATE, ORACLE_SDO, 
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + RELATE, RELATE, ORACLE_SDO,  
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$

    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + RELATE, RELATE, ORACLE_SDO, 
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + NEAREST_NEIGHBOR, NEAREST_NEIGHBOR, ORACLE_SDO,   
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("NUMBER", DataTypeManager.DefaultDataTypes.INTEGER, "") }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + NEAREST_NEIGHBOR, NEAREST_NEIGHBOR, ORACLE_SDO,  
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("NUMBER", DataTypeManager.DefaultDataTypes.INTEGER, "") }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    	

    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + NEAREST_NEIGHBOR, NEAREST_NEIGHBOR, ORACLE_SDO,  
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("NUMBER", DataTypeManager.DefaultDataTypes.INTEGER, "") }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    	
    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + NEAREST_NEIGHBOR_DISTANCE, NEAREST_NEIGHBOR_DISTANCE, ORACLE_SDO,  
                new FunctionParameter[] {
                    new FunctionParameter("NUMBER", DataTypeManager.DefaultDataTypes.INTEGER, "") }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$  
    	    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + WITHIN_DISTANCE, WITHIN_DISTANCE, ORACLE_SDO, 
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + WITHIN_DISTANCE, WITHIN_DISTANCE, ORACLE_SDO,   
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    	
    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + WITHIN_DISTANCE, WITHIN_DISTANCE, ORACLE_SDO,  
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + FILTER, FILTER, ORACLE_SDO,   
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + FILTER, FILTER, ORACLE_SDO,  
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    
    	
    	spatialFuncs.add(new FunctionMethod(ORACLE_SDO + '.' + FILTER, FILTER, ORACLE_SDO,   
                new FunctionParameter[] {
                    new FunctionParameter("GEOM1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("GEOM2", DataTypeManager.DefaultDataTypes.OBJECT, ""), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("PARAMS", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$      	
    	
    	return spatialFuncs;
    }
}
