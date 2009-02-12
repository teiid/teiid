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

package com.metamatrix.connector.jdbc.oracle.spatial;

import java.util.ArrayList;
import java.util.List;

/**
 * This class serves as the single repository for the acceptable mutations of the Oracle Spatial function names.
 * Adding a new permutation here should make all of the
 */
public class OracleSpatialFunctions {

    public static List relateFunctions = new ArrayList();

    public static List nearestNeighborFunctions = new ArrayList();

    public static List filterFunctions = new ArrayList();

    public static List withinDistanceFunctions = new ArrayList();

    public static List nnDistanceFunctions = new ArrayList();
    
    static {
        relateFunctions.add("SDORELATE"); //$NON-NLS-1$
        relateFunctions.add("SDO_RELATE"); //$NON-NLS-1$
        relateFunctions.add("SDORELATE2"); //$NON-NLS-1$
        relateFunctions.add("SDORELATE3"); //$NON-NLS-1$
        relateFunctions.add("sdorelate"); //$NON-NLS-1$
        relateFunctions.add("sdo_relate"); //$NON-NLS-1$
        relateFunctions.add("sdorelate2"); //$NON-NLS-1$
        relateFunctions.add("sdorelate3"); //$NON-NLS-1$
        relateFunctions.add("sdoRelate"); //$NON-NLS-1$
        relateFunctions.add("sdo_Relate"); //$NON-NLS-1$
        relateFunctions.add("sdoRelate2"); //$NON-NLS-1$
        relateFunctions.add("sdoRelate3"); //$NON-NLS-1$

        nearestNeighborFunctions.add("sdo_nn"); //$NON-NLS-1$
        nearestNeighborFunctions.add("SDO_NN"); //$NON-NLS-1$
        nearestNeighborFunctions.add("Sso_Nn"); //$NON-NLS-1$

        nearestNeighborFunctions.add("sdonn"); //$NON-NLS-1$
        nearestNeighborFunctions.add("SDONN"); //$NON-NLS-1$
        nearestNeighborFunctions.add("SdoNn"); //$NON-NLS-1$

        filterFunctions.add("sdo_filter"); //$NON-NLS-1$
        filterFunctions.add("SDO_FILTER"); //$NON-NLS-1$
        filterFunctions.add("Sdo_Filter"); //$NON-NLS-1$

        filterFunctions.add("sdofilter"); //$NON-NLS-1$
        filterFunctions.add("SDOFILTER"); //$NON-NLS-1$
        filterFunctions.add("SdoFilter"); //$NON-NLS-1$

        withinDistanceFunctions.add("sdo_within_distance"); //$NON-NLS-1$
        withinDistanceFunctions.add("SDO_WITHIN_DISTANCE"); //$NON-NLS-1$
        withinDistanceFunctions.add("Sdo_Within_Distance"); //$NON-NLS-1$

        withinDistanceFunctions.add("sdowithindistance"); //$NON-NLS-1$
        withinDistanceFunctions.add("SDOWITHINDISTANCE"); //$NON-NLS-1$
        withinDistanceFunctions.add("SdoWithinDistance"); //$NON-NLS-1$
        
        nnDistanceFunctions.add("Sdo_Nn_Distance"); //$NON-NLS-1$
        nnDistanceFunctions.add("sdo_sn_distance"); //$NON-NLS-1$
        nnDistanceFunctions.add("SDO_NN_DISTANCE"); //$NON-NLS-1$
    }

}