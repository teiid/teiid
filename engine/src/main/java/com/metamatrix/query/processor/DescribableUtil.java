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

package com.metamatrix.query.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

/**
 */
public class DescribableUtil {

    /**
     * Never construct - just a utilities class
     */
    private DescribableUtil() {
        super();
    }

    /**
     * Helper method to turn a list of projected symbols into a suitable list of
     * output column strings with name and type.
     * @param projectedSymbols The list of SingleElementSymbol projected from a plan or node
     * @return List of output columns for sending to the client as part of the plan
     */                
    public static List getOutputColumnProperties(List projectedSymbols) {
        if(projectedSymbols != null) {
            List outputCols = new ArrayList(projectedSymbols.size());
            for(int i=0; i<projectedSymbols.size() ; i++) {
                SingleElementSymbol symbol = (SingleElementSymbol) projectedSymbols.get(i);
                outputCols.add(symbol.getShortName() + " (" + DataTypeManager.getDataTypeName(symbol.getType()) + ")"); //$NON-NLS-1$ //$NON-NLS-2$
            }
            return outputCols;
        }
        return Collections.EMPTY_LIST;
    }
    

}
