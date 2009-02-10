/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

/*
 * Created on Mar 23, 2004
 */
package com.metamatrix.connector.jdbc.oracle.spatial;

import java.util.Iterator;

import com.metamatrix.connector.jdbc.oracle.OracleSQLConversionVisitor;
import com.metamatrix.connector.language.ISelect;
import com.metamatrix.connector.language.ISelectSymbol;

public class OracleSpatialConversionVisitor extends OracleSQLConversionVisitor {

    public void visit(ISelect obj) {
        buffer.append(SELECT);
        buffer.append(SPACE);

        // Add a hint here if one is in the SELECT
        if (obj instanceof SpatialHint) {
            SpatialHint hint = (SpatialHint)obj;
            buffer.append(hint.getHint());
            buffer.append(SPACE);
        }

        // Add DISTINCT if necessary
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(SPACE);
        }

        // Add select symbols
        Iterator iter = obj.getSelectSymbols().iterator();
        while (iter.hasNext()) {
            ISelectSymbol sSymbol = (ISelectSymbol)iter.next();
            if (sSymbol.getExpression().getType().equals(Object.class)) {
                buffer.append(NULL).append(SPACE);
                buffer.append(AS).append(SPACE);
                String outName = sSymbol.getOutputName();
                int lIndx = outName.lastIndexOf("."); //$NON-NLS-1$
                buffer.append(outName.substring(lIndx + 1)).append(SPACE);
            } else {
                append(sSymbol);
            }
            if (iter.hasNext())
                buffer.append(COMMA).append(SPACE);
        }
    }

}