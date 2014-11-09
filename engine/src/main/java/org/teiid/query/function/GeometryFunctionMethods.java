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

package org.teiid.query.function;

import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.translator.SourceSystemFunctions;

public class GeometryFunctionMethods {

    @TeiidFunction(name=SourceSystemFunctions.ST_ASTEXT, 
                   category=FunctionCategoryConstants.GEOMETRY)
    public static ClobType asText(GeometryType geometry) {
       return GeometryUtils.geometryToClob(geometry);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMTEXT,
                   category=FunctionCategoryConstants.GEOMETRY)
    public static GeometryType geomFromText(ClobType wkt) {
        return GeometryUtils.geometryFromClob(wkt);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_INTERSECTS,
                   category=FunctionCategoryConstants.GEOMETRY,
                   pushdown=PushDown.MUST_PUSHDOWN)
    public static Boolean intersects(GeometryType geom1, GeometryType geom2) {
        throw new UnsupportedOperationException();
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_CONTAINS,
                   category=FunctionCategoryConstants.GEOMETRY,
                   pushdown=PushDown.MUST_PUSHDOWN)
    public static Boolean contains(GeometryType geom1, GeometryType geom2) {
        throw new UnsupportedOperationException();
    }
}
