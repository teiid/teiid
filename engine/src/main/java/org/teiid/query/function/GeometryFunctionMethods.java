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

import java.sql.Blob;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.translator.SourceSystemFunctions;

public class GeometryFunctionMethods {

    @TeiidFunction(name=SourceSystemFunctions.ST_ASTEXT, 
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true)
    public static ClobType asText(GeometryType geometry) throws FunctionExecutionException {
       return GeometryUtils.geometryToClob(geometry);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ASBINARY,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true)
    public static BlobType asBlob(GeometryType geometry) {
        Blob b = geometry.getReference();
        return new BlobType(b);
    }
    
    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMBINARY,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true)
    public static GeometryType geoFromBlob(BlobType wkb) throws FunctionExecutionException {
    	return GeometryUtils.geometryFromBlob(wkb);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMBINARY,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true)
    public static GeometryType geoFromBlob(BlobType wkb, int srid) throws FunctionExecutionException {
    	return GeometryUtils.geometryFromBlob(wkb, srid);
    }
    
    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMTEXT,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true)
    public static GeometryType geomFromText(ClobType wkt) throws FunctionExecutionException {
    	return GeometryUtils.geometryFromClob(wkt);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMTEXT,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true)
    public static GeometryType geomFromText(ClobType wkt, int srid) throws FunctionExecutionException {
    	return GeometryUtils.geometryFromClob(wkt, srid);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_INTERSECTS,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean intersects(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
    	return GeometryUtils.intersects(geom1, geom2);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_CONTAINS,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean contains(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
    	return GeometryUtils.contains(geom1, geom2);
    }
    
    @TeiidFunction(name=SourceSystemFunctions.ST_CROSSES,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean crosses(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
    	return GeometryUtils.crosses(geom1, geom2);
    }
    
    @TeiidFunction(name=SourceSystemFunctions.ST_DISJOINT,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean disjoint(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
    	return GeometryUtils.disjoint(geom1, geom2);
    }
    
    @TeiidFunction(name=SourceSystemFunctions.ST_DISTANCE,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static Double distance(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
    	return GeometryUtils.distance(geom1, geom2);
    }
    
    @TeiidFunction(name=SourceSystemFunctions.ST_OVERLAPS,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean overlaps(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
    	return GeometryUtils.overlaps(geom1, geom2);
    }
    
    @TeiidFunction(name=SourceSystemFunctions.ST_TOUCHES,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean touches(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
    	return GeometryUtils.touches(geom1, geom2);
    }
}
