/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.function;

import java.sql.Blob;
import java.sql.SQLException;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeographyType;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;

public class GeographyFunctionMethods {

    @TeiidFunction(name=SourceSystemFunctions.ST_ASEWKT,
            category=FunctionCategoryConstants.GEOGRAPHY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static ClobType asEwkt(GeographyType geometry)
         throws FunctionExecutionException {
        return GeometryUtils.geometryToClob(geometry, true);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ASBINARY,
            category=FunctionCategoryConstants.GEOGRAPHY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static BlobType asBlob(GeographyType geometry) {
        Blob b = geometry.getReference();
        return new BlobType(b);
    }

    //postgis does not provide geography as ewkb
    /*@TeiidFunction(name=SourceSystemFunctions.ST_ASEWKB,
            category=FunctionCategoryConstants.GEOGRAPHY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static BlobType asEwkb(final GeographyType geometry) {
        return GeometryUtils.geometryToEwkb(geometry);
    }*/


    @TeiidFunction(name=SourceSystemFunctions.ST_GEOGFROMTEXT,
            category=FunctionCategoryConstants.GEOGRAPHY,
            nullOnNull=true)
    public static GeographyType geogFromText(CommandContext ctx, ClobType wkt)
         throws FunctionExecutionException {
        return GeometryUtils.getGeographyType(GeometryUtils.geometryFromClob(wkt, null, true), ctx);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOGFROMWKB,
            category=FunctionCategoryConstants.GEOGRAPHY,
            nullOnNull=true)
    public static GeographyType geogFromBlob(CommandContext context, BlobType wkb)
         throws FunctionExecutionException, SQLException {
        return GeometryUtils.geographyFromEwkb(context, wkb.getBinaryStream());
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_SETSRID,
            category=FunctionCategoryConstants.GEOGRAPHY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeographyType setSrid(GeographyType geog, int srid) {
        GeographyType gt = new GeographyType();
        gt.setReference(geog.getReference());
        gt.setSrid(srid);
        return gt;
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_SRID,
            category=FunctionCategoryConstants.GEOGRAPHY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static int getSrid(GeographyType geog) {
        return geog.getSrid();
    }

    /*
     * Not yet supported, need a geodetic library
     */


    @TeiidFunction(name=SourceSystemFunctions.ST_INTERSECTS,
            category=FunctionCategoryConstants.GEOGRAPHY,
            nullOnNull=true,
            pushdown=PushDown.MUST_PUSHDOWN)
    public static Boolean intersects(GeographyType geog1, GeographyType geog2) throws FunctionExecutionException {
        throw new UnsupportedOperationException();
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_DISTANCE,
            category=FunctionCategoryConstants.GEOGRAPHY,
            nullOnNull=true,
            pushdown=PushDown.MUST_PUSHDOWN)
    public static Double distance(GeographyType geog1, GeographyType geog2) throws FunctionExecutionException {
        throw new UnsupportedOperationException();
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_LENGTH,
            category=FunctionCategoryConstants.GEOGRAPHY,
            nullOnNull=true,
            pushdown=PushDown.MUST_PUSHDOWN)
    public static Double length(GeographyType geog)
         throws FunctionExecutionException {
        throw new UnsupportedOperationException();
    }

}
