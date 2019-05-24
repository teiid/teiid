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
import org.teiid.core.types.AbstractGeospatialType;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;

public class GeometryFunctionMethods {

    @TeiidFunction(name=SourceSystemFunctions.ST_ASTEXT,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static ClobType asText(GeometryType geometry)
            throws FunctionExecutionException {
       return GeometryUtils.geometryToClob(geometry, false);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ASEWKT,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static ClobType asEwkt(GeometryType geometry)
            throws FunctionExecutionException {
        return GeometryUtils.geometryToClob(geometry, true);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ASBINARY,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static BlobType asBlob(GeometryType geometry) {
        Blob b = geometry.getReference();
        return new BlobType(b);
    }

    public static BlobType asBlob(AbstractGeospatialType geometry, String encoding) throws FunctionExecutionException {
        if ("NDR".equals(encoding)) { //$NON-NLS-1$
            return new BlobType(GeometryUtils.getBytes(GeometryUtils.getGeometry(geometry), false));
        }
        Blob b = geometry.getReference();
        return new BlobType(b);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ASEWKB,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static BlobType asEwkb(final GeometryType geometry) {
        return GeometryUtils.geometryToEwkb(geometry);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ASGML,
                   category=FunctionCategoryConstants.GEOMETRY,
                   pushdown=PushDown.CAN_PUSHDOWN,
                   nullOnNull=true)
    public static ClobType asGml(CommandContext context, GeometryType geometry)
            throws FunctionExecutionException {
        return GeometryUtils.geometryToGml(context, geometry, true);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ASKML,
                   category=FunctionCategoryConstants.GEOMETRY,
                   pushdown=PushDown.CAN_PUSHDOWN,
                   nullOnNull=true)
    public static ClobType asKml(CommandContext context, GeometryType geometry)
            throws FunctionExecutionException {
        return GeometryUtils.geometryToGml(context, geometry, false);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMTEXT,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true)
    public static GeometryType geomFromText(ClobType wkt)
            throws FunctionExecutionException {
        return GeometryUtils.geometryFromClob(wkt);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMTEXT,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType geomFromText(ClobType wkt, int srid)
            throws FunctionExecutionException {
        return GeometryUtils.getGeometryType(GeometryUtils.geometryFromClob(wkt, srid, false));
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMWKB,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   alias="ST_GEOMFROMBINARY")
    public static GeometryType geoFromBlob(BlobType wkb)
            throws FunctionExecutionException {
        return GeometryUtils.geometryFromBlob(wkb);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMWKB,
                   category=FunctionCategoryConstants.GEOMETRY,
                   pushdown=PushDown.CAN_PUSHDOWN,
                   nullOnNull=true,
                   alias="ST_GEOMFROMBINARY")
    public static GeometryType geoFromBlob(BlobType wkb, int srid)
            throws FunctionExecutionException {
        return GeometryUtils.geometryFromBlob(wkb, srid);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMGML,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true)
    public static GeometryType geomFromGml(ClobType gml)
            throws FunctionExecutionException {
        return GeometryUtils.geometryFromGml(gml, null);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMGML,
                   category=FunctionCategoryConstants.GEOMETRY,
                   pushdown=PushDown.CAN_PUSHDOWN,
                   nullOnNull=true)
    public static GeometryType geomFromGml(ClobType gml, int srid)
            throws FunctionExecutionException {
        return GeometryUtils.geometryFromGml(gml, srid);
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

    @TeiidFunction(name=SourceSystemFunctions.ST_SRID,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static int getSrid(GeometryType geom1) {
        return geom1.getSrid();
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_SETSRID,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType setSrid(GeometryType geom1, int srid) {
        GeometryType gt = new GeometryType();
        gt.setReference(geom1.getReference());
        gt.setSrid(srid);
        return gt;
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_EQUALS,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean equals(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        return GeometryUtils.equals(geom1, geom2);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ENVELOPE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType envelope(GeometryType geom) throws FunctionExecutionException {
        return GeometryUtils.envelope(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_WITHIN,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean within(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        return GeometryUtils.within(geom1, geom2);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_DWITHIN,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean dwithin(GeometryType geom1, GeometryType geom2, double distance) throws FunctionExecutionException {
        return GeometryUtils.dwithin(geom1, geom2, distance);
    }

    /*
     * PostGIS compatibility
     */

    @TeiidFunction(name=SourceSystemFunctions.ST_SIMPLIFY,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType simplify(GeometryType geom,
                                      double tolerance)
                                              throws FunctionExecutionException {
        return GeometryUtils.simplify(geom, tolerance);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_SIMPLIFYPRESERVETOPOLOGY,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType simplifyPreserveTopology(GeometryType geom,
                                      double tolerance)
                                              throws FunctionExecutionException {
        return GeometryUtils.simplifyPreserveTopology(geom, tolerance);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_FORCE_2D,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType force2D(GeometryType geom) {
        return geom; //higher dimensions not supported
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_HASARC,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static boolean hasArc(@SuppressWarnings("unused") GeometryType geom) {
        return false; //curved not supported
    }

    @TeiidFunction(name=SQLConstants.Tokens.DOUBLE_AMP,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static boolean boundingBoxIntersects(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        return GeometryUtils.boundingBoxIntersects(geom1, geom2);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMEWKT,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType geomFromEwkt(ClobType ewkt)
         throws FunctionExecutionException {
        return GeometryUtils.getGeometryType(GeometryUtils.geometryFromClob(ewkt, null, true));
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMFROMEWKB,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType geomFromEwkb(BlobType ewkb)
         throws FunctionExecutionException, SQLException {
        return GeometryUtils.geometryFromEwkb(ewkb.getBinaryStream(), null);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_AREA,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Double area(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.area(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_BOUNDARY,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType boundary(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.boundary(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_BUFFER,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType buffer(GeometryType geom, double distance)
         throws FunctionExecutionException {
        return GeometryUtils.buffer(geom, distance);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_CENTROID,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType centroid(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.centroid(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_CONVEXHULL,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType convexHull(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.convexHull(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_COORDDIM,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Integer coordDim(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.coordDim(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_CURVETOLINE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType curveToLine(@SuppressWarnings("unused") GeometryType geom)
         throws FunctionExecutionException {
        throw new FunctionExecutionException(QueryPlugin.Event.TEIID31206, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31206));
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_DIFFERENCE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType difference(GeometryType geom1, GeometryType geom2)
         throws FunctionExecutionException {
        return GeometryUtils.difference(geom1, geom2);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_DIMENSION,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Integer dimension(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.dimension(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ENDPOINT,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType endPoint(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.startEndPoint(geom, false);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_EXTERIORRING,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType exteriorRing(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.exteriorRing(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMETRYN,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType geometryN(GeometryType geom, int index)
         throws FunctionExecutionException {
        return GeometryUtils.geometryN(geom, index - 1);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_GEOMETRYTYPE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static String geometryType(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.geometryType(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_INTERIORRINGN,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType interiorRingN(GeometryType geom, int index)
         throws FunctionExecutionException {
        return GeometryUtils.interiorRingN(geom, index - 1);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_INTERSECTION,
                   category=FunctionCategoryConstants.GEOMETRY,
                   nullOnNull=true,
                   pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType intersection(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        return GeometryUtils.intersection(geom1, geom2);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ISCLOSED,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean isClosed(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.isClosed(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ISEMPTY,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean isEmpty(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.isEmpty(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ISRING,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean isRing(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.isRing(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ISSIMPLE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean isSimple(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.isSimple(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ISVALID,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean isValid(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.isValid(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_LENGTH,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Double length(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.length(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_NUMGEOMETRIES,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Integer numGeometries(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.numGeometries(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_NUMINTERIORRINGS,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Integer numInteriorRings(GeometryType geom) throws FunctionExecutionException {
        return GeometryUtils.numInteriorRings(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_NUMPOINTS,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Integer numPoints(GeometryType geom) throws FunctionExecutionException {
        return GeometryUtils.numPoints(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_ORDERINGEQUALS,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean orderingEquals(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        return GeometryUtils.orderingEquals(geom1, geom2);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_POINT,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType point(double x, double y) {
        return GeometryUtils.point(x, y);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_POINTN,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType pointN(GeometryType geom, int index) throws FunctionExecutionException {
        return GeometryUtils.pointN(geom, index - 1);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_PERIMETER,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Double perimeter(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.perimeter(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_POINTONSURFACE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType pointOnSurface(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.pointOnSurface(geom);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_POLYGON,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType polygon(GeometryType geom, int srid)
         throws FunctionExecutionException {
        return GeometryUtils.polygon(geom, srid);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_RELATE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static String relate(GeometryType geom1, GeometryType geom2)
         throws FunctionExecutionException {
        return GeometryUtils.relate(geom1, geom2);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_RELATE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Boolean relate(GeometryType geom1, GeometryType geom2, String intersectionPattern)
         throws FunctionExecutionException {
        return GeometryUtils.relate(geom1, geom2, intersectionPattern);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_STARTPOINT,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType startPoint(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.startEndPoint(geom, true);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_SYMDIFFERENCE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType symDifference(GeometryType geom1, GeometryType geom2)
         throws FunctionExecutionException {
        return GeometryUtils.symDifference(geom1, geom2);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_UNION,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType union(GeometryType geom1, GeometryType geom2)
         throws FunctionExecutionException {
        return GeometryUtils.union(geom1, geom2);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_X,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Double ordinateX(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.ordinate(geom, GeometryUtils.Ordinate.X);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_Y,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Double ordinateY(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.ordinate(geom, GeometryUtils.Ordinate.Y);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_Z,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static Double ordinateZ(GeometryType geom)
         throws FunctionExecutionException {
        return GeometryUtils.ordinate(geom, GeometryUtils.Ordinate.Z);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_MAKEENVELOPE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType makeEnvelope(double xmin, double ymin, double xmax, double ymax) {
        return GeometryUtils.makeEnvelope(xmin, ymin, xmax, ymax, null);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_MAKEENVELOPE,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType makeEnvelope(double xmin, double ymin, double xmax, double ymax, int srid) {
        return GeometryUtils.makeEnvelope(xmin, ymin, xmax, ymax, srid);
    }

    @TeiidFunction(name=SourceSystemFunctions.ST_SNAPTOGRID,
            category=FunctionCategoryConstants.GEOMETRY,
            nullOnNull=true,
            pushdown=PushDown.CAN_PUSHDOWN)
    public static GeometryType snapToGrid(GeometryType geom, float size) throws FunctionExecutionException {
        return GeometryUtils.snapToGrid(geom, size);
    }

}
