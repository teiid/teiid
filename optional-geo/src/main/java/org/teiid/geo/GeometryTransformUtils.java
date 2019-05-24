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

package org.teiid.geo;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.locationtech.jts.geom.*;
import org.osgeo.proj4j.CRSFactory;
import org.osgeo.proj4j.CoordinateReferenceSystem;
import org.osgeo.proj4j.CoordinateTransform;
import org.osgeo.proj4j.CoordinateTransformFactory;
import org.osgeo.proj4j.ProjCoordinate;
import org.teiid.CommandContext;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.GeometryType;
import org.teiid.jdbc.TeiidConnection;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.GeometryUtils;
import org.teiid.query.function.TeiidFunction;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.translator.SourceSystemFunctions;

/**
 * Wrapper around proj4j library to transform geometries to different coordinate
 * systems (ST_Transform).
 */
public class GeometryTransformUtils {

    /**
     * Convert geometry to a different coordinate system. Geometry must have valid
     * SRID.
     *
     * @param ctx Command context used to lookup proj4 parameters from table.
     * @param geom Geometry to transform.
     * @param srid Target SRID; must exist in SPATIAL_REF_SYS table.
     * @return Reprojected geometry.
     * @throws FunctionExecutionException
     */
    @TeiidFunction(name = SourceSystemFunctions.ST_TRANSFORM, category = FunctionCategoryConstants.GEOMETRY, nullOnNull = true, pushdown = PushDown.CAN_PUSHDOWN)
    public static GeometryType transform(CommandContext ctx,
                                         GeometryType geom,
                                         int srid)
            throws FunctionExecutionException {
        Geometry jtsGeomSrc = GeometryUtils.getGeometry(geom);

        Geometry jtsGeomTgt = transform(ctx, jtsGeomSrc, srid);

        return GeometryUtils.getGeometryType(jtsGeomTgt, srid);
    }

    /**
     * Convert the raw geometry to the target srid coordinate system.
     * @param ctx Command context used to lookup proj4 parameters from table.
     * @param jtsGeomSrc Geometry to transform.
     * @param srid Target SRID; must exist in SPATIAL_REF_SYS table.
     * @return
     * @throws FunctionExecutionException
     */
    static Geometry transform(CommandContext ctx, Geometry jtsGeomSrc, int srid) throws FunctionExecutionException {
        String srcParam = lookupProj4Text(ctx, jtsGeomSrc.getSRID());
        String tgtParam = lookupProj4Text(ctx, srid);

        Geometry jtsGeomTgt = transform(jtsGeomSrc, srcParam, tgtParam);
        return jtsGeomTgt;
    }

    /**
     * Lookup proj4 parameters in SPATIAL_REF_SYS using SRID as key.
     *
     * @param ctx
     * @param srid
     * @return
     * @throws FunctionExecutionException
     */
    public static String lookupProj4Text(CommandContext ctx, int srid)
            throws FunctionExecutionException {
        String projText;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            TeiidConnection conn = ctx.getConnection();
            pstmt = conn.prepareStatement("select proj4text from SYS.spatial_ref_sys where srid = ?"); //$NON-NLS-1$
            pstmt.setInt(1, srid);
            rs = pstmt.executeQuery();
            if (!rs.next()) {
                throw new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31162, srid));
            }
            projText = rs.getString(1);
        } catch (SQLException e) {
            throw new FunctionExecutionException(e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31163));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e) {
                // ignore
            }
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception e) {
                // ignore
            }
        }

        return projText;
    }

    public static boolean isLatLong(CommandContext ctx, int srid)
            throws FunctionExecutionException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            TeiidConnection conn = ctx.getConnection();
            pstmt = conn.prepareStatement("select (proj4text like '%longlat%') from SYS.spatial_ref_sys where srid = ?"); //$NON-NLS-1$
            pstmt.setInt(1, srid);
            rs = pstmt.executeQuery();
            if (!rs.next()) {
                throw new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31162, srid));
            }
            return rs.getBoolean(1);
        } catch (SQLException e) {
            throw new FunctionExecutionException(e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31163));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e) {
                // ignore
            }
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Convert geometry to different coordinate system given the source/target
     * proj4 parameters. Presumably these were pulled from SPATIAL_REF_SYS.
     *
     * @param geom
     * @param srcParams
     * @param tgtParams
     * @return
     * @throws FunctionExecutionException
     */
    public static Geometry transform(Geometry geom,
                                     String srcParams,
                                     String tgtParams)
            throws FunctionExecutionException {

        CoordinateTransformFactory ctFactory = new CoordinateTransformFactory();
        CRSFactory crsFactory = new CRSFactory();

        CoordinateReferenceSystem srcCrs = crsFactory.createFromParameters(null, srcParams);
        CoordinateReferenceSystem tgtCrs = crsFactory.createFromParameters(null, tgtParams);

        CoordinateTransform coordTransform = ctFactory.createTransform(srcCrs, tgtCrs);

        return transformGeometry(coordTransform, geom);
    }

    protected static Geometry transformGeometry(CoordinateTransform ct,
                                                Geometry geom)
            throws FunctionExecutionException {
        if (geom instanceof Polygon) {
            return transformPolygon(ct, (Polygon) geom);
        } else if (geom instanceof Point) {
            return transformPoint(ct, (Point) geom);
        } else if (geom instanceof LinearRing) {
            return transformLinearRing(ct, (LinearRing) geom);
        } else if (geom instanceof LineString) {
            return transformLineString(ct, (LineString) geom);
        } else if (geom instanceof MultiPolygon) {
            return transformMultiPolygon(ct, (MultiPolygon) geom);
        } else if (geom instanceof MultiPoint) {
            return transformMultiPoint(ct, (MultiPoint) geom);
        } else if (geom instanceof MultiLineString) {
            return transformMultiLineString(ct, (MultiLineString) geom);
        } else if (geom instanceof GeometryCollection) {
            return transformGeometryCollection(ct, (GeometryCollection) geom);
        } else {
            throw new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31164, geom.getGeometryType()));
        }
    }

    /**
     * Convert proj4 coordinates to JTS coordinates.
     *
     * @param projCoords
     * @return
     */
    protected static Coordinate[] convert(ProjCoordinate[] projCoords) {
        Coordinate[] jtsCoords = new Coordinate[projCoords.length];
        for (int i = 0; i < projCoords.length; ++i) {
            jtsCoords[i] = new Coordinate(projCoords[i].x, projCoords[i].y);
        }
        return jtsCoords;
    }

    /**
     * Convert JTS coordinates to proj4j coordinates.
     *
     * @param jtsCoords
     * @return
     */
    protected static ProjCoordinate[] convert(Coordinate[] jtsCoords) {
        ProjCoordinate[] projCoords = new ProjCoordinate[jtsCoords.length];
        for (int i = 0; i < jtsCoords.length; ++i) {
            projCoords[i] = new ProjCoordinate(jtsCoords[i].x, jtsCoords[i].y);
        }
        return projCoords;
    }

    protected static Coordinate[] transformCoordinates(CoordinateTransform ct,
                                                       Coordinate[] in) {
        return convert(transformCoordinates(ct, convert(in)));
    }

    protected static ProjCoordinate[] transformCoordinates(CoordinateTransform ct,
                                                           ProjCoordinate[] in) {
        ProjCoordinate[] out = new ProjCoordinate[in.length];
        for (int i = 0; i < in.length; ++i) {
            out[i] = ct.transform(in[i], new ProjCoordinate());
        }
        return out;
    }

    protected static Polygon transformPolygon(CoordinateTransform ct,
                                              Polygon polygon) {
        return polygon.getFactory().createPolygon(transformCoordinates(ct, polygon.getCoordinates()));
    }

    protected static Geometry transformPoint(CoordinateTransform ct,
                                             Point point) {
        return point.getFactory().createPoint(transformCoordinates(ct, point.getCoordinates())[0]);
    }

    protected static Geometry transformLinearRing(CoordinateTransform ct,
                                                  LinearRing linearRing) {
        return linearRing.getFactory().createLinearRing(transformCoordinates(ct, linearRing.getCoordinates()));
    }

    protected static Geometry transformLineString(CoordinateTransform ct,
                                                  LineString lineString) {
        return lineString.getFactory().createLineString(transformCoordinates(ct, lineString.getCoordinates()));
    }

    protected static Geometry transformMultiPolygon(CoordinateTransform ct,
                                                    MultiPolygon multiPolygon) {
        Polygon[] polygon = new Polygon[multiPolygon.getNumGeometries()];
        for (int i = 0; i < polygon.length; ++i) {
            polygon[i] = multiPolygon.getFactory()
                    .createPolygon(transformCoordinates(ct,
                            multiPolygon.getGeometryN(i).getCoordinates()));
        }
        return multiPolygon.getFactory().createMultiPolygon(polygon);
    }

    protected static Geometry transformMultiPoint(CoordinateTransform ct,
                                                  MultiPoint multiPoint) {
        return multiPoint.getFactory().createMultiPoint(transformCoordinates(ct, multiPoint.getCoordinates()));
    }

    protected static Geometry transformMultiLineString(CoordinateTransform ct,
                                                       MultiLineString multiLineString) {
        LineString[] lineString = new LineString[multiLineString.getNumGeometries()];
        for (int i = 0; i < lineString.length; ++i) {
            lineString[i] = multiLineString.getFactory()
                    .createLineString(transformCoordinates(ct,
                            multiLineString.getGeometryN(i).getCoordinates()));
        }
        return multiLineString.getFactory().createMultiLineString(lineString);
    }

    protected static Geometry transformGeometryCollection(CoordinateTransform ct,
                                                          GeometryCollection geometryCollection)
            throws FunctionExecutionException {
        Geometry[] geometry = new Geometry[geometryCollection.getNumGeometries()];
        for (int i = 0; i < geometry.length; ++i) {
            geometry[i] = transformGeometry(ct, geometryCollection.getGeometryN(i));
        }
        return geometryCollection.getFactory().createGeometryCollection(geometry);
    }
}
