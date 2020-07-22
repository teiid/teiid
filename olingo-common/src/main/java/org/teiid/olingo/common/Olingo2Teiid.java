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
package org.teiid.olingo.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.api.edm.geo.GeospatialCollection;
import org.apache.olingo.commons.api.edm.geo.LineString;
import org.apache.olingo.commons.api.edm.geo.MultiLineString;
import org.apache.olingo.commons.api.edm.geo.MultiPoint;
import org.apache.olingo.commons.api.edm.geo.MultiPolygon;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.apache.olingo.commons.api.edm.geo.Polygon;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.teiid.core.types.AbstractGeospatialType;
import org.teiid.core.types.GeographyType;
import org.teiid.query.function.GeometryUtils;

class Olingo2Teiid {

    public static AbstractGeospatialType convert(Geospatial geospatial, Class<?> expectedType, String srid) {
        Geometry result = convertToJTS(geospatial);
        if (geospatial.getSrid() != null && geospatial.getSrid().isNotDefault()) {
            srid = geospatial.getSrid().toString();
        }
        if (srid != null) {
            try {
                result.setSRID(Integer.valueOf(geospatial.getSrid().toString()));
            } catch (NumberFormatException e) {

            }
        }

        if (expectedType == GeographyType.class) {
            //assume normalization and valid srid?
            return GeometryUtils.getGeographyType(result);
        }
        return GeometryUtils.getGeometryType(result);
    }

    public static Geometry convertToJTS(Geospatial geospatial) {
        if (geospatial instanceof Point) {
            Point point = (Point) geospatial;
            org.locationtech.jts.geom.Point result =
                    GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(point.getX(), point.getY(), point.getZ()));
            return result;
        } else if (geospatial instanceof LineString) {
            LineString lineString = (LineString) geospatial;
            org.locationtech.jts.geom.LineString result =
                    GeometryUtils.GEOMETRY_FACTORY.createLineString(convertLineStringToPoints(lineString));
            return result;
        } else if (geospatial instanceof Polygon) {
            Polygon polygon = (Polygon) geospatial;
            return convertPolygon(polygon);
        } else if (geospatial instanceof MultiPoint) {
            MultiPoint multipoint = (MultiPoint)geospatial;
            Coordinate[] coords = convertLineStringToPoints(multipoint);
            org.locationtech.jts.geom.MultiPoint result = GeometryUtils.GEOMETRY_FACTORY.createMultiPoint(coords);
            return result;
        } else if (geospatial instanceof MultiLineString) {
            MultiLineString multiLineString = (MultiLineString)geospatial;
            List<org.locationtech.jts.geom.LineString> vals = new ArrayList<>();
            for (LineString lineString : multiLineString) {
                vals.add(GeometryUtils.GEOMETRY_FACTORY.createLineString(convertLineStringToPoints(lineString)));
            }
            return GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection(
                    vals.toArray(new org.locationtech.jts.geom.LineString[vals.size()]));
        } else if (geospatial instanceof MultiPolygon) {
            MultiPolygon multiPolygon = (MultiPolygon)geospatial;
            ArrayList<org.locationtech.jts.geom.Polygon> vals = new ArrayList<>();
            for (Polygon val : multiPolygon) {
                vals.add(convertPolygon(val));
            }
            return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(
                    vals.toArray(new org.locationtech.jts.geom.Polygon[vals.size()]));
        } else if (geospatial instanceof GeospatialCollection) {
            GeospatialCollection geometryCollection = (GeospatialCollection)geospatial;
            ArrayList<Geometry> vals = new ArrayList<>();
            for (Geospatial val : geometryCollection) {
                vals.add(convertToJTS(val));
            }
            return GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection(vals.toArray(new Geometry[vals.size()]));
        } else {
            throw new AssertionError(geospatial.getClass());
        }
    }

    static private org.locationtech.jts.geom.Polygon convertPolygon(
            Polygon polygon) throws AssertionError {
        Coordinate[] coords = convertLineStringToPoints(polygon.getExterior());
        LinearRing shell = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(coords);
        LinearRing[] holes = new LinearRing[polygon.getNumberOfInteriorRings()];
        for (int i = 0; i < polygon.getNumberOfInteriorRings(); i++) {
            holes[i] = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(convertLineStringToPoints(polygon.getInterior(i)));
        }
        return GeometryUtils.GEOMETRY_FACTORY.createPolygon(shell, holes);
    }

    static private Coordinate[] convertLineStringToPoints(Iterable<Point> points) {
        ArrayList<Coordinate> coords = new ArrayList<>(2);
        for (Point point : points) {
            coords.add(new Coordinate(point.getX(), point.getY(), point.getZ()));
        }
        return coords.toArray(new Coordinate[coords.size()]);
    }

}
