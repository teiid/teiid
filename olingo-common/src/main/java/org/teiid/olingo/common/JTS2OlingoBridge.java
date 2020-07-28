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
import org.apache.olingo.commons.api.edm.geo.Geospatial.Dimension;
import org.apache.olingo.commons.api.edm.geo.SRID;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

class JTS2OlingoBridge {
    private Dimension dimension;
    private SRID srid;

    public JTS2OlingoBridge(Dimension dimension, SRID srid) {
        this.dimension = dimension;
        this.srid = srid;
    }

    public Geospatial convert(Geometry geometry) {
        if (geometry instanceof Point) {
            Point point = (Point) geometry;
            org.apache.olingo.commons.api.edm.geo.Point result = new org.apache.olingo.commons.api.edm.geo.Point(
                    dimension, srid);
            result.setX(point.getX());
            result.setY(point.getY());
            return result;
        } else if (geometry instanceof LineString) {
            LineString lineString = (LineString) geometry;
            return convertLineString(lineString.getCoordinates());
        } else if (geometry instanceof Polygon) {
            Polygon polygon = (Polygon) geometry;
            return convertPolygon(polygon);
        } else if (geometry instanceof MultiPoint) {
            MultiPoint multipoint = (MultiPoint)geometry;
            List<org.apache.olingo.commons.api.edm.geo.Point> points = convertLineStringToPoints(multipoint.getCoordinates());
            org.apache.olingo.commons.api.edm.geo.MultiPoint result = new org.apache.olingo.commons.api.edm.geo.MultiPoint(dimension, srid, points);
            return result;
        } else if (geometry instanceof MultiLineString) {
            MultiLineString multiLineString = (MultiLineString)geometry;
            List<org.apache.olingo.commons.api.edm.geo.LineString> lineStrings = new ArrayList<>(multiLineString.getNumGeometries());
            for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
                LineString lineString = (LineString)multiLineString.getGeometryN(i);
                lineStrings.add(convertLineString(lineString.getCoordinates()));
            }
            org.apache.olingo.commons.api.edm.geo.MultiLineString result = new org.apache.olingo.commons.api.edm.geo.MultiLineString(dimension, srid, lineStrings);
            return result;
        } else if (geometry instanceof MultiPolygon) {
            MultiPolygon multiPolygon = (MultiPolygon)geometry;
            List<org.apache.olingo.commons.api.edm.geo.Polygon> polygons = new ArrayList<>(multiPolygon.getNumGeometries());
            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                Polygon polygon = (Polygon)multiPolygon.getGeometryN(i);
                polygons.add(convertPolygon(polygon));
            }
            org.apache.olingo.commons.api.edm.geo.MultiPolygon result = new org.apache.olingo.commons.api.edm.geo.MultiPolygon(dimension, srid, polygons);
            return result;
        } else if (geometry instanceof GeometryCollection) {
            GeometryCollection geometryCollection = (GeometryCollection)geometry;
            List<org.apache.olingo.commons.api.edm.geo.Geospatial> geometries = new ArrayList<>(geometryCollection.getNumGeometries());
            for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
                Geometry geo = geometryCollection.getGeometryN(i);
                geometries.add(convert(geo));
            }
            org.apache.olingo.commons.api.edm.geo.GeospatialCollection result = new org.apache.olingo.commons.api.edm.geo.GeospatialCollection(dimension, srid, geometries);
            return result;
        } else {
            throw new AssertionError(geometry.getClass());
        }
    }

    private org.apache.olingo.commons.api.edm.geo.Polygon convertPolygon(
            Polygon polygon) throws AssertionError {
        List<org.apache.olingo.commons.api.edm.geo.LineString> interior = new ArrayList<>(polygon.getNumInteriorRing());
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            interior.add(convertLineString(polygon.getInteriorRingN(i).getCoordinates()));
        }
        org.apache.olingo.commons.api.edm.geo.LineString exterior = convertLineString(polygon.getExteriorRing().getCoordinates());
        return new org.apache.olingo.commons.api.edm.geo.Polygon(dimension, srid, interior, exterior);
    }

    private org.apache.olingo.commons.api.edm.geo.LineString convertLineString(
            Coordinate[] lineString) {
        ArrayList<org.apache.olingo.commons.api.edm.geo.Point> points = convertLineStringToPoints(
                lineString);
        return new org.apache.olingo.commons.api.edm.geo.LineString(dimension, srid, points);
    }

    private ArrayList<org.apache.olingo.commons.api.edm.geo.Point> convertLineStringToPoints(
            Coordinate[] lineString) {
        ArrayList<org.apache.olingo.commons.api.edm.geo.Point> points = new ArrayList<>(lineString.length);
        for (Coordinate c : lineString) {
            org.apache.olingo.commons.api.edm.geo.Point p = new org.apache.olingo.commons.api.edm.geo.Point(dimension, srid);
            p.setX(c.x);
            p.setY(c.y);
            points.add(p);
        }
        return points;
    }

}
