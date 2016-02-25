/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.teiid.olingo.common;

import java.util.Collections;
import java.util.Iterator;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.api.edm.geo.GeospatialCollection;
import org.apache.olingo.commons.api.edm.geo.LineString;
import org.apache.olingo.commons.api.edm.geo.MultiLineString;
import org.apache.olingo.commons.api.edm.geo.MultiPoint;
import org.apache.olingo.commons.api.edm.geo.MultiPolygon;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.apache.olingo.commons.api.edm.geo.Polygon;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDouble;

/**
 * Taken from org.apache.olingo.client.core.serialization to avoid the dependency on olingo client
 */
class AtomGeoValueSerializer {

  private void points(final XMLStreamWriter writer, final Iterator<Point> itor, final boolean wrap)
      throws XMLStreamException {

    while (itor.hasNext()) {
      final Point point = itor.next();

      if (wrap) {
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_POINT, Constants.NS_GML);
      }

      writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_POS, Constants.NS_GML);
      try {
        writer.writeCharacters(EdmDouble.getInstance().valueToString(point.getX(), null, null,
            Constants.DEFAULT_PRECISION, Constants.DEFAULT_SCALE, null)
            + " "
            + EdmDouble.getInstance().valueToString(point.getY(), null, null,
                Constants.DEFAULT_PRECISION, Constants.DEFAULT_SCALE, null));
      } catch (EdmPrimitiveTypeException e) {
        throw new XMLStreamException("While serializing point coordinates as double", e);
      }
      writer.writeEndElement();

      if (wrap) {
        writer.writeEndElement();
      }
    }
  }

  private void lineStrings(final XMLStreamWriter writer, final Iterator<LineString> itor, final boolean wrap)
      throws XMLStreamException {

    while (itor.hasNext()) {
      final LineString lineString = itor.next();

      if (wrap) {
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_LINESTRING, Constants.NS_GML);
      }

      points(writer, lineString.iterator(), false);

      if (wrap) {
        writer.writeEndElement();
      }
    }
  }

  private void polygons(final XMLStreamWriter writer, final Iterator<Polygon> itor, final boolean wrap)
      throws XMLStreamException {

    while (itor.hasNext()) {
      final Polygon polygon = itor.next();

      if (wrap) {
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_POLYGON, Constants.NS_GML);
      }

      if (!polygon.getExterior().isEmpty()) {
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_POLYGON_EXTERIOR, Constants.NS_GML);
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_POLYGON_LINEARRING, Constants.NS_GML);

        points(writer, polygon.getExterior().iterator(), false);

        writer.writeEndElement();
        writer.writeEndElement();
      }
      if (!polygon.getInterior().isEmpty()) {
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_POLYGON_INTERIOR, Constants.NS_GML);
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_POLYGON_LINEARRING, Constants.NS_GML);

        points(writer, polygon.getInterior().iterator(), false);

        writer.writeEndElement();
        writer.writeEndElement();
      }

      if (wrap) {
        writer.writeEndElement();
      }
    }
  }

  private void writeSrsName(final XMLStreamWriter writer, final Geospatial value) throws XMLStreamException {
    if (value.getSrid() != null && value.getSrid().isNotDefault()) {
      writer.writeAttribute(Constants.PREFIX_GML, Constants.NS_GML, Constants.ATTR_SRSNAME,
          Constants.SRS_URLPREFIX + value.getSrid().toString());
    }
  }

  public void serialize(final XMLStreamWriter writer, final Geospatial value) throws XMLStreamException {
    switch (value.getEdmPrimitiveTypeKind()) {
    case GeographyPoint:
    case GeometryPoint:
      writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_POINT, Constants.NS_GML);
      writeSrsName(writer, value);

      points(writer, Collections.singleton((Point) value).iterator(), false);

      writer.writeEndElement();
      break;

    case GeometryMultiPoint:
    case GeographyMultiPoint:
      writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_MULTIPOINT, Constants.NS_GML);
      writeSrsName(writer, value);

      if (!((MultiPoint) value).isEmpty()) {
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_POINTMEMBERS, Constants.NS_GML);
        points(writer, ((MultiPoint) value).iterator(), true);
        writer.writeEndElement();
      }

      writer.writeEndElement();
      break;

    case GeometryLineString:
    case GeographyLineString:
      writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_LINESTRING, Constants.NS_GML);
      writeSrsName(writer, value);

      lineStrings(writer, Collections.singleton((LineString) value).iterator(), false);

      writer.writeEndElement();
      break;

    case GeometryMultiLineString:
    case GeographyMultiLineString:
      writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_MULTILINESTRING, Constants.NS_GML);
      writeSrsName(writer, value);

      if (!((MultiLineString) value).isEmpty()) {
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_LINESTRINGMEMBERS, Constants.NS_GML);
        lineStrings(writer, ((MultiLineString) value).iterator(), true);
        writer.writeEndElement();
      }

      writer.writeEndElement();
      break;

    case GeographyPolygon:
    case GeometryPolygon:
      writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_POLYGON, Constants.NS_GML);
      writeSrsName(writer, value);

      polygons(writer, Collections.singleton(((Polygon) value)).iterator(), false);

      writer.writeEndElement();
      break;

    case GeographyMultiPolygon:
    case GeometryMultiPolygon:
      writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_MULTIPOLYGON, Constants.NS_GML);
      writeSrsName(writer, value);

      if (!((MultiPolygon) value).isEmpty()) {
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_SURFACEMEMBERS, Constants.NS_GML);
        polygons(writer, ((MultiPolygon) value).iterator(), true);
        writer.writeEndElement();
      }

      writer.writeEndElement();
      break;

    case GeographyCollection:
    case GeometryCollection:
      writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_GEOCOLLECTION, Constants.NS_GML);
      writeSrsName(writer, value);

      if (!((GeospatialCollection) value).isEmpty()) {
        writer.writeStartElement(Constants.PREFIX_GML, Constants.ELEM_GEOMEMBERS, Constants.NS_GML);
        for (Geospatial geospatial : ((GeospatialCollection) value)) {
          serialize(writer, geospatial);
        }
        writer.writeEndElement();
      }

      writer.writeEndElement();
      break;

    default:
    }
  }

}
