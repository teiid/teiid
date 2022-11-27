/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDouble;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.util.Assert;
import org.teiid.core.TeiidRuntimeException;

/**
 * Writes the Well-Known Text representation of a {@link Geometry}.
 * The Well-Known Text format is defined in the
 * OGC <A HREF="http://www.opengis.org/techno/specs.htm">
 * <p>
 * The <code>WKTWriter</code> outputs coordinates rounded to the precision
 * model. Only the maximum number of decimal places
 * necessary to represent the ordinates to the required precision will be
 * output.
 * <p>
 * The SFS WKT spec does not define a special tag for {@link LinearRing}s.
 * Under the spec, rings are output as <code>LINESTRING</code>s.
 * In order to allow precisely specifying constructed geometries,
 * JTS also supports a non-standard <code>LINEARRING</code> tag which is used
 * to output LinearRings.
 *
 * Forked from JTS to conform to OData BNF
 *
 * Relicensed to ASL as the work is reasonably distinct from the original
 * and implements a trivial mapping to OData WKT
 *
 */
class ODataWKTWriter
{

  private int outputDimension = 2;

  /**
   * Creates a new WKTWriter with default settings
   */
  public ODataWKTWriter()
  {
  }

  /**
   * Creates a writer that writes {@link Geometry}s with
   * the given output dimension (2 or 3).
   * If the specified output dimension is 3, the Z value
   * of coordinates will be written if it is present
   * (i.e. if it is not <code>Double.NaN</code>).
   *
   * @param outputDimension the coordinate dimension to output (2 or 3)
   */
  public ODataWKTWriter(int outputDimension) {
    this.outputDimension = outputDimension;

    if (outputDimension < 2 || outputDimension > 3)
      throw new IllegalArgumentException("Invalid output dimension (must be 2 or 3)");
  }

  /**
   *  Converts a <code>Geometry</code> to its Well-known Text representation.
   *
   *@param  geometry  a <code>Geometry</code> to process
   *@return           a <Geometry Tagged Text> string (see the OpenGIS Simple
   *      Features Specification)
   */
  public String write(Geometry geometry)
  {
    Writer sw = new StringWriter();
    try {
      write(geometry, sw);
    }
    catch (IOException ex) {
      Assert.shouldNeverReachHere();
    }
    return sw.toString();
  }

  /**
   *  Converts a <code>Geometry</code> to its Well-known Text representation.
   *
   *@param  geometry  a <code>Geometry</code> to process
   */
  public void write(Geometry geometry, Writer writer)
    throws IOException
  {
    if (geometry instanceof Point) {
      Point point = (Point) geometry;
      appendPointTaggedText(point.getCoordinate(), writer, point.getPrecisionModel());
    }
    else if (geometry instanceof LineString) {
      appendLineStringTaggedText((LineString) geometry, writer);
    }
    else if (geometry instanceof Polygon) {
      appendPolygonTaggedText((Polygon) geometry, writer);
    }
    else if (geometry instanceof MultiPoint) {
      appendMultiPointTaggedText((MultiPoint) geometry, writer);
    }
    else if (geometry instanceof MultiLineString) {
      appendMultiLineStringTaggedText((MultiLineString) geometry, writer);
    }
    else if (geometry instanceof MultiPolygon) {
      appendMultiPolygonTaggedText((MultiPolygon) geometry, writer);
    }
    else if (geometry instanceof GeometryCollection) {
      appendGeometryCollectionTaggedText((GeometryCollection) geometry, writer);
    }
    else {
      Assert.shouldNeverReachHere("Unsupported Geometry implementation:"
           + geometry.getClass());
    }
  }

  /**
   *  Converts a <code>Coordinate</code> to &lt;Point Tagged Text&gt; format,
   *  then appends it to the writer.
   *
   *@param  coordinate      the <code>Coordinate</code> to process
   *@param  writer          the output writer to append to
   *@param  precisionModel  the <code>PrecisionModel</code> to use to convert
   *      from a precise coordinate to an external coordinate
   */
  private void appendPointTaggedText(Coordinate coordinate, Writer writer,
      PrecisionModel precisionModel)
    throws IOException
  {
    writer.write("Point");
    appendPointText(coordinate, writer, precisionModel);
  }

  /**
   *  Converts a <code>LineString</code> to &lt;LineString Tagged Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  lineString  the <code>LineString</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendLineStringTaggedText(LineString lineString, Writer writer)
    throws IOException
  {
    writer.write("LineString");
    appendLineStringText(lineString, writer);
  }

  /**
   *  Converts a <code>Polygon</code> to &lt;Polygon Tagged Text&gt; format,
   *  then appends it to the writer.
   *
   *@param  polygon  the <code>Polygon</code> to process
   *@param  writer   the output writer to append to
   */
  private void appendPolygonTaggedText(Polygon polygon, Writer writer)
    throws IOException
  {
    writer.write("Polygon");
    appendPolygonText(polygon, writer);
  }

  /**
   *  Converts a <code>MultiPoint</code> to &lt;MultiPoint Tagged Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  multipoint  the <code>MultiPoint</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendMultiPointTaggedText(MultiPoint multipoint, Writer writer)
    throws IOException
  {
    writer.write("MultiPoint");
    appendMultiPointText(multipoint, writer);
  }

  /**
   *  Converts a <code>MultiLineString</code> to &lt;MultiLineString Tagged
   *  Text&gt; format, then appends it to the writer.
   *
   *@param  multiLineString  the <code>MultiLineString</code> to process
   *@param  writer           the output writer to append to
   */
  private void appendMultiLineStringTaggedText(MultiLineString multiLineString,
      Writer writer)
    throws IOException
  {
    writer.write("MultiLineString");
    appendMultiLineStringText(multiLineString, writer);
  }

  /**
   *  Converts a <code>MultiPolygon</code> to &lt;MultiPolygon Tagged Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  multiPolygon  the <code>MultiPolygon</code> to process
   *@param  writer        the output writer to append to
   */
  private void appendMultiPolygonTaggedText(MultiPolygon multiPolygon, Writer writer)
    throws IOException
  {
    writer.write("MultiPolygon");
    appendMultiPolygonText(multiPolygon, writer);
  }

  /**
   *  Converts a <code>GeometryCollection</code> to &lt;GeometryCollection
   *  Tagged Text&gt; format, then appends it to the writer.
   *
   *@param  geometryCollection  the <code>GeometryCollection</code> to process
   *@param  writer              the output writer to append to
   */
  private void appendGeometryCollectionTaggedText(GeometryCollection geometryCollection,
      Writer writer)
    throws IOException
  {
    writer.write("Collection");
    appendGeometryCollectionText(geometryCollection, writer);
  }

  /**
   *  Converts a <code>Coordinate</code> to &lt;Point Text&gt; format, then
   *  appends it to the writer.
   *
   *@param  coordinate      the <code>Coordinate</code> to process
   *@param  writer          the output writer to append to
   *@param  precisionModel  the <code>PrecisionModel</code> to use to convert
   *      from a precise coordinate to an external coordinate
   */
  private void appendPointText(Coordinate coordinate, Writer writer,
      PrecisionModel precisionModel)
    throws IOException
  {
      writer.write("(");
      if (coordinate != null) {
          appendCoordinate(coordinate, writer);
      }
      writer.write(")");
  }

  /**
   *  Converts a <code>Coordinate</code> to <code>&lt;Point&gt;</code> format,
   *  then appends it to the writer.
   *
   *@param  coordinate      the <code>Coordinate</code> to process
   *@param  writer          the output writer to append to
   */
  private void appendCoordinate(Coordinate coordinate, Writer writer)
    throws IOException
  {
    writer.write(writeNumber(coordinate.x) + " " + writeNumber(coordinate.y));
    if (outputDimension >= 3 && ! Double.isNaN(coordinate.z)) {
      writer.write(" ");
      writer.write(writeNumber(coordinate.z));
    }
  }

  /**
   *  Converts a <code>double</code> to a <code>String</code>, not in scientific
   *  notation.
   *
   *@param  d  the <code>double</code> to convert
   *@return    the <code>double</code> as a <code>String</code>, not in
   *      scientific notation
   */
  private String writeNumber(double d) {
    try {
        return EdmDouble.getInstance().valueToString(d, false, null, null, null, false);
    } catch (EdmPrimitiveTypeException e) {
        throw new TeiidRuntimeException(e);
    }
  }

  /**
   *  Converts a <code>LineString</code> to &lt;LineString Text&gt; format, then
   *  appends it to the writer.
   *
   *@param  lineString  the <code>LineString</code> to process
   * @param  writer      the output writer to append to
   */
  private void appendLineStringText(LineString lineString, Writer writer)
    throws IOException
  {
      writer.write("(");
      for (int i = 0; i < lineString.getNumPoints(); i++) {
        if (i > 0) {
          writer.write(",");
        }
        appendCoordinate(lineString.getCoordinateN(i), writer);
      }
      writer.write(")");
  }

  /**
   *  Converts a <code>Polygon</code> to &lt;Polygon Text&gt; format, then
   *  appends it to the writer.
   *
   *@param  polygon  the <code>Polygon</code> to process
   * @param  writer   the output writer to append to
   */
  private void appendPolygonText(Polygon polygon, Writer writer)
    throws IOException
  {
      writer.write("(");
      for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
        appendLineStringText(polygon.getInteriorRingN(i), writer);
        writer.write(",");
      }
      appendLineStringText(polygon.getExteriorRing(), writer);
      writer.write(")");
  }

  /**
   *  Converts a <code>MultiPoint</code> to &lt;MultiPoint Text&gt; format, then
   *  appends it to the writer.
   *
   *@param  multiPoint  the <code>MultiPoint</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendMultiPointText(MultiPoint multiPoint, Writer writer)
    throws IOException
  {
      writer.write("(");
      for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(",");
        }
        writer.write("(");
        appendCoordinate(((Point) multiPoint.getGeometryN(i)).getCoordinate(), writer);
        writer.write(")");
     }
      writer.write(")");
  }

  /**
   *  Converts a <code>MultiLineString</code> to &lt;MultiLineString Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  multiLineString  the <code>MultiLineString</code> to process
   *@param  writer           the output writer to append to
   */
  private void appendMultiLineStringText(MultiLineString multiLineString,
      Writer writer)
    throws IOException
  {
      writer.write("(");
      for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(",");
        }
        appendLineStringText((LineString) multiLineString.getGeometryN(i), writer);
      }
      writer.write(")");
  }

  /**
   *  Converts a <code>MultiPolygon</code> to &lt;MultiPolygon Text&gt; format,
   *  then appends it to the writer.
   *
   *@param  multiPolygon  the <code>MultiPolygon</code> to process
   *@param  writer        the output writer to append to
   */
  private void appendMultiPolygonText(MultiPolygon multiPolygon, Writer writer)
    throws IOException
  {
      writer.write("(");
      for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(",");
        }
        appendPolygonText((Polygon) multiPolygon.getGeometryN(i), writer);
      }
      writer.write(")");
  }

  /**
   *  Converts a <code>GeometryCollection</code> to &lt;GeometryCollectionText&gt;
   *  format, then appends it to the writer.
   *
   *@param  geometryCollection  the <code>GeometryCollection</code> to process
   *@param  writer              the output writer to append to
   */
  private void appendGeometryCollectionText(GeometryCollection geometryCollection,
      Writer writer)
    throws IOException
  {
      writer.write("(");
      for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(",");
        }
        write(geometryCollection.getGeometryN(i), writer);
      }
      writer.write(")");
  }

}