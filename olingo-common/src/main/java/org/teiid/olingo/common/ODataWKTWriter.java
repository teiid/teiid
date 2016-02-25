/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */
package org.teiid.olingo.common;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDouble;
import org.teiid.core.TeiidRuntimeException;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.util.Assert;

/**
 * Writes the Well-Known Text representation of a {@link Geometry}.
 * The Well-Known Text format is defined in the
 * OGC <A HREF="http://www.opengis.org/techno/specs.htm">
 * <i>Simple Features Specification for SQL</i></A>.
 * See {@link WKTReader} for a formal specification of the format syntax.
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
 */
class ODataWKTWriter
{

  private int outputDimension = 2;
  private boolean isFormatted = false;

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
      writeFormatted(geometry, isFormatted, sw);
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
    writeFormatted(geometry, false, writer);
  }

  /**
   *  Same as <code>write</code>, but with newlines and spaces to make the
   *  well-known text more readable.
   *
   *@param  geometry  a <code>Geometry</code> to process
   *@return           a <Geometry Tagged Text> string (see the OpenGIS Simple
   *      Features Specification), with newlines and spaces
   */
  public String writeFormatted(Geometry geometry)
  {
    Writer sw = new StringWriter();
    try {
      writeFormatted(geometry, true, sw);
    }
    catch (IOException ex) {
      Assert.shouldNeverReachHere();
    }
    return sw.toString();
  }
  /**
   *  Same as <code>write</code>, but with newlines and spaces to make the
   *  well-known text more readable.
   *
   *@param  geometry  a <code>Geometry</code> to process
   */
  public void writeFormatted(Geometry geometry, Writer writer)
    throws IOException
  {
    writeFormatted(geometry, true, writer);
  }
  /**
   *  Converts a <code>Geometry</code> to its Well-known Text representation.
   *
   *@param  geometry  a <code>Geometry</code> to process
   */
  private void writeFormatted(Geometry geometry, boolean useFormatting, Writer writer)
    throws IOException
  {
    appendGeometryTaggedText(geometry, 0, writer);
  }


  /**
   *  Converts a <code>Geometry</code> to &lt;Geometry Tagged Text&gt; format,
   *  then appends it to the writer.
   *
   *@param  geometry  the <code>Geometry</code> to process
   *@param  writer    the output writer to append to
   */
  private void appendGeometryTaggedText(Geometry geometry, int level, Writer writer)
    throws IOException
  {
    if (geometry instanceof Point) {
      Point point = (Point) geometry;
      appendPointTaggedText(point.getCoordinate(), level, writer, point.getPrecisionModel());
    }
    else if (geometry instanceof LineString) {
      appendLineStringTaggedText((LineString) geometry, level, writer);
    }
    else if (geometry instanceof Polygon) {
      appendPolygonTaggedText((Polygon) geometry, level, writer);
    }
    else if (geometry instanceof MultiPoint) {
      appendMultiPointTaggedText((MultiPoint) geometry, level, writer);
    }
    else if (geometry instanceof MultiLineString) {
      appendMultiLineStringTaggedText((MultiLineString) geometry, level, writer);
    }
    else if (geometry instanceof MultiPolygon) {
      appendMultiPolygonTaggedText((MultiPolygon) geometry, level, writer);
    }
    else if (geometry instanceof GeometryCollection) {
      appendGeometryCollectionTaggedText((GeometryCollection) geometry, level, writer);
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
  private void appendPointTaggedText(Coordinate coordinate, int level, Writer writer,
      PrecisionModel precisionModel)
    throws IOException
  {
    writer.write("Point");
    appendPointText(coordinate, level, writer, precisionModel);
  }

  /**
   *  Converts a <code>LineString</code> to &lt;LineString Tagged Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  lineString  the <code>LineString</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendLineStringTaggedText(LineString lineString, int level, Writer writer)
    throws IOException
  {
    writer.write("LineString");
    appendLineStringText(lineString, level, false, writer);
  }

  /**
   *  Converts a <code>Polygon</code> to &lt;Polygon Tagged Text&gt; format,
   *  then appends it to the writer.
   *
   *@param  polygon  the <code>Polygon</code> to process
   *@param  writer   the output writer to append to
   */
  private void appendPolygonTaggedText(Polygon polygon, int level, Writer writer)
    throws IOException
  {
    writer.write("Polygon");
    appendPolygonText(polygon, level, false, writer);
  }

  /**
   *  Converts a <code>MultiPoint</code> to &lt;MultiPoint Tagged Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  multipoint  the <code>MultiPoint</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendMultiPointTaggedText(MultiPoint multipoint, int level, Writer writer)
    throws IOException
  {
    writer.write("MultiPoint");
    appendMultiPointText(multipoint, level, writer);
  }

  /**
   *  Converts a <code>MultiLineString</code> to &lt;MultiLineString Tagged
   *  Text&gt; format, then appends it to the writer.
   *
   *@param  multiLineString  the <code>MultiLineString</code> to process
   *@param  writer           the output writer to append to
   */
  private void appendMultiLineStringTaggedText(MultiLineString multiLineString, int level,
      Writer writer)
    throws IOException
  {
    writer.write("MultiLineString");
    appendMultiLineStringText(multiLineString, level, false, writer);
  }

  /**
   *  Converts a <code>MultiPolygon</code> to &lt;MultiPolygon Tagged Text&gt;
   *  format, then appends it to the writer.
   *
   *@param  multiPolygon  the <code>MultiPolygon</code> to process
   *@param  writer        the output writer to append to
   */
  private void appendMultiPolygonTaggedText(MultiPolygon multiPolygon, int level, Writer writer)
    throws IOException
  {
    writer.write("MultiPolygon");
    appendMultiPolygonText(multiPolygon, level, writer);
  }

  /**
   *  Converts a <code>GeometryCollection</code> to &lt;GeometryCollection
   *  Tagged Text&gt; format, then appends it to the writer.
   *
   *@param  geometryCollection  the <code>GeometryCollection</code> to process
   *@param  writer              the output writer to append to
   */
  private void appendGeometryCollectionTaggedText(GeometryCollection geometryCollection, int level,
      Writer writer)
    throws IOException
  {
    writer.write("Collection");
    appendGeometryCollectionText(geometryCollection, level, writer);
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
  private void appendPointText(Coordinate coordinate, int level, Writer writer,
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
   * Appends the i'th coordinate from the sequence to the writer
   *
   * @param  seq  the <code>CoordinateSequence</code> to process
   * @param i     the index of the coordinate to write
   * @param  writer the output writer to append to
   */
  private void appendCoordinate(CoordinateSequence seq, int i, Writer writer)
      throws IOException
  {
    writer.write(writeNumber(seq.getX(i)) + " " + writeNumber(seq.getY(i)));
    if (outputDimension >= 3 && seq.getDimension() >= 3) {
      double z = seq.getOrdinate(i, 3);
      if (! Double.isNaN(z)) {
        writer.write(" ");
        writer.write(writeNumber(z));
      }
    }
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
   *@param  writer      the output writer to append to
   */
  private void appendLineStringText(LineString lineString, int level, boolean doIndent, Writer writer)
    throws IOException
  {
      writer.write("(");
      for (int i = 0; i < lineString.getNumPoints(); i++) {
        if (i > 0) {
          writer.write(", ");
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
   *@param  writer   the output writer to append to
   */
  private void appendPolygonText(Polygon polygon, int level, boolean indentFirst, Writer writer)
    throws IOException
  {
      writer.write("(");
      appendLineStringText(polygon.getExteriorRing(), level, false, writer);
      for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
        writer.write(", ");
        appendLineStringText(polygon.getInteriorRingN(i), level + 1, true, writer);
      }
      writer.write(")");
  }

  /**
   *  Converts a <code>MultiPoint</code> to &lt;MultiPoint Text&gt; format, then
   *  appends it to the writer.
   *
   *@param  multiPoint  the <code>MultiPoint</code> to process
   *@param  writer      the output writer to append to
   */
  private void appendMultiPointText(MultiPoint multiPoint, int level, Writer writer)
    throws IOException
  {
      writer.write("(");
      for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(", ");
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
  private void appendMultiLineStringText(MultiLineString multiLineString, int level, boolean indentFirst,
      Writer writer)
    throws IOException
  {
      int level2 = level;
      boolean doIndent = indentFirst;
      writer.write("(");
      for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(", ");
          level2 = level + 1;
          doIndent = true;
        }
        appendLineStringText((LineString) multiLineString.getGeometryN(i), level2, doIndent, writer);
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
  private void appendMultiPolygonText(MultiPolygon multiPolygon, int level, Writer writer)
    throws IOException
  {
      int level2 = level;
      boolean doIndent = false;
      writer.write("(");
      for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(", ");
          level2 = level + 1;
          doIndent = true;
        }
        appendPolygonText((Polygon) multiPolygon.getGeometryN(i), level2, doIndent, writer);
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
  private void appendGeometryCollectionText(GeometryCollection geometryCollection, int level,
      Writer writer)
    throws IOException
  {
      int level2 = level;
      writer.write("(");
      for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
        if (i > 0) {
          writer.write(", ");
          level2 = level + 1;
        }
        appendGeometryTaggedText(geometryCollection.getGeometryN(i), level2, writer);
      }
      writer.write(")");
  }

}