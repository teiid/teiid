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

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.SQLException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.InputStreamInStream;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.gml2.GMLHandler;
import org.locationtech.jts.io.gml2.GMLWriter;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.teiid.CommandContext;
import org.teiid.UserDefinedAggregate;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.AbstractGeospatialType;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeographyType;
import org.teiid.core.types.GeometryType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.translator.SourceSystemFunctions;
import org.xml.sax.Attributes;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Utility methods for geometry
 * TODO: determine if we should use buffermanager to minimize memory footprint
 */
public class GeometryUtils {

    public static enum Ordinate {
        X, Y, Z
    }

    public static GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private static final int SRID_4326 = 4326;

    private static String SRS_PREFIX = "EPSG:"; //$NON-NLS-1$

    public static ClobType geometryToClob(AbstractGeospatialType geometry,
                                          boolean withSrid)
            throws FunctionExecutionException {
        Geometry jtsGeometry = getGeometry(geometry);
        int srid = jtsGeometry.getSRID();
        StringBuilder geomText = new StringBuilder();
        if (withSrid && srid != GeometryType.UNKNOWN_SRID) {
            geomText.append("SRID=").append(jtsGeometry.getSRID()).append(";"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        geomText.append(jtsGeometry.toText());
        return new ClobType(new ClobImpl(geomText.toString()));
    }

    public static GeometryType geometryFromClob(ClobType wkt)
            throws FunctionExecutionException {
        return getGeometryType(geometryFromClob(wkt, GeometryType.UNKNOWN_SRID, false));
    }

    public static Geometry geometryFromClob(ClobType wkt, Integer srid, boolean allowEwkt)
            throws FunctionExecutionException {
        Reader r = null;
        try {
            WKTReader reader = new WKTReader(GEOMETRY_FACTORY);
            r = wkt.getCharacterStream();
            if (allowEwkt) {
                PushbackReader pbr = new PushbackReader(r, 1);
                r = pbr;
                char[] expected = new char[] {'s', 'r', 'i', 'd','='};
                int expectedIndex = 0;
                StringBuilder sridBuffer = null;
                for (int i = 0; i < 100000; i++) {
                    int charRead = pbr.read();
                    if (charRead == -1) {
                        break;
                    }
                    if (expectedIndex == expected.length) {
                        //parse srid
                        if (sridBuffer == null) {
                            sridBuffer = new StringBuilder(4);
                        }
                        if (charRead == ';') {
                            if (sridBuffer.length() == 0) {
                                pbr.unread(charRead);
                            }
                            break;
                        }
                        sridBuffer.append((char)charRead);
                        continue;
                    }
                    if (expectedIndex == 0 && Character.isWhitespace(charRead)) {
                        continue;
                    }
                    if (expected[expectedIndex] != Character.toLowerCase(charRead)) {
                        pbr.unread(charRead);
                        break;
                    }
                    expectedIndex++;
                }
                if (sridBuffer != null) {
                    srid = Integer.parseInt(sridBuffer.toString());
                }
            }
            Geometry jtsGeometry = reader.read(r);
            if (jtsGeometry == null) {
                //for some reason io and parse exceptions are caught in their logic if they occur on the first word, then null is returned
                throw new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31203));
            }
            if (!allowEwkt && (jtsGeometry.getSRID() != GeometryType.UNKNOWN_SRID || (jtsGeometry.getCoordinate() != null && !Double.isNaN(jtsGeometry.getCoordinate().z)))) {
                //don't allow ewkt that requires a specific function
                throw new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31160, "EWKT")); //$NON-NLS-1$
            }
            if (srid != null) {
                jtsGeometry.setSRID(srid);
            }
            return jtsGeometry;
        } catch (ParseException e) {
            throw new FunctionExecutionException(e);
        } catch (SQLException e) {
            throw new FunctionExecutionException(e);
        } catch (IOException e) {
            throw new FunctionExecutionException(e);
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public static ClobType geometryToGml(CommandContext ctx, GeometryType geometry,
                                         boolean withGmlPrefix)
            throws FunctionExecutionException {
        Geometry jtsGeometry = getGeometry(geometry);
        GMLWriter writer = new GMLWriter();

        if (!withGmlPrefix) {
            if (geometry.getSrid() != SRID_4326) {
                if (geometry.getSrid() == GeometryType.UNKNOWN_SRID) {
                    throw new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31161));
                }
                jtsGeometry = GeometryHelper.getInstance().transform(ctx, jtsGeometry, SRID_4326);
            }
            writer.setPrefix(null);
        } else if (geometry.getSrid() != GeometryType.UNKNOWN_SRID) {
            writer.setSrsName(SRS_PREFIX + geometry.getSrid());
        }
        String gmlText = writer.write(jtsGeometry);
        return new ClobType(new ClobImpl(gmlText));
    }

    public static GeometryType geometryFromGml(ClobType gml, Integer srid)
            throws FunctionExecutionException {
        try {
            Geometry geom = geometryFromGml(gml.getCharacterStream(), srid);
            return getGeometryType(geom);
        } catch (SQLException e) {
            throw new FunctionExecutionException(e);
        }
    }

    /**
     * Custom SAX handler extending GMLHandler to handle parsing SRIDs.
     *
     * The default JTS logic only handles srsName=int or srsName=uri/int
     * whereas other systems commonly use srsName=name:int
     */
    private static class GmlSridHandler extends GMLHandler {
        private int srid = GeometryType.UNKNOWN_SRID;

        public GmlSridHandler(GeometryFactory gf, ErrorHandler delegate) {
            super(gf, delegate);
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
                throws SAXException {
            String srsName = attributes.getValue("srsName"); //$NON-NLS-1$
            if (srsName != null) {
                String[] srsParts = srsName.split(":"); //$NON-NLS-1$
                try {
                    if (srsParts.length == 2) {
                        srid = Integer.parseInt(srsParts[1]);
                    }
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
            super.startElement(uri, localName, qName, attributes);
        }

        public int getSrid() {
            return srid;
        }
    }

    public static Geometry geometryFromGml(Reader reader, Integer srid)
            throws FunctionExecutionException {
        Geometry jtsGeometry = null;
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(false);
            factory.setValidating(false);
            SAXParser parser = factory.newSAXParser();

            GmlSridHandler handler = new GmlSridHandler(GEOMETRY_FACTORY, null);
            parser.parse(new InputSource(reader), handler);

            jtsGeometry = handler.getGeometry();

            if (srid == null) {
                if (jtsGeometry.getSRID() == GeometryType.UNKNOWN_SRID) {
                    jtsGeometry.setSRID(handler.getSrid());
                }
            } else {
                jtsGeometry.setSRID(srid);
            }
        } catch (IOException e) {
            throw new FunctionExecutionException(e);
        } catch (SAXException e) {
            throw new FunctionExecutionException(e);
        } catch (ParserConfigurationException e) {
            throw new FunctionExecutionException(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    // Nothing
                }
            }
        }
        return jtsGeometry;
    }

    public static GeometryType geometryFromBlob(BlobType wkb)
            throws FunctionExecutionException {
        return geometryFromBlob(wkb, GeometryType.UNKNOWN_SRID);
    }

    //TODO: should allow an option to assume well formed
    public static GeometryType geometryFromBlob(BlobType wkb, int srid) throws FunctionExecutionException {
        //return as geometry
        GeometryType gt = new GeometryType(wkb.getReference(), srid);

        //validate
        getGeometry(gt);
        return gt;
    }

    public static Boolean intersects(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return g1.intersects(g2);
    }

    public static Boolean contains(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return g1.contains(g2);
    }

    public static Boolean disjoint(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return g1.disjoint(g2);
    }

    public static Boolean crosses(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return g1.crosses(g2);
    }

    public static Double distance(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return g1.distance(g2);
    }

    public static Boolean touches(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return g1.touches(g2);
    }

    public static Boolean overlaps(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return g1.overlaps(g2);
    }

    public static GeometryType getGeometryType(Geometry jtsGeom) {
        return getGeometryType(jtsGeom, jtsGeom.getSRID());
    }

    public static GeometryType getGeometryType(Geometry jtsGeom, int srid) {
        jtsGeom.setSRID(srid);
        byte[] bytes = getBytes(jtsGeom, true);
        GeometryType result = new GeometryType(bytes, srid);
        result.setGeoCache(jtsGeom);
        return result;
    }

    public static GeographyType getGeographyType(Geometry geom, CommandContext ctx) throws FunctionExecutionException {
        if (geom.getSRID() == GeometryType.UNKNOWN_SRID) {
            geom.setSRID(GeographyType.DEFAULT_SRID);
        }
        if (!GeometryHelper.getInstance().isLatLong(ctx, geom.getSRID())) {
            throw new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31290, geom.getSRID()));
        }
        geom.apply(new CoordinateFilter() {

            @Override
            public void filter(Coordinate coord) {
                if (coord.x > 180) {
                    ctx.addWarning(new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31291)));
                    coord.x = ((coord.x+180)%360) - 180;
                } else if (coord.x < -180) {
                    ctx.addWarning(new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31291)));
                    coord.x = ((coord.x+180)%360) + 180;
                }
                if (coord.y > 90) {
                    ctx.addWarning(new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31291)));
                    coord.y = (((coord.y/90)%2>=1?90:0)-(coord.y%90))*((((coord.y-90)/180)%2>=1)?-1:1);
                } else if (coord.y < -90) {
                    ctx.addWarning(new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31291)));
                    coord.y = (((coord.y/90)%2<=-1?-90:0)-(coord.y%90))*((((coord.y+90)/180)%2<=-1)?-1:1);
                }
            }
        });
        GeographyType result = getGeographyType(geom);
        return result;
    }

    public static GeographyType getGeographyType(Geometry geom) {
        byte[] bytes = getBytes(geom, true);
        GeographyType result = new GeographyType(bytes, geom.getSRID());
        result.setGeoCache(geom);
        return result;
    }

    public static byte[] getBytes(Geometry jtsGeom, boolean bigEndian) {
        WKBWriter writer = new WKBWriter(2, bigEndian?ByteOrderValues.BIG_ENDIAN:ByteOrderValues.LITTLE_ENDIAN);
        return writer.write(jtsGeom);
    }

    public static Geometry getGeometry(AbstractGeospatialType geom)
            throws FunctionExecutionException {
        Object value = geom.getGeoCache();
        if (value instanceof Geometry) {
            return (Geometry)value;
        }

        try {
            Geometry result = getGeometry(geom.getBinaryStream(), geom.getSrid(), false);
            geom.setGeoCache(result);
            return result;
        } catch (SQLException e) {
            throw new FunctionExecutionException(e);
        }
    }

    public static Geometry getGeometry(InputStream is1, Integer srid, boolean allowEwkb)
            throws FunctionExecutionException {
        try {
            WKBReader reader = new WKBReader();
            Geometry jtsGeom = reader.read(new InputStreamInStream(is1));
            if (!allowEwkb && (jtsGeom.getSRID() != GeometryType.UNKNOWN_SRID || (jtsGeom.getCoordinate() != null && !Double.isNaN(jtsGeom.getCoordinate().z)))) {
                //don't allow ewkb - that needs an explicit function
                throw new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31160, "EWKB")); //$NON-NLS-1$
            }
            if (srid != null) {
                jtsGeom.setSRID(srid);
            }
            return jtsGeom;
        } catch (ParseException e) {
            throw new FunctionExecutionException(e);
        } catch (IOException e) {
            throw new FunctionExecutionException(e);
        } finally {
            if (is1 != null) {
                try {
                    is1.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public static Boolean equals(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        return getGeometry(geom1).equalsTopo(getGeometry(geom2));
    }

    public static GeometryType geometryFromEwkb(InputStream is, Integer srid) throws FunctionExecutionException {
        Geometry geom = getGeometry(is, srid, true);
        return getGeometryType(geom);
    }

    public static GeographyType geographyFromEwkb(CommandContext ctx, InputStream is) throws FunctionExecutionException {
        Geometry geom = getGeometry(is, null, true);
        return getGeographyType(geom, ctx);
    }

    public static GeometryType simplify(
            GeometryType geom, double tolerance) throws FunctionExecutionException {
        DouglasPeuckerSimplifier douglasPeuckerSimplifier = new DouglasPeuckerSimplifier(getGeometry(geom));
        douglasPeuckerSimplifier.setEnsureValid(false);
        douglasPeuckerSimplifier.setDistanceTolerance(tolerance);
        Geometry resultGeometry = douglasPeuckerSimplifier.getResultGeometry();
        return getGeometryType(resultGeometry, geom.getSrid());
    }

    public static GeometryType simplifyPreserveTopology(
            GeometryType geom, double tolerance) throws FunctionExecutionException {
        return getGeometryType(TopologyPreservingSimplifier.simplify(getGeometry(geom), tolerance), geom.getSrid());
    }

    public static boolean boundingBoxIntersects(
            GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return g1.getEnvelope().intersects(g2.getEnvelope());
    }

    public static GeometryType envelope(GeometryType geom) throws FunctionExecutionException {
        return getGeometryType(getGeometry(geom).getEnvelope(), geom.getSrid());
    }

    public static Boolean within(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return g1.within(g2);
    }

    public static Boolean dwithin(GeometryType geom1, GeometryType geom2, double distance) throws FunctionExecutionException {
        return distance(geom1, geom2) < distance;
    }

    public static class Extent implements UserDefinedAggregate<GeometryType> {

        private Envelope e;
        private int srid = GeometryType.UNKNOWN_SRID;

        public Extent() {
        }

        @Override
        public void reset() {
            e = null;
        }

        @TeiidFunction(name=SourceSystemFunctions.ST_EXTENT, category=FunctionCategoryConstants.GEOMETRY)
        public void addInput(GeometryType geom) throws FunctionExecutionException {
            srid = geom.getSrid();
            Geometry g1 = getGeometry(geom);
            if (e == null) {
                e = new Envelope();
            }
            e.expandToInclude(g1.getEnvelopeInternal());
        }

        @Override
        public GeometryType getResult(CommandContext commandContext) {
            if (e == null) {
                return null;
            }
            //created a closed polygon box result
            return getGeometryType(GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
                    new Coordinate(e.getMinX(), e.getMinY()),
                    new Coordinate(e.getMinX(), e.getMaxY()),
                    new Coordinate(e.getMaxX(), e.getMaxY()),
                    new Coordinate(e.getMaxX(), e.getMinY()),
                    new Coordinate(e.getMinX(), e.getMinY())}), srid);
        }

    }

    /**
     * We'll take the wkb format and add the extended flag/srid
     * @param geometry
     * @return
     */
    public static BlobType geometryToEwkb(final AbstractGeospatialType geometry) {
        final Blob b = geometry.getReference();
        BlobImpl blobImpl = new BlobImpl(new InputStreamFactory() {

            @Override
            public InputStream getInputStream() throws IOException {
                PushbackInputStream pbis;
                try {
                    pbis = new PushbackInputStream(b.getBinaryStream(), 9);
                } catch (SQLException e) {
                    throw new IOException(e);
                }
                int byteOrder = pbis.read();
                if (byteOrder == -1) {
                    return pbis;
                }
                byte[] typeInt = new byte[4];
                int bytesRead = pbis.read(typeInt);
                if (bytesRead == 4) {
                    int srid = geometry.getSrid();
                    byte[] sridInt = new byte[4];
                    ByteOrderValues.putInt(srid, sridInt, byteOrder==0?ByteOrderValues.BIG_ENDIAN:ByteOrderValues.LITTLE_ENDIAN);
                    pbis.unread(sridInt);
                    typeInt[byteOrder==0?0:3] |= 0x20;
                }
                pbis.unread(typeInt, 0, bytesRead);
                pbis.unread(byteOrder);
                return pbis;
            }
        });
        return new BlobType(blobImpl);
    }

    public static double area(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return g.getArea();
    }

    public static GeometryType boundary(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return getGeometryType(g.getBoundary(), geom.getSrid());
    }

    public static GeometryType buffer(GeometryType geom, double distance)  throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return getGeometryType(g.buffer(distance), geom.getSrid());
    }

    public static GeometryType buffer(GeometryType geom, double distance, int quadrantSegments)  throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return getGeometryType(g.buffer(distance, quadrantSegments), geom.getSrid());
    }

    public static GeometryType centroid(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return getGeometryType(g.getCentroid(), geom.getSrid());
    }

    public static GeometryType convexHull(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return getGeometryType(g.convexHull(), geom.getSrid());
    }

    public static Integer coordDim(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        Coordinate c = g.getCoordinate();
        if (c != null && !Double.isNaN(c.z)) {
            return 3;
        }
        return 2;
    }

    public static GeometryType difference(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return getGeometryType(g1.difference(g2), geom1.getSrid());
    }

    public static GeometryType intersection(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return getGeometryType(g1.intersection(g2), geom1.getSrid());
    }

    public static int dimension(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return g.getDimension();
    }

    public static GeometryType startEndPoint(GeometryType geom, boolean start) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (g instanceof LineString) {
            LineString lineString = (LineString)g;
            Geometry p = null;
            if (start) {
                p = lineString.getStartPoint();
            } else {
                p = lineString.getEndPoint();
            }
            if (p == null) {
                //can be empty
                return null;
            }
            return getGeometryType(p, geom.getSrid());
        }
        return null;
    }

    public static GeometryType exteriorRing(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (!(g instanceof Polygon)) {
            return null;
        }
        return getGeometryType(((Polygon)g).getExteriorRing(), geom.getSrid());
    }

    public static GeometryType geometryN(GeometryType geom, int index) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        int num = g.getNumGeometries();
        if (index < 0 || index >= num) {
            return null;
        }
        Geometry n = g.getGeometryN(index);
        return getGeometryType(n, geom.getSrid());
    }

    public static String geometryType(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return "ST_" + g.getGeometryType(); //$NON-NLS-1$
    }

    public static Boolean isClosed(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (!(g instanceof LineString)) {
            return false;
        }
        LineString lineString = ((LineString)g);
        return lineString.isClosed();
    }

    public static Boolean isEmpty(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return g.isEmpty();
    }

    public static Boolean isRing(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (!(g instanceof LineString)) {
            return false;
        }
        LineString lineString = ((LineString)g);
        return lineString.isClosed() && lineString.isSimple();
    }

    public static Boolean isSimple(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return g.isSimple();
    }

    public static Boolean isValid(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return g.isValid();
    }

    public static Double length(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (g instanceof LineString || g instanceof MultiLineString) {
            return g.getLength();
        }
        return 0.0;
    }

    public static Integer numInteriorRings(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (!(g instanceof Polygon)) {
            return null;
        }
        return ((Polygon)g).getNumInteriorRing();
    }

    public static int numGeometries(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return g.getNumGeometries();
    }

    public static int numPoints(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        return g.getNumPoints();
    }

    public static GeometryType interiorRingN(GeometryType geom, int i) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (!(g instanceof Polygon)) {
            return null;
        }
        Polygon g2 = (Polygon)g;
        if (i < 0 || i >= g2.getNumInteriorRing()) {
            return null;
        }
        return getGeometryType(g2.getInteriorRingN(i), geom.getSrid());
    }

    public static Boolean orderingEquals(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        return getGeometry(geom1).equalsExact(getGeometry(geom2));
    }

    public static GeometryType point(double x, double y) {
        return getGeometryType(GEOMETRY_FACTORY.createPoint(new Coordinate(x, y)));
    }

    public static GeometryType pointN(GeometryType geom, int i) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (!(g instanceof LineString)) {
            return null;
        }
        LineString g2 = (LineString)g;
        if (i < 0 || i >= g2.getNumPoints()) {
            return null;
        }
        return getGeometryType(g2.getPointN(i), geom.getSrid());
    }

    public static Double perimeter(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (g instanceof Polygon || g instanceof MultiPolygon) {
            return g.getLength();
        }
        return 0.0;
    }

    public static GeometryType pointOnSurface(GeometryType geom) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        Point point = g.getInteriorPoint();
        if (point == null) {
            return null;
        }
        return getGeometryType(point, geom.getSrid());
    }

    public static GeometryType polygon(GeometryType geom, int srid) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (!(g instanceof LineString)) {
            throw new FunctionExecutionException(QueryPlugin.Event.TEIID31207, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31207));
        }
        LineString ls = (LineString)g;
        Geometry result = GEOMETRY_FACTORY.createPolygon(ls.getCoordinateSequence());
        return getGeometryType(result, srid);
    }

    public static Boolean relate(GeometryType geom1, GeometryType geom2, String intersectionPattern) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return g1.relate(g2, intersectionPattern);
    }

    public static String relate(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        IntersectionMatrix im = g1.relate(g2);
        return im.toString();
    }

    public static GeometryType symDifference(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return getGeometryType(g1.symDifference(g2), geom1.getSrid());
    }

    public static GeometryType union(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
        Geometry g1 = getGeometry(geom1);
        Geometry g2 = getGeometry(geom2);
        return getGeometryType(g1.union(g2), geom1.getSrid());
    }

    public static Double ordinate(GeometryType geom, Ordinate ordinate) throws FunctionExecutionException {
        Geometry g = getGeometry(geom);
        if (!(g instanceof Point)) {
            throw new FunctionExecutionException(QueryPlugin.Event.TEIID31208, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31208));
        }
        Point p = (Point)g;
        Coordinate c = p.getCoordinate();
        if (c == null) {
            return null;
        }
        double value = c.getOrdinate(ordinate.ordinal());
        if (Double.isNaN(value)) {
            return null;
        }
        return value;
    }

    public static GeometryType makeEnvelope(double xmin, double ymin,
            double xmax, double ymax, Integer srid) {
        Geometry geom = GEOMETRY_FACTORY.createPolygon(new Coordinate[] {new Coordinate(xmin, ymin), new Coordinate(xmin, ymax), new Coordinate(xmax, ymax), new Coordinate(xmax, ymin), new Coordinate(xmin, ymin)});
        if (srid != null) {
            geom.setSRID(srid);
        }
        return getGeometryType(geom);
    }

    public static GeometryType snapToGrid(GeometryType geom, double size) throws FunctionExecutionException {
        if (size == 0) {
            return geom;
        }
        Geometry g1 = getGeometry(geom);
        PrecisionModel precisionModel = new PrecisionModel(1/size);
        GeometryPrecisionReducer reducer = new GeometryPrecisionReducer(precisionModel);
        reducer.setPointwise(true);
        reducer.setChangePrecisionModel(true);
        Geometry result = reducer.reduce(g1);
        //since the wkb writer doesn't consider precision, we have to first write/read through wkt
        WKTWriter writer = new WKTWriter();
        String val = writer.write(result);
        WKTReader reader = new WKTReader(GEOMETRY_FACTORY);
        try {
            result = reader.read(new StringReader(val));
        } catch (ParseException e) {
            throw new FunctionExecutionException(e);
        }
        return getGeometryType(result, geom.getSrid());
    }

}
