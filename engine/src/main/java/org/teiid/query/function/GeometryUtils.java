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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.teiid.CommandContext;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.ClobType.Type;
import org.teiid.core.types.GeometryType;
import org.teiid.query.QueryPlugin;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONReader;
import org.wololo.jts2geojson.GeoJSONWriter;
import org.xml.sax.Attributes;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.InputStreamInStream;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.gml2.GMLHandler;
import com.vividsolutions.jts.io.gml2.GMLWriter;

/**
 * Utility methods for geometry
 * TODO: determine if we should use buffermanager to minimize memory footprint
 */
public class GeometryUtils {
	
    private static final int SRID_4326 = 4326;

	public static ClobType geometryToClob(GeometryType geometry, 
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
        return geometryFromClob(wkt, GeometryType.UNKNOWN_SRID);
    }

    public static GeometryType geometryFromClob(ClobType wkt, int srid) 
            throws FunctionExecutionException {
    	Reader r = null;
        try {
            WKTReader reader = new WKTReader();
            r = wkt.getCharacterStream();
            Geometry jtsGeometry = reader.read(r);
            return getGeometryType(jtsGeometry, srid);
        } catch (ParseException e) {
            throw new FunctionExecutionException(e);
        } catch (SQLException e) {
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
    
    public static ClobType geometryToGeoJson(GeometryType geometry) 
            throws FunctionExecutionException {        
        Geometry jtsGeometry = getGeometry(geometry);
        GeoJSONWriter writer = new GeoJSONWriter();
        try {
            GeoJSON geoJson = writer.write(jtsGeometry);
            ClobType result = new ClobType(new ClobImpl(geoJson.toString()));
            result.setType(Type.JSON);
            return result;
        } catch (Exception e) {
            throw new FunctionExecutionException(e);
        }
    }
    
    public static GeometryType geometryFromGeoJson(ClobType json) 
            throws FunctionExecutionException {
        return geometryFromGeoJson(json, GeometryType.UNKNOWN_SRID);
    }
    
    public static GeometryType geometryFromGeoJson(ClobType json, int srid) 
            throws FunctionExecutionException {
        try {
            GeoJSONReader reader = new GeoJSONReader();
            String jsonText = ClobType.getString(json);
            Geometry jtsGeometry = reader.read(jsonText);
            return getGeometryType(jtsGeometry, srid);
        } catch (SQLException e) {
            throw new FunctionExecutionException(e);            
        } catch (IOException e) {
            throw new FunctionExecutionException(e);            
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
        		jtsGeometry = GeometryTransformUtils.transform(ctx, jtsGeometry, SRID_4326);
        	}
            writer.setPrefix(null);
        } else if (geometry.getSrid() != GeometryType.UNKNOWN_SRID) {
        	//TODO: should include the srsName
        	//writer.setSrsName(String.valueOf(geometry.getSrid()));
        }
        String gmlText = writer.write(jtsGeometry);
        return new ClobType(new ClobImpl(gmlText));
    }
        
    public static GeometryType geometryFromGml(ClobType gml, Integer srid) 
            throws FunctionExecutionException {
        try {
			return geometryFromGml(gml.getCharacterStream(), srid);
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
    
    public static GeometryType geometryFromGml(Reader reader, Integer srid) 
            throws FunctionExecutionException {
        GeometryFactory gf = new GeometryFactory();        
        Geometry jtsGeometry = null;
        try {            
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(false);
            factory.setValidating(false);            
            SAXParser parser = factory.newSAXParser();            
            
            GmlSridHandler handler = new GmlSridHandler(gf, null);
            parser.parse(new InputSource(reader), handler);
            
            jtsGeometry = handler.getGeometry();
        
            if (srid == null) { 
        		if (jtsGeometry.getSRID() == GeometryType.UNKNOWN_SRID) {
        			srid = handler.getSrid();
        		} else {
        			srid = jtsGeometry.getSRID();
        		}
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
        return getGeometryType(jtsGeometry, srid);
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
        WKBWriter writer = new WKBWriter();
        byte[] bytes = writer.write(jtsGeom);
        return new GeometryType(bytes, srid);        
    }
    
    public static Geometry getGeometry(GeometryType geom)
            throws FunctionExecutionException {
        try {
			return getGeometry(geom.getBinaryStream(), geom.getSrid(), false);
		} catch (SQLException e) {
			throw new FunctionExecutionException(e);
		}
    }

	public static Geometry getGeometry(InputStream is1, Integer srid, boolean allowEwkb) 
            throws FunctionExecutionException {
        try {
            WKBReader reader = new WKBReader();
            Geometry jtsGeom = reader.read(new InputStreamInStream(is1));
            if (!allowEwkb && (jtsGeom.getSRID() != GeometryType.UNKNOWN_SRID || jtsGeom.getDimension() > 2)) {
            	//don't allow ewkb - that needs an explicit function
            	throw new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31160));
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
}
