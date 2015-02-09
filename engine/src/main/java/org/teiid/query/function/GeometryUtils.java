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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * Utility methods for geometry
 * TODO: determine if we should use buffermanager to minimize memory footprint
 * TODO: Split into GeometryFilterUtils and GeometryConvertUtils. - Tom
 */
public class GeometryUtils {
	
    public static ClobType geometryToClob(GeometryType geometry) 
            throws FunctionExecutionException {
        Geometry jtsGeometry = getGeometry(geometry);
        return new ClobType(new ClobImpl(jtsGeometry.toText()));
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
    
    public static ClobType geometryToGml(GeometryType geometry) 
            throws FunctionExecutionException {        
        Geometry jtsGeometry = getGeometry(geometry);
        GMLWriter writer = new GMLWriter();
        String gmlText = writer.write(jtsGeometry);
        return new ClobType(new ClobImpl(gmlText));
    }
        
    public static GeometryType geometryFromGml(ClobType gml) 
            throws FunctionExecutionException {
        return geometryFromGml(gml, GeometryType.UNKNOWN_SRID);
    }
    
    /**
     * Custom SAX handler extending GMLHandler to handle parsing SRIDs.
     */
    private static class GmlSridHandler extends GMLHandler {
        private int srid = GeometryType.UNKNOWN_SRID;
        
        public GmlSridHandler(GeometryFactory gf, ErrorHandler delegate) {
            super(gf, delegate);
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) 
                throws SAXException {
            String srsName = attributes.getValue("srsName");
            if (srsName != null) {
                String[] srsParts = srsName.split(":");
                try {
                    srid = Integer.parseInt(srsParts[1]);
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
    
    public static GeometryType geometryFromGml(ClobType gml, int srid) 
            throws FunctionExecutionException {
        GeometryFactory gf = new GeometryFactory();        
        Geometry jtsGeometry = null;
        Reader reader = null;
        try {            
            reader = gml.getCharacterStream();            
            
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(false);
            factory.setValidating(false);            
            SAXParser parser = factory.newSAXParser();            
            
            GmlSridHandler handler = new GmlSridHandler(gf, null);
            parser.parse(new InputSource(reader), handler);
            
            jtsGeometry = handler.getGeometry();
        
            if (srid == GeometryType.UNKNOWN_SRID) {
                srid = handler.getSrid();
            }

        } catch (IOException e) {
            throw new FunctionExecutionException(e);
        } catch (SAXException e) {
            throw new FunctionExecutionException(e);
        } catch (SQLException e) {
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
        return getGeometry(geom, geom.getSrid());
    }

	public static Geometry getGeometry(GeometryType geom, int srid) 
            throws FunctionExecutionException {
		InputStream is1 = null;
        try {
            WKBReader reader = new WKBReader();
            is1 = geom.getBinaryStream();
            Geometry jtsGeom = reader.read(new InputStreamInStream(is1));
            if (jtsGeom.getSRID() != GeometryType.UNKNOWN_SRID || jtsGeom.getDimension() > 2) {
            	//don't allow ewkb - that needs an explicit function
            	throw new FunctionExecutionException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31160));
            }
            jtsGeom.setSRID(srid);
            return jtsGeom;
        } catch (ParseException e) {
            throw new FunctionExecutionException(e);
        } catch (SQLException e) {
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
}
