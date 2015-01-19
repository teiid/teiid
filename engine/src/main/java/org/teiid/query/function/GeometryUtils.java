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
import org.teiid.core.types.GeometryType;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.InputStreamInStream;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Utility methods for geometry
 * TODO: determine if we should use buffermanager to minimize memory footprint
 */
public class GeometryUtils {
	
    public static ClobType geometryToClob(GeometryType geometry) throws FunctionExecutionException {
        Geometry jtsGeometry = getGeometry(geometry);
        return new ClobType(new ClobImpl(jtsGeometry.toText()));
    }

    public static GeometryType geometryFromClob(ClobType wkt)
            throws FunctionExecutionException {
        return geometryFromClob(wkt, GeometryType.UNKNOWN_SRID);
    }

    public static GeometryType geometryFromClob(ClobType wkt, int srid) throws FunctionExecutionException {
    	Reader r = null;
        try {
            WKTReader reader = new WKTReader();
            WKBWriter writer = new WKBWriter();
            r = wkt.getCharacterStream();
            Geometry jtsGeometry = reader.read(r);
            byte[] bytes = writer.write(jtsGeometry);
            return new GeometryType(bytes, srid);
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

    static Geometry getGeometry(GeometryType geom)
            throws FunctionExecutionException {
        return getGeometry(geom, GeometryType.UNKNOWN_SRID);
    }

	static Geometry getGeometry(GeometryType geom, int srid) throws FunctionExecutionException {
		InputStream is1 = null;
        try {
            WKBReader reader = new WKBReader();
            is1 = geom.getBinaryStream();
            Geometry jtsGeom = reader.read(new InputStreamInStream(is1));
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
}
