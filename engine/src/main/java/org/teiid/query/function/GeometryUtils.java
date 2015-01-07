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
    	InputStream is = null;
        try {
            WKBReader reader = new WKBReader();
            is = geometry.getBinaryStream();
            Geometry jtsGeometry = reader.read(new InputStreamInStream(is));
            return new ClobType(new ClobImpl(jtsGeometry.toText()));
        } catch (IOException e) {
            throw new FunctionExecutionException(e);
        } catch (ParseException e) {
            throw new FunctionExecutionException(e);
        } catch (SQLException e) {
            throw new FunctionExecutionException(e);
        } finally {
        	if (is != null) {
        		try {
					is.close();
				} catch (IOException e) {
				}
        	}
        }
    }

    public static GeometryType geometryFromClob(ClobType wkt) throws FunctionExecutionException {
    	Reader r = null;
        try {
            WKTReader reader = new WKTReader();
            WKBWriter writer = new WKBWriter();
            r = wkt.getCharacterStream();
            Geometry jtsGeometry = reader.read(r);
            byte[] bytes = writer.write(jtsGeometry);
            return new GeometryType(bytes);
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
    
    //TODO: should allow an option to assume well formed
    public static GeometryType geometryFromBlob(BlobType wkb) throws FunctionExecutionException {
    	InputStream is = null;
        try {
        	//validate
            WKBReader reader = new WKBReader();
            is = wkb.getBinaryStream();
            reader.read(new InputStreamInStream(is));
            
            //return as geometry
            return new GeometryType(wkb.getReference());
        } catch (ParseException e) {
            throw new FunctionExecutionException(e);
        } catch (SQLException e) {
            throw new FunctionExecutionException(e);
        } catch (IOException e) {
        	throw new FunctionExecutionException(e);
		} finally {
        	if (is != null) {
        		try {
					is.close();
				} catch (IOException e) {
				}
        	}
        }
    }

	public static Boolean intersects(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
		InputStream is1 = null;
		InputStream is2 = null;
        try {
            WKBReader reader = new WKBReader();
            is1 = geom1.getBinaryStream();
            is2 = geom2.getBinaryStream();
            Geometry g1 = reader.read(new InputStreamInStream(is1));
            Geometry g2 = reader.read(new InputStreamInStream(is2));
            return g1.intersects(g2);
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
        	if (is2 != null) {
        		try {
					is2.close();
				} catch (IOException e) {
				}
        	}
        }
	}
	
	public static Boolean contains(GeometryType geom1, GeometryType geom2) throws FunctionExecutionException {
		InputStream is1 = null;
		InputStream is2 = null;
        try {
            WKBReader reader = new WKBReader();
            is1 = geom1.getBinaryStream();
            is2 = geom2.getBinaryStream();
            Geometry g1 = reader.read(new InputStreamInStream(is1));
            Geometry g2 = reader.read(new InputStreamInStream(is2));
            return g1.contains(g2);
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
        	if (is2 != null) {
        		try {
					is2.close();
				} catch (IOException e) {
				}
        	}
        }
	}
}
