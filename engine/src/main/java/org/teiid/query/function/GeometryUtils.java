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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.InputStreamInStream;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import java.io.IOException;
import java.sql.SQLException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;

public class GeometryUtils {
    public static ClobType geometryToClob(GeometryType geometry) {
        try {
            WKBReader reader = new WKBReader();
            Geometry jtsGeometry = reader.read(new InputStreamInStream(geometry.getBinaryStream()));
            return new ClobType(ClobImpl.createClob(jtsGeometry.toText().toCharArray()));
        } catch (IOException e) {
            throw new TeiidRuntimeException(e);
        } catch (ParseException e) {
            throw new TeiidRuntimeException(e);
        } catch (SQLException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    public static GeometryType geometryFromClob(ClobType wkt) {
        try {
            WKTReader reader = new WKTReader();
            WKBWriter writer = new WKBWriter();
            Geometry jtsGeometry = reader.read(wkt.getCharacterStream());
            byte[] bytes = writer.write(jtsGeometry);
            return new GeometryType(bytes);
        } catch (ParseException e) {
            throw new TeiidRuntimeException(e);
        } catch (SQLException e) {
            throw new TeiidRuntimeException(e);
        }
    }
}
