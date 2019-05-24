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

package org.teiid.geo;

import java.io.IOException;
import java.sql.SQLException;

import org.locationtech.jts.geom.Geometry;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.ClobType.Type;
import org.teiid.core.types.GeometryType;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.function.GeometryUtils;
import org.teiid.query.function.TeiidFunction;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.translator.SourceSystemFunctions;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONReader;
import org.wololo.jts2geojson.GeoJSONWriter;

public class GeometryJsonUtils {

    @TeiidFunction(name = SourceSystemFunctions.ST_ASGEOJSON, category = FunctionCategoryConstants.GEOMETRY, pushdown = PushDown.CAN_PUSHDOWN, nullOnNull = true)
    public static ClobType asGeoJson(GeometryType geometry)
            throws FunctionExecutionException {
        return geometryToGeoJson(geometry);
    }

    @TeiidFunction(name = SourceSystemFunctions.ST_GEOMFROMGEOJSON, category = FunctionCategoryConstants.GEOMETRY, pushdown = PushDown.CAN_PUSHDOWN, nullOnNull = true)
    public static GeometryType geomFromGeoJson(ClobType clob, int srid)
            throws FunctionExecutionException {
        return geometryFromGeoJson(clob, srid);
    }

    @TeiidFunction(name = SourceSystemFunctions.ST_GEOMFROMGEOJSON, category = FunctionCategoryConstants.GEOMETRY, nullOnNull = true)
    public static GeometryType geomFromGeoJson(ClobType clob)
            throws FunctionExecutionException {
        return geometryFromGeoJson(clob, GeometryType.UNKNOWN_SRID);
    }

    public static ClobType geometryToGeoJson(GeometryType geometry)
            throws FunctionExecutionException {
        Geometry jtsGeometry = GeometryUtils.getGeometry(geometry);
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

    public static GeometryType geometryFromGeoJson(ClobType json, int srid)
            throws FunctionExecutionException {
        try {
            GeoJSONReader reader = new GeoJSONReader();
            String jsonText = ClobType.getString(json);
            Geometry jtsGeometry = reader.read(jsonText);
            return GeometryUtils.getGeometryType(jtsGeometry, srid);
        } catch (SQLException e) {
            throw new FunctionExecutionException(e);
        } catch (IOException e) {
            throw new FunctionExecutionException(e);
        }
    }

}
