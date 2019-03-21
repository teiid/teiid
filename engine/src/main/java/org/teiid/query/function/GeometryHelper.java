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

import org.locationtech.jts.geom.Geometry;
import org.teiid.CommandContext;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidException;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;
import org.teiid.core.util.ReflectionHelper;

public class GeometryHelper {
    
    private static GeometryHelper INSTANCE;
    
    public static GeometryHelper getInstance() {
        if (INSTANCE == null) {
            try {
                INSTANCE = (GeometryHelper) ReflectionHelper.create("org.teiid.geo.GeometryHelperImpl", null, GeometryHelper.class.getClassLoader());//$NON-NLS-1$
            } catch (TeiidException e) {
                INSTANCE = new GeometryHelper();
            }
        }
        return INSTANCE;
    }
    
    /**
     * 
     * @param ctx
     * @param jtsGeomSrc
     * @param srid
     * @return
     * @throws FunctionExecutionException
     */
    public Geometry transform(CommandContext ctx, Geometry jtsGeomSrc,
            int srid) throws FunctionExecutionException {
        throw new FunctionExecutionException("Without the optional geospatial library, cannot transform the value to the expected SRID"); //$NON-NLS-1$
    }
    
    /**
     * 
     * @param ctx
     * @param srid
     * @return
     * @throws FunctionExecutionException
     */
    public boolean isLatLong(CommandContext ctx, int srid)
            throws FunctionExecutionException {
        //may create invalid values
        return true;
    }
    
    /**
     * 
     * @param object
     * @return
     * @throws FunctionExecutionException 
     */
    public ClobType geometryToGeoJson(GeometryType object) throws FunctionExecutionException {
        throw new FunctionExecutionException("Without the optional geospatial library, cannot convert to geojson"); //$NON-NLS-1$
    }
    
}
