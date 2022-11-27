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

package org.teiid.olingo.common;

import static org.junit.Assert.*;

import java.sql.Timestamp;

import org.apache.olingo.commons.core.edm.primitivetype.EdmGeographyPoint;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeometryPoint;
import org.junit.Test;
import org.teiid.core.types.DataTypeManager.DefaultDataTypes;
import org.teiid.metadata.BaseColumn;
import org.teiid.metadata.Column;
import org.teiid.query.metadata.SystemMetadata;

@SuppressWarnings("nls")
public class TestODataTypeManager {

    @Test public void testGeometryTypes() {
        Column c = new Column();
        c.setDatatype(SystemMetadata.getInstance().getRuntimeTypeMap().get(DefaultDataTypes.GEOMETRY));
        c.setProperty(BaseColumn.SPATIAL_TYPE, "MULTIPOLYGON"); //$NON-NLS-1$
        assertEquals("GeometryMultiPolygon", ODataTypeManager.odataType(c).name());
        c.setProperty(BaseColumn.SPATIAL_TYPE, "LINESTRING"); //$NON-NLS-1$
        assertEquals("GeometryLineString", ODataTypeManager.odataType(c).name());

        assertEquals("geometry", ODataTypeManager.teiidType(EdmGeometryPoint.getInstance(), false));
    }

    @Test public void testGeographyTypes() {
        Column c = new Column();
        c.setDatatype(SystemMetadata.getInstance().getRuntimeTypeMap().get(DefaultDataTypes.GEOGRAPHY));
        c.setProperty(BaseColumn.SPATIAL_TYPE, "MULTILINESTRING"); //$NON-NLS-1$
        assertEquals("GeographyMultiLineString", ODataTypeManager.odataType(c).name());
        c.setProperty(BaseColumn.SPATIAL_TYPE, "POLYGON"); //$NON-NLS-1$
        assertEquals("GeographyPolygon", ODataTypeManager.odataType(c).name());

        assertEquals("geography", ODataTypeManager.teiidType(EdmGeographyPoint.getInstance(), false));
    }

    @Test public void testTimestampPrecision() {
        Timestamp value = new Timestamp(1234);
        value.setNanos(56789);
        Timestamp corrected = (Timestamp)ODataTypeManager.rationalizePrecision(0, null, value);
        assertEquals(0, corrected.getNanos());

        corrected = (Timestamp)ODataTypeManager.rationalizePrecision(5, null, value);
        assertEquals(50000, corrected.getNanos());

        corrected = (Timestamp)ODataTypeManager.rationalizePrecision(8, null, value);
        assertEquals(56780, corrected.getNanos());
    }


}
