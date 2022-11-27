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

package org.teiid.core.types;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.sql.Blob;

/**
 * Base type for geography and geometry.
 *
 * Simply a blob reference with an srid.
 */
public abstract class AbstractGeospatialType extends BlobType {

    private int srid;
    private Reference<?> geoCache;

    public AbstractGeospatialType() {

    }

    public AbstractGeospatialType(Blob blob) {
        super(blob);
    }

    public AbstractGeospatialType(byte[] bytes) {
        super(bytes);
    }

    public int getSrid() {
        return srid;
    }

    public void setSrid(int srid) {
        this.srid = srid;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(srid);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        srid = in.readInt();
    }

    /**
     * Get the Object model representation of the geospatial value
     */
    public synchronized Object getGeoCache() {
        if (geoCache == null) {
            return null;
        }
        Object result = geoCache.get();
        if (result != null && geoCache instanceof WeakReference) {
            geoCache = new SoftReference(result);
        }
        return result;
    }

    /**
     * Set the Object model representation of the geospatial value
     */
    public synchronized void setGeoCache(Object objectReference) {
        if (objectReference == null) {
            this.geoCache = null;
        } else {
            this.geoCache = new WeakReference(objectReference);
        }
    }

    public synchronized void copyTo(AbstractGeospatialType geo) {
        geo.setGeoCache(geoCache==null?null:geoCache.get());
        geo.setSrid(srid);
        geo.setReference(this.reference);
    }

}
