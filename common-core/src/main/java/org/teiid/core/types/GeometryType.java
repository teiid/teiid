package org.teiid.core.types;

import java.sql.Blob;

public class GeometryType extends BlobType {
    public GeometryType() {

    }

    public GeometryType(Blob blob) {
        super(blob);
    }

    public GeometryType(byte[] bytes) {
        super(bytes);
    }
}
