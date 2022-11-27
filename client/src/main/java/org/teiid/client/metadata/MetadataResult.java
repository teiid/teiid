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

package org.teiid.client.metadata;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

import org.teiid.core.util.ExternalizeUtil;


public class MetadataResult implements Externalizable {
    private static final long serialVersionUID = -1520482281079030324L;
    private Map<Integer, Object>[] columnMetadata;
    private Map<Integer, Object>[] parameterMetadata;

    public MetadataResult() {
    }

    public MetadataResult(Map<Integer, Object>[] columnMetadata, Map<Integer, Object>[] parameterMetadata) {
        this.columnMetadata = columnMetadata;
        this.parameterMetadata = parameterMetadata;
    }
    public Map<Integer, Object>[] getColumnMetadata() {
        return columnMetadata;
    }

    public Map[] getParameterMetadata() {
        return parameterMetadata;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        columnMetadata = ExternalizeUtil.readArray(in, Map.class);
        parameterMetadata = ExternalizeUtil.readArray(in, Map.class);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ExternalizeUtil.writeArray(out, columnMetadata);
        ExternalizeUtil.writeArray(out, parameterMetadata);
    }

}
