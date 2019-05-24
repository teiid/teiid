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

package org.teiid.metadata;

import java.io.Serializable;

public class ColumnStats implements Serializable {

    private static final long serialVersionUID = 7827734836519486538L;

    private Number distinctValues;
    private Number nullValues;
    private String minimumValue;
    private String maximumValue;

    public String getMinimumValue() {
        return minimumValue;
    }

    public void setMinimumValue(String min) {
        this.minimumValue = min;
    }

    public String getMaximumValue() {
        return maximumValue;
    }

    public void setMaximumValue(String max) {
        this.maximumValue = max;
    }

    public Number getDistinctValues() {
        return distinctValues;
    }

    public void setDistinctValues(Number numDistinctValues) {
        this.distinctValues = numDistinctValues;
    }

    public Number getNullValues() {
        return nullValues;
    }

    public void setNullValues(Number numNullValues) {
        this.nullValues = numNullValues;
    }

}
