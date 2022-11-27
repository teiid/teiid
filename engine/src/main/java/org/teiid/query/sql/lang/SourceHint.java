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

package org.teiid.query.sql.lang;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.StringUtil;

public class SourceHint {

    public static class SpecificHint {
        LinkedHashSet<String> hints = new LinkedHashSet<String>();
        boolean useAliases;

        public SpecificHint(String hint, boolean useAliases) {
            this.hints.add(hint);
            this.useAliases = useAliases;
        }

        public String getHint() {
            return StringUtil.join(hints, " ");
        }
        public boolean isUseAliases() {
            return useAliases;
        }

        public Collection<String> getHints() {
            return hints;
        }
    }

    private boolean useAliases;
    private LinkedHashSet<String> generalHint;
    private Map<String, SpecificHint> sourceHints;

    public String getGeneralHint() {
        if (generalHint == null) {
            return null;
        }
        return StringUtil.join(generalHint, " ");
    }

    public void setGeneralHint(String generalHint) {
        if (this.generalHint == null) {
            this.generalHint = new LinkedHashSet<String>();
        }
        this.generalHint.add(generalHint);
    }

    public void setSourceHint(String sourceName, String hint, boolean useAliases) {
        if (this.sourceHints == null) {
            this.sourceHints = new TreeMap<String, SpecificHint>(String.CASE_INSENSITIVE_ORDER);
        }
        SpecificHint sh = this.sourceHints.get(sourceName);
        if (sh == null) {
            this.sourceHints.put(sourceName, new SpecificHint(hint, useAliases));
        } else {
            sh.useAliases |= useAliases;
            sh.hints.add(hint);
        }
    }

    public SpecificHint getSpecificHint(String sourceName) {
        if (this.sourceHints == null) {
            return null;
        }
        return this.sourceHints.get(sourceName);
    }

    public String getSourceHint(String sourceName) {
        SpecificHint sp = getSpecificHint(sourceName);
        if (sp != null) {
            return sp.getHint();
        }
        return null;
    }

    public Map<String, SpecificHint> getSpecificHints() {
        return sourceHints;
    }

    public boolean isUseAliases() {
        return useAliases;
    }

    public void setUseAliases(boolean useAliases) {
        this.useAliases |= useAliases;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SourceHint)) {
            return false;
        }
        SourceHint other = (SourceHint)obj;
        return EquivalenceUtil.areEqual(generalHint, other.generalHint)
        && EquivalenceUtil.areEqual(this.sourceHints, other.sourceHints);
    }

    public static SourceHint combine(SourceHint sourceHint,
            SourceHint sourceHint2) {
        if (sourceHint == null) {
            if (sourceHint2 == null) {
                return null;
            }
            return sourceHint2;
        } else if (sourceHint2 == null) {
            return sourceHint;
        }
        SourceHint newHint = new SourceHint();
        addHints(sourceHint, newHint);
        addHints(sourceHint2, newHint);
        return newHint;
    }

    private static void addHints(SourceHint sourceHint,
            SourceHint newHint) {
        if (sourceHint.sourceHints != null) {
            for (Map.Entry<String, SpecificHint> entry : sourceHint.sourceHints.entrySet()) {
                for (String hint : entry.getValue().hints) {
                    newHint.setSourceHint(entry.getKey(), hint, entry.getValue().useAliases);
                }
            }
        }
        newHint.setUseAliases(sourceHint.isUseAliases());
        if (sourceHint.generalHint != null) {
            if (newHint.generalHint == null) {
                newHint.generalHint = new LinkedHashSet<String>();
            }
            newHint.generalHint.addAll(sourceHint.generalHint);
        }
    }

    public Collection<String> getGeneralHints() {
        return generalHint;
    }

}
