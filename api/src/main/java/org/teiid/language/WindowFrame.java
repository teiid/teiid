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

package org.teiid.language;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.language.visitor.LanguageObjectVisitor;
import org.teiid.language.visitor.SQLStringVisitor;

/**
 * Defines with window frame for a window function
 */
public class WindowFrame extends BaseLanguageObject {

    public enum BoundMode {
        CURRENT_ROW,
        PRECEDING,
        FOLLOWING
    }

    public enum FrameMode {
        RANGE,
        ROWS
    }

    public static class FrameBound {
        private BoundMode boundMode;
        private Integer bound;

        public FrameBound(BoundMode mode) {
            this.boundMode = mode;
        }

        /**
         * Get the numeric bound.  May be null in CURRENT_ROW or to represent UNBOUNDED
         * @return
         */
        public Integer getBound() {
            return bound;
        }

        public BoundMode getBoundMode() {
            return boundMode;
        }

        public FrameBound bound(Integer i) {
            this.bound = i;
            return this;
        }

        public void setBound(Integer bound) {
            this.bound = bound;
        }

        public void setBoundMode(BoundMode boundMode) {
            this.boundMode = boundMode;
        }

        @Override
        public int hashCode() {
            return HashCodeUtil.hashCode(0, bound, boundMode);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof FrameBound)) {
                return false;
            }
            FrameBound other = (FrameBound)obj;
            return EquivalenceUtil.areEqual(boundMode, other.boundMode)
                    && EquivalenceUtil.areEqual(bound, other.bound);
        }

        @Override
        public FrameBound clone() {
            FrameBound clone = new FrameBound(boundMode);
            clone.bound = bound;
            return clone;
        }
    }

    private FrameMode mode;
    private FrameBound start;
    private FrameBound end;

    public WindowFrame(FrameMode mode) {
        this.mode = mode;
    }

    public FrameBound getStart() {
        return start;
    }

    /**
     * Return the end bound, may be null
     * @return
     */
    public FrameBound getEnd() {
        return end;
    }

    public void setStart(FrameBound lowerBound) {
        this.start = lowerBound;
    }

    public void setEnd(FrameBound upperBound) {
        this.end = upperBound;
    }

    public FrameMode getMode() {
        return mode;
    }

    public void setMode(FrameMode mode) {
        this.mode = mode;
    }

    @Override
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(mode.hashCode(), start, end);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof WindowFrame)) {
            return false;
        }
        WindowFrame other = (WindowFrame)obj;
        return EquivalenceUtil.areEqual(mode, other.mode)
                && EquivalenceUtil.areEqual(start, other.start)
                && EquivalenceUtil.areEqual(end, other.end);
    }

    @Override
    public WindowFrame clone() {
        WindowFrame clone = new WindowFrame(this.mode);
        if (start != null) {
            clone.setStart(start.clone());
        }
        if (end != null) {
            clone.setEnd(end.clone());
        }
        return clone;
    }

}
