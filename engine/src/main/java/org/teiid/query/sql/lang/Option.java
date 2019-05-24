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

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents MetaMatrix extension options to normal SQL.  Options
 * are declared in a list after the OPTION keyword, such as:
 * "OPTION SHOWPLAN DEBUG".
 */
public class Option implements LanguageObject {

    public final static String MAKEDEP = Reserved.MAKEDEP;
    public final static String MAKENOTDEP = Reserved.MAKENOTDEP;
    public final static String OPTIONAL = "optional"; //$NON-NLS-1$

    public static class MakeDep {
        private Integer max;
        private Boolean join;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((max == null) ? 0 : max);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof MakeDep)) {
                return false;
            }
            MakeDep other = (MakeDep) obj;
            return EquivalenceUtil.areEqual(max, other.max)
                    && join == other.join;
        }

        public MakeDep() {

        }

        @Override
        public String toString() {
            return new SQLStringVisitor().appendMakeDepOptions(this).getSQLString();
        }

        public Integer getMax() {
            return max;
        }

        public void setMax(Integer max) {
            this.max = max;
        }

        public Boolean getJoin() {
            return join;
        }

        public void setJoin(Boolean join) {
            this.join = join;
        }

        public boolean isSimple() {
            return max == null && join == null;
        }
    }

    private List<String> makeDependentGroups;
    private List<String> makeIndependentGroups;
    private List<MakeDep> makeDependentOptions;
    private List<MakeDep> makeIndependentOptions;
    private List<String> makeNotDependentGroups;
    private List<String> noCacheGroups;
    private boolean noCache;

    /**
     * Construct a default instance of the Option clause.
     */
    public Option() {
    }

    /**
     * Add group to make dependent
     * @param group Group to make dependent
     */
    public void addDependentGroup(String group) {
        addDependentGroup(group, new MakeDep());
    }

    public void addDependentGroup(String group, MakeDep makedep) {
        if (makedep == null) {
            return;
        }
        if(this.makeDependentGroups == null) {
            this.makeDependentGroups = new ArrayList<String>();
            this.makeDependentOptions = new ArrayList<MakeDep>();
        }
        this.makeDependentGroups.add(group);
        this.makeDependentOptions.add(makedep);
    }

    public void addIndependentGroup(String group, MakeDep makedep) {
        if (makedep == null) {
            return;
        }
        if(this.makeIndependentGroups == null) {
            this.makeIndependentGroups = new ArrayList<String>();
            this.makeIndependentOptions = new ArrayList<MakeDep>();
        }
        this.makeIndependentGroups.add(group);
        this.makeIndependentOptions.add(makedep);
    }

    /**
     * Get all groups to make dependent
     * @return List of String defining groups to be made dependent, may be null if no groups
     */
    public List<String> getDependentGroups() {
        return this.makeDependentGroups;
    }

    public List<MakeDep> getMakeDepOptions() {
        return this.makeDependentOptions;
    }

    public List<MakeDep> getMakeIndependentOptions() {
        return makeIndependentOptions;
    }

    public List<String> getMakeIndependentGroups() {
        return makeIndependentGroups;
    }

    /**
     * Add group to make dependent
     * @param group Group to make dependent
     */
    public void addNotDependentGroup(String group) {
        if(this.makeNotDependentGroups == null) {
            this.makeNotDependentGroups = new ArrayList<String>();
        }
        this.makeNotDependentGroups.add(group);
    }

    /**
     * Get all groups to make dependent
     * @return List of String defining groups to be made dependent, may be null if no groups
     */
    public List<String> getNotDependentGroups() {
        return this.makeNotDependentGroups;
    }

    /**
     * Add group that overrides the default behavior of Materialized View feautre
     * to route the query to the primary virtual group transformation instead of
     * the Materialized View transformation.
     * @param group Group that overrides the default behavior of Materialized View
     */
    public void addNoCacheGroup(String group) {
        if(this.noCacheGroups == null) {
            this.noCacheGroups = new ArrayList<String>();
        }
        this.noCacheGroups.add(group);
    }

    /**
     * Get all groups that override the default behavior of Materialized View feautre
     * to route the query to the primary virtual group transformation instead of
     * the Materialized View transformation.
     * @return List of String defining groups that overrides the default behavior of
     * Materialized View, may be null if there are no groups
     */
    public List<String> getNoCacheGroups() {
        return this.noCacheGroups;
    }

    public boolean isNoCache() {
        return noCache;
    }

    public void setNoCache(boolean noCache) {
        this.noCache = noCache;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    /**
     * Compare two Option clauses for equality.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(!(obj instanceof Option)) {
            return false;
        }

        Option other = (Option) obj;

        return noCache == other.noCache &&
               EquivalenceUtil.areEqual(makeDependentGroups, other.makeDependentGroups) &&
               EquivalenceUtil.areEqual(makeIndependentGroups, other.makeIndependentGroups) &&
               EquivalenceUtil.areEqual(makeDependentOptions, other.makeDependentOptions) &&
               EquivalenceUtil.areEqual(makeIndependentOptions, other.makeIndependentOptions) &&
               EquivalenceUtil.areEqual(getNotDependentGroups(), other.getNotDependentGroups()) &&
               EquivalenceUtil.areEqual(getNoCacheGroups(), other.getNoCacheGroups());
    }

    /**
     * Get hash code for Option.
     * @return Hash code
     */
    public int hashCode() {
        int hc = 0;
        if(this.makeDependentGroups != null) {
            hc = HashCodeUtil.hashCode(hc, this.makeDependentGroups);
        }
        if(getNotDependentGroups() != null) {
            hc = HashCodeUtil.hashCode(hc, getNotDependentGroups());
        }
        if(getNoCacheGroups() != null) {
            hc = HashCodeUtil.hashCode(hc, getNoCacheGroups());
        }
        return hc;
    }

    /**
     * Return deep copy of this option object
     * @return Deep copy of the object
     */
    public Object clone() {
        Option newOption = new Option();
        newOption.setNoCache(noCache);

        if(this.makeDependentGroups != null) {
            newOption.makeDependentGroups = new ArrayList<String>(this.makeDependentGroups);
            newOption.makeDependentOptions = new ArrayList<MakeDep>(this.makeDependentOptions);
        }

        if(this.makeIndependentGroups != null) {
            newOption.makeIndependentGroups = new ArrayList<String>(this.makeIndependentGroups);
            newOption.makeIndependentOptions = new ArrayList<MakeDep>(this.makeIndependentOptions);
        }

        if(getNotDependentGroups() != null) {
            newOption.makeNotDependentGroups = new ArrayList<String>(getNotDependentGroups());
        }

        if(getNoCacheGroups() != null) {
            newOption.noCacheGroups = new ArrayList<String>(getNoCacheGroups());
        }

        return newOption;
    }
}
