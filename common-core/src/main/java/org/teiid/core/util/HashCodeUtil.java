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

package org.teiid.core.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;

/**
 * <P>This class provides utility functions for generating good
 * hash codes.  Hash codes generated with these methods should
 * have a reasonably good distribution when placed in a hash
 * structure such as Hashtable, HashSet, or HashMap.
 *
 * <P>General usage is something like:
 * <PRE>
 * public int hashCode() {
 *     int hc = 0;    // or = super.hashCode();
 *     hc = HashCodeUtil.hashCode(hc, intField);
 *     hc = HashCodeUtil.hashCode(hc, objectField);
 *     // etc, etc
 *     return hc;
 * }
 * </PRE>
 */
public final class HashCodeUtil {

    // Prime number used in improving distribution: 1,000,003
    private static final int PRIME = 1000003;

    public static final int hashCode(int previous, boolean x) {
        return (PRIME*previous) + (x ? 1 : 0);
    }

    public static final int hashCode(int previous, int x) {
        return (PRIME*previous) + x;
    }

    public static final int hashCode(int previous, long x) {
        // convert to two ints
        return (PRIME*previous) +
               (int) (PRIME*(x >>> 32) + (x & 0xFFFFFFFF));
    }

    public static final int hashCode(int previous, float x) {
        return hashCode(previous, (x == 0.0F) ? 0 : Float.floatToIntBits(x));
    }

    public static final int hashCode(int previous, double x) {
        // convert to long
        return hashCode(previous, (x == 0.0) ? 0L : Double.doubleToLongBits(x));
    }

    public static final int hashCode(int previous, Object... x) {
        if(x == null) {
            return PRIME*previous;
        }
        int hc = previous;
        for(int i=0; i<x.length; i++) {
            hc = (x[i] == null) ? (PRIME*hc) : (PRIME*hc) + x[i].hashCode();
        }
        return hc;
    }

    /**
     * Compute a hash code on a large array by walking the list
     * and combining the hash code at every exponential index:
     * 1, 2, 4, 8, ...  This has been shown to give a good hash
     * for good time complexity.
     */
    public static final int expHashCode(int previous, Object[] x) {
        if(x == null) {
            return PRIME*previous;
        }
        int hc = (PRIME*previous) + x.length;
        int index = 1;
        int xlen = x.length+1;    // switch to 1-based
        while(index < xlen) {
            hc = hashCode(hc, x[index-1]);
            index = index << 1;        // left shift by 1 to double
        }
        return hc;
    }

    /**
     * Compute a hash code on a large collection by walking the list
     * and combining the hash code at every exponential index:
     * 1, 2, 4, 8, ...  This has been shown to give a good hash
     * for good time complexity.
     */
    public static final int expHashCode(int previous, Collection<?> x) {
        if(x == null || x.size() == 0) {
            return PRIME*previous;
        }
        int size = x.size();                // size of collection
        int hc = (PRIME*previous) + size;    // hash code so far
        if (x instanceof RandomAccess && x instanceof List<?>) {
            List<?> l = List.class.cast(x);
            int index = 1;
            int xlen = x.size()+1;    // switch to 1-based
            while(index < xlen) {
                hc = hashCode(hc, l.get(index-1));
                index = index << 1;        // left shift by 1 to double
            }
        } else {
            int skip = 0;                        // skip between samples
            int total = 0;                        // collection examined already
            Iterator<?> iter = x.iterator();        // collection iterator
            Object obj = iter.next();            // last iterated object, primed at first
            while(total < size) {
                for(int i=0; i<skip; i++) {        // skip to next sample
                    obj = iter.next();
                }
                hc = hashCode(hc, obj);            // add sample to hashcode
                skip = (skip == 0) ? 1 : skip << 1;        // left shift by 1 to double
                total += skip;                    // update total
            }
        }
        return hc;
    }

    public static final int expHashCode(CharSequence x) {
        return expHashCode(x, true);
    }

    public static final int expHashCode(CharSequence x, boolean caseSensitive) {
        if(x == null) {
            return 0;
        }
        int hc = x.length();
        int index = 1;
        int xlen = x.length()+1;    // switch to 1-based
        while(index < xlen) {
            int charHash = x.charAt(index-1);
            if (!caseSensitive) {
                charHash = Character.toUpperCase(charHash);
            }
            hc = PRIME * hc + charHash;
            index = index << 1;        // left shift by 1 to double
        }
        return hc;
    }

}
