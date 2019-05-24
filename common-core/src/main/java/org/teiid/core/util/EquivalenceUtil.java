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

import java.util.Arrays;

/**
 * Utilities to test the equivalence (see method-specific definitions) of any
 * two object/array references.
 */
public class EquivalenceUtil {

    /**
     * Cannot be instantiated
     */
    protected EquivalenceUtil() {
    }

    /**
     * Tests whether two object references refer to equal objects. The object
     * references can both be null, in which case they are also considered equal.
     * @param obj1 object reference
     * @param obj2 object reference
     * @return true if both references are null, OR if neither is null and both
     * objects are equal; false otherwise
     */
    public static boolean areEqual(Object obj1, Object obj2) {
        if (obj1 == obj2) {
            return true;
        } else if (obj1 == null || obj2 == null) {
            return false;
        } else {
            return obj1.equals(obj2);
        }
    }

    /**
     * Tests whether two arrays are equivalent. This method ignores the array
     * types, but checks the number of references in each array, and the
     * equivalence of those references (in ascending index order).
     * @param array1 an object array
     * @param array2 an object array
     */
    public static boolean areEquivalent(Object[] array1, Object[] array2) {
        return Arrays.equals(array1, array2);
    }

    /**
     * Tests whether two objects references are equivalent but not the same object.
     * If both references are the same, this method will return false;
     * @param obj1 object reference
     * @param obj2 object reference
     * @return true if the two references are unequal, and the objects they
     * refer to are equal
     */
    public static boolean areStrictlyEquivalent(Object obj1, Object obj2) {
        if (obj1 == obj2 || obj1 == null || obj2 == null) {
            return false;
        }
        return areEqual(obj1, obj2);
    }

    /**
     * Tests whether the array references are equivalent, but are not the same.
     * This method checks for the strict equivalence of each pair of objects
     * in the two arrays (i.e. corresponding objects cannot be the same but must
     * be equivalent) in ascending index order. This method also considers a
     * null array and a 0-length array equivalent, and ignores the array type.
     * @param array1 an object array
     * @param array2 an object array
     * @return true if the arrays are equivalent, but not the same.
     */
    public static boolean areStrictlyEquivalent(Object[] array1, Object[] array2) {
        if (array1 == array2) {
            return false;
        } else if (array1 == null) {
            return array2.length == 0;
        } else if (array2 == null) {
            return array1.length == 0;
        } else if (array1.length != array2.length) {
             return false;
        } else {
            for (int i = 0; i < array1.length; i++) {
                if (!areStrictlyEquivalent(array1[i], array2[i])) {
                    return false;
                }
            }
        }
        return true;
    }
}
