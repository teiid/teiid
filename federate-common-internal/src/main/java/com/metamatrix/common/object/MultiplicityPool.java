/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.common.object;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.HashCodeUtil;

public class MultiplicityPool {
    private static Map INSTANCES = new HashMap();

    public static final Multiplicity UNBOUNDED      = Multiplicity.UNBOUNDED;
    public static final Multiplicity ZERO_OR_ONE    = Multiplicity.getInstance(0,1);
    public static final Multiplicity ONLY_ONE       = Multiplicity.getInstance(1);
    public static final Multiplicity ONE_OR_MORE    = Multiplicity.getInstance(1,Multiplicity.UNBOUNDED_VALUE);

    static {
        // Preload the pool with very common instances ...
        try {
            INSTANCES.put(new MultiplicityHolder(UNBOUNDED.toString(),  UNBOUNDED.isOrdered(),  UNBOUNDED.isUnique()),  UNBOUNDED);
            INSTANCES.put(new MultiplicityHolder(ZERO_OR_ONE.toString(),ZERO_OR_ONE.isOrdered(),ZERO_OR_ONE.isUnique()),ZERO_OR_ONE);
            INSTANCES.put(new MultiplicityHolder(ONLY_ONE.toString(),   ONLY_ONE.isOrdered(),   ONLY_ONE.isUnique()),   ONLY_ONE);
            INSTANCES.put(new MultiplicityHolder(ONE_OR_MORE.toString(),ONE_OR_MORE.isOrdered(),ONE_OR_MORE.isUnique()),ONE_OR_MORE);
//            MultiplicityPool.get("*");
            MultiplicityPool.get("0..*"); //$NON-NLS-1$
//            MultiplicityPool.get("0..1");
//            MultiplicityPool.get("1");
        } catch ( MultiplicityExpressionException e ) {
        }
    }

    public static Multiplicity get( String multiplicityValue, boolean isOrdered, boolean isUnique ) throws MultiplicityExpressionException {
        if ( multiplicityValue == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0013));
        }
        MultiplicityHolder key = new MultiplicityHolder(multiplicityValue,isOrdered,isUnique);
        Multiplicity result = (Multiplicity) INSTANCES.get(key);
        if ( result == null ) {
            result = Multiplicity.getInstance(multiplicityValue,isOrdered,isUnique);
            INSTANCES.put(key,result);
//            System.out.println("Adding Multiplicity: \"" + result + "\"");
//            LogManager.logInfo("MultiplicityPool","Adding Multiplicity: \"" + result + "\"");
//        } else {
//            System.out.println("Found Multiplicity: \"" + result + "\"");
//            LogManager.logInfo("MultiplicityPool","Found Multiplicity : \"" + result + "\"");
        }
        return result;
    }
    public static Multiplicity get( String multiplicityValue )  throws MultiplicityExpressionException {
        return get(multiplicityValue,Multiplicity.DEFAULT_ORDERING,Multiplicity.DEFAULT_UNIQUENESS);
    }

}

class MultiplicityHolder {
    String multiplicity = null;
    boolean isOrdered = true;
    boolean isUnique = true;
    public MultiplicityHolder(String m, boolean isOrdered, boolean isUnique ) {
        this.multiplicity = m;
        this.isOrdered = isOrdered;
        this.isUnique = isUnique;
    }
    public int hashCode() {
        int seed = 0;
        seed = HashCodeUtil.hashCode(seed,multiplicity);
        seed = HashCodeUtil.hashCode(seed,isOrdered);
        seed = HashCodeUtil.hashCode(seed,isUnique);
        return seed;
    }
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (this.getClass().isInstance(obj)) {
            MultiplicityHolder that = (MultiplicityHolder)obj;
            if ( that.isOrdered != this.isOrdered ) {
                return false;
            }
            if ( that.isUnique != this.isUnique ) {
                return false;
            }
            if ( ! this.multiplicity.equals(that.multiplicity) ) {
                return false;
            }
            return true;
        }

        // Otherwise not comparable ...
        return false;
    }
}

