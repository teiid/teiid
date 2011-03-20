/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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
 
package org.teiid.core.types;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Same as a regular BigDecimal, but changes the implementation of hashCode and equals
 * so that the same numerical value regardless of scale will be equal
 */
public class TeiidBigDecimal extends BigDecimal {

	private static final long serialVersionUID = -5796515987947436480L;

	public TeiidBigDecimal(BigInteger unscaled, int scale) {
		super(unscaled, scale);
	}

	public TeiidBigDecimal(BigDecimal bigDecimal) {
		this(bigDecimal.unscaledValue(), bigDecimal.scale());
	}
	
	public TeiidBigDecimal(String val) {
		super(val);
	}
	
	@Override
	public int hashCode() {
		int xsign = this.signum();
        if (xsign == 0)
            return 0;
        BigDecimal bd = this.stripTrailingZeros();
        return bd.hashCode();
    }
	
	@Override
	public boolean equals(Object x) {
		if (x == this) {
			return true;
		}
		if (!(x instanceof BigDecimal)) {
			return false;
		}
		return this.compareTo((BigDecimal)x) == 0;
	}
	
}
