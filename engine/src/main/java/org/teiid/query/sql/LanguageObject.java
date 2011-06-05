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

package org.teiid.query.sql;

import java.util.ArrayList;
import java.util.Collection;

/**
 * This is the primary interface for all language objects.  It extends a few 
 * key interfaces and adds some additional methods to allow the {@link LanguageVisitor}
 * to work.
 */
public interface LanguageObject extends Cloneable {

    /**
     * Method for accepting a visitor.  It is the responsibility of the 
     * language object to call back on the visitor.
     * @param visitor Visitor being used
     */
    void acceptVisitor(LanguageVisitor visitor);
		
	/**
	 * Implement clone to make objects cloneable.
	 * @return Deep clone of this object
	 */
    Object clone();
    
    public static class Util {

		public static <S extends LanguageObject, T extends S> ArrayList<S> deepClone(Collection<T> collection, Class<S> type) {
			if (collection == null) {
				return null;
			}
			ArrayList<S> result = new ArrayList<S>(collection.size());
			for (LanguageObject obj : collection) {
				result.add(type.cast(obj.clone()));
			}
			return result;
		}
    	
    }
    	 
}
