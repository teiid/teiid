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

package com.metamatrix.common.jdbc.metadata;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.core.util.ArgCheck;

public class Table extends ColumnSet {
    
    public static final int NO_CARDINALITY = -1;
    
    private String type;
    private String remarks;
    private final List uniqueKeys;
    private final List foreignKeys;
    private final List indexes;
    private final List privileges;
    private int cardinality;

    public Table() {
        super();
        this.uniqueKeys = new LinkedList();
        this.foreignKeys = new LinkedList();
        this.indexes = new LinkedList();
        this.privileges = new LinkedList();
        this.cardinality = NO_CARDINALITY;
    }

    public Table(String name) {
        super(name);
        this.uniqueKeys = new LinkedList();
        this.foreignKeys = new LinkedList();
        this.indexes = new LinkedList();
        this.privileges = new LinkedList();
        this.cardinality = NO_CARDINALITY;
    }

    public Table(String catalogName, String schemaName, String name) {
        super(catalogName, schemaName, name);
        this.uniqueKeys = new LinkedList();
        this.foreignKeys = new LinkedList();
        this.indexes = new LinkedList();
        this.privileges = new LinkedList();
        this.cardinality = NO_CARDINALITY;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public void addAll(Collection tableEntities) {
        for (Iterator it=tableEntities.iterator(); it.hasNext(); ) {
            Object entity = it.next();
            if (entity instanceof Column) {
                this.add( (Column)entity );
            } else if (entity instanceof ForeignKey) {
                this.add( (ForeignKey)entity );
            } else if (entity instanceof UniqueKey) {
                this.add( (UniqueKey)entity );
            } else if (entity instanceof Index) {
                this.add((Index) entity);
            }
        }
    }

    public void add(Column object) {
        super.addColumn(object, true);
    }

    public boolean remove(Column object) {
        return super.removeColumn(object, true);
    }

    public List getUniqueKeys() {
        return this.uniqueKeys;
    }

    public void add(UniqueKey object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The UniqueKey reference may not be null"); //$NON-NLS-1$
        }
        if (!this.uniqueKeys.contains(object)) {
            Iterator iter = object.getColumns().iterator();
            while (iter.hasNext()) {
                JDBCObject obj = (JDBCObject)iter.next();
                if (obj.hasOwner() && obj.getOwner() != this) {
                    ArgCheck.isTrue(false, "The UniqueKey contains columns that are not owned by this table"); //$NON-NLS-1$
                }
            }
            this.uniqueKeys.add(object);
            object.setOwner(this);
        }
    }

    public boolean remove(UniqueKey object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The UniqueKey reference may not be null"); //$NON-NLS-1$
        }
        
        if(object.getOwner() != this){
            ArgCheck.isTrue(object.getOwner() == this, "The specified object is not contained by this object"); //$NON-NLS-1$
        }
        
        object.setOwner(null);
        return this.uniqueKeys.remove(object);
    }

    public boolean contains(UniqueKey object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The UniqueKey reference may not be null"); //$NON-NLS-1$
        }
        return this.uniqueKeys.contains(object);
    }

    public boolean hasUniqueKeys() {
        return this.uniqueKeys.size() != 0;
    }

    public List getForeignKeys() {
        return this.foreignKeys;
    }

    public void add(ForeignKey object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The ForeignKey reference may not be null"); //$NON-NLS-1$
        }
        
        if (!this.foreignKeys.contains(object)) {
            Iterator iter = object.getColumns().iterator();
            while (iter.hasNext()) {
                JDBCObject obj = (JDBCObject)iter.next();
                if (obj.hasOwner() && obj.getOwner() != this) {
                    ArgCheck.isTrue(false, "The ForeignKey contains columns that are not owned by this table"); //$NON-NLS-1$
                }
            }

            this.foreignKeys.add(object);
            object.setOwner(this);
        }
    }

    public boolean remove(ForeignKey object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The ForeignKey reference may not be null"); //$NON-NLS-1$
        }
        if(object.getOwner() != this){
            ArgCheck.isTrue(object.getOwner() == this, "The specified object is not contained by this object"); //$NON-NLS-1$
        }
        object.setOwner(null);
        return this.foreignKeys.remove(object);
    }

    public boolean contains(ForeignKey object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The ForeignKey reference may not be null"); //$NON-NLS-1$
        }
        return this.foreignKeys.contains(object);
    }

    public boolean hasForeignKeys() {
        return this.foreignKeys.size() != 0;
    }

    public List getIndexes() {
        return this.indexes;
    }

    /**
     * Add a reference from this table to the specified Index.
     * Note that the Table is not considered to 'own' the Index.
     */
    public void add(final Index object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The Index reference may not be null"); //$NON-NLS-1$
        }
        
        if (!this.indexes.contains(object)) {
            this.indexes.add(object);
        }
    }

    public boolean remove(final Index object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The Index reference may not be null"); //$NON-NLS-1$
        }
        
        return this.indexes.remove(object);
    }

    public boolean contains(final Index object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The Index reference may not be null"); //$NON-NLS-1$
        }
        return this.indexes.contains(object);
    }

    public boolean hasIndexes() {
        return this.indexes.size() != 0;
    }
    
    public void setCardinality( final int cardinality ) {
        if ( cardinality < 0 ) {
            this.cardinality = NO_CARDINALITY;    
        } else {
            this.cardinality = cardinality;
        } 
    }
    /**
     * This method obtains the cardinality (i.e., number of records as defined
     * by the SQL standard) of the supplied {@link Table} instances. 
     * @return the cardinality of this table if known or available from its
     * associated indices, or NO_CARDINALITY if the cardinality is unknown.
     */
    public int getCardinality() {
        if ( this.cardinality != NO_CARDINALITY ) {
            return this.cardinality;    
        }
        final List indexes = this.getIndexes();
        if ( indexes.size() != 0 ) {
        
            // If the table has an associated statistics index ...
            int maxCardinality = 0;
            final Iterator iter = indexes.iterator();
            while (iter.hasNext()) {
                final Index index = (Index) iter.next();
                if ( index.getType().equals(IndexType.STATISTIC) ) {
                    return index.getCardinality();
                }
                maxCardinality = Math.max(maxCardinality, index.getCardinality());
            }
            
            // If no statistics index, then return the maximum cardinality found from
            // any of the indexes ...
            return maxCardinality;
        }
        return NO_CARDINALITY;
    }
        
    /**
     * Adds the specified privilege to this table.  The JDBC API specification uses
     * the following as example privileges: "SELECT", "INSERT", "UPDATE", and "REFERENCES".
     */
    public void addPrivilege(final String privilege) {
        if (privilege != null && !this.privileges.contains(privilege)) {
            this.privileges.add(privilege);
        }
    }

    public boolean removePrivilege(final String privilege) {
        if (privilege != null) {
            return this.privileges.remove(privilege);
        }
        return false;
    }
    
    public boolean hasPrivilege( final String privilege ) {
        return hasPrivilege(privilege,true);
    }

    public boolean hasPrivilege( final String privilege, boolean caseSensitive ) {
        if (privilege != null ) {
            if ( caseSensitive ) {
                return this.privileges.contains(privilege);
            }
            final Iterator iter = this.privileges.iterator();
            while (iter.hasNext()) {
                final String priv = (String) iter.next();
                if ( privilege.equalsIgnoreCase(priv) ) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    public Index lookupIndex(String indexName) {
        return (Index) JDBCObject.lookupJDBCObject(this.getIndexes(),indexName,Index.class);
    }

    public UniqueKey lookupUniqueKey(String keyName) {
        return (UniqueKey) JDBCObject.lookupJDBCObject(this.getUniqueKeys(),keyName,UniqueKey.class);
    }

    public UniqueKey lookupUniqueKey(UniqueKey matchingKey) {
        if(matchingKey == null){
            ArgCheck.isNotNull(matchingKey,"The UniqueKey used for lookup may not be null"); //$NON-NLS-1$
        }
        Iterator iter = this.getUniqueKeys().iterator();
        while ( iter.hasNext() ) {
            UniqueKey uk = (UniqueKey) iter.next();
            if ( matchingKey.hasMatchingColumns(uk) ) {
                return uk;
            }
        }
        
        // If not found, lookup by name ...
        if ( matchingKey.getName() != null && matchingKey.getName().length() != 0 ) {
            return lookupUniqueKey(matchingKey.getName());
        }
        return null;
    }

    public ForeignKey lookupForeignKey(String keyName) {
        return (ForeignKey) JDBCObject.lookupJDBCObject(this.getForeignKeys(),keyName,ForeignKey.class);
    }

    public ForeignKey lookupForeignKey(ForeignKey matchingKey) {
        if(matchingKey == null){
            ArgCheck.isNotNull(matchingKey,"The ForeignKey used for lookup may not be null"); //$NON-NLS-1$
        }
        Iterator iter = this.getForeignKeys().iterator();
        while ( iter.hasNext() ) {
            ForeignKey fk = (ForeignKey) iter.next();
            if ( matchingKey.hasMatchingColumns(fk) ) {
                return fk;
            }
        }
        
        // If not found, lookup by name ...
        if ( matchingKey.getName() != null && matchingKey.getName().length() != 0 ) {
            return lookupForeignKey(matchingKey.getName());
        }
        return null;
    }

    public void print(PrintStream stream) {
        print(stream, "  "); //$NON-NLS-1$
    }

    public void print(PrintStream stream, String lead) {
        super.print(stream,lead);
        JDBCObject child = null;
        Iterator iter = this.getColumns().iterator();
        while (iter.hasNext()) {
            child = (JDBCObject)iter.next();
            child.print(stream, lead + "  "); //$NON-NLS-1$
        }
        iter = this.getUniqueKeys().iterator();
        while (iter.hasNext()) {
            child = (JDBCObject)iter.next();
            child.print(stream, lead + "  "); //$NON-NLS-1$
        }
        iter = this.getForeignKeys().iterator();
        while (iter.hasNext()) {
            child = (JDBCObject)iter.next();
            child.print(stream, lead + "  "); //$NON-NLS-1$
        }
    }


}



