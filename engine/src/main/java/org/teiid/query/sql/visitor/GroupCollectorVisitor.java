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

package org.teiid.query.sql.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;

import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Into;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 * <p>This visitor class will traverse a language object tree and collect all group
 * symbol references it finds.  It uses a collection to collect the groups in so
 * different collections will give you different collection properties - for instance,
 * using a Set will remove duplicates.
 *
 * <p>The easiest way to use this visitor is to call the static methods which create
 * the visitor (and possibly the collection), run the visitor, and get the collection.
 * The public visit() methods should NOT be called directly.
 */
public class GroupCollectorVisitor extends LanguageVisitor {

    private Collection<GroupSymbol> groups;

    private boolean isIntoClauseGroup;

    // In some cases, set a flag to ignore groups created by a subquery from clause
    private boolean ignoreInlineViewGroups = false;
    private Collection<GroupSymbol> inlineViewGroups;    // groups defined by a SubqueryFromClause

    /**
     * Construct a new visitor with the specified collection, which should
     * be non-null.
     * @param groups Collection to use for groups
     * @throws IllegalArgumentException If groups is null
     */
    public GroupCollectorVisitor(Collection<GroupSymbol> groups) {
        if(groups == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0023")); //$NON-NLS-1$
        }
        this.groups = groups;
    }

    /**
     * Get the groups collected by the visitor.  This should best be called
     * after the visitor has been run on the language object tree.
     * @return Collection of {@link org.teiid.query.sql.symbol.GroupSymbol}
     */
    public Collection<GroupSymbol> getGroups() {
        return this.groups;
    }

    public Collection<GroupSymbol> getInlineViewGroups() {
        return this.inlineViewGroups;
    }

    public void setIgnoreInlineViewGroups(boolean ignoreInlineViewGroups) {
        this.ignoreInlineViewGroups = ignoreInlineViewGroups;
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(GroupSymbol obj) {
        if(this.isIntoClauseGroup){
            if (!obj.isTempGroupSymbol()) {
                // This is a physical group. Collect it.
                this.groups.add(obj);
            }
            this.isIntoClauseGroup = false;
        }else{
            this.groups.add(obj);
        }
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(StoredProcedure obj) {
        this.groups.add(obj.getGroup());
    }

    public void visit(Into obj) {
        this.isIntoClauseGroup = true;
    }


    public void visit(SubqueryFromClause obj) {
        if(this.ignoreInlineViewGroups) {
            if(this.inlineViewGroups == null) {
                this.inlineViewGroups = new ArrayList<GroupSymbol>();
            }
            this.inlineViewGroups.add(obj.getGroupSymbol());
        }
    }

    /**
     * Helper to quickly get the groups from obj in the groups collection
     * @param obj Language object
     * @param groups Collection to collect groups in
     */
    public static void getGroups(LanguageObject obj, Collection<GroupSymbol> groups) {
        GroupCollectorVisitor visitor = new GroupCollectorVisitor(groups);
        PreOrderNavigator.doVisit(obj, visitor);
    }

    /**
     * Helper to quickly get the groups from obj in a collection.  The
     * removeDuplicates flag affects whether duplicate groups will be
     * filtered out.
     * @param obj Language object
     * @param removeDuplicates True to remove duplicates
     * @return Collection of {@link org.teiid.query.sql.symbol.GroupSymbol}
     */
    public static Collection<GroupSymbol> getGroups(LanguageObject obj, boolean removeDuplicates) {
        Collection<GroupSymbol> groups = null;
        if(removeDuplicates) {
            groups = new LinkedHashSet<GroupSymbol>();
        } else {
            groups = new ArrayList<GroupSymbol>();
        }
        GroupCollectorVisitor visitor = new GroupCollectorVisitor(groups);
        PreOrderNavigator.doVisit(obj, visitor);
        return groups;
    }

    /**
     * Helper to quickly get the groups from obj in the groups collection
     * @param obj Language object
     * @param groups Collection to collect groups in
     */
    public static void getGroupsIgnoreInlineViewsAndEvaluatableSubqueries(LanguageObject obj, Collection<GroupSymbol> groups) {
        GroupCollectorVisitor visitor = new GroupCollectorVisitor(groups);
        visitor.setIgnoreInlineViewGroups(true);
        PreOrPostOrderNavigator nav = new PreOrPostOrderNavigator(visitor, PreOrPostOrderNavigator.PRE_ORDER, true);
        nav.setSkipEvaluatable(true);
        obj.acceptVisitor(nav);

        if(visitor.getInlineViewGroups() != null) {
            groups.removeAll(visitor.getInlineViewGroups());
        }
    }

    /**
     * Helper to quickly get the groups from obj in a collection.  The
     * removeDuplicates flag affects whether duplicate groups will be
     * filtered out.
     * @param obj Language object
     * @param removeDuplicates True to remove duplicates
     * @return Collection of {@link org.teiid.query.sql.symbol.GroupSymbol}
     */
    public static Collection<GroupSymbol> getGroupsIgnoreInlineViews(LanguageObject obj, boolean removeDuplicates) {
        Collection<GroupSymbol> groups = null;
        if(removeDuplicates) {
            groups = new LinkedHashSet<GroupSymbol>();
        } else {
            groups = new ArrayList<GroupSymbol>();
        }
        GroupCollectorVisitor visitor = new GroupCollectorVisitor(groups);
        visitor.setIgnoreInlineViewGroups(true);
        DeepPreOrderNavigator.doVisit(obj, visitor);

        if(visitor.getInlineViewGroups() != null) {
            groups.removeAll(visitor.getInlineViewGroups());
        }

        return groups;
    }


}
