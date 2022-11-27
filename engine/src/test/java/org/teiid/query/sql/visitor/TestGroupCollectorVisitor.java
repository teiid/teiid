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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;

import junit.framework.TestCase;


/**
 */
public class TestGroupCollectorVisitor extends TestCase {

    public TestGroupCollectorVisitor(String name) {
        super(name);
    }

    public GroupSymbol exampleGroupSymbol(int number) {
        return new GroupSymbol("group." + number); //$NON-NLS-1$
    }

    public void helpTestGroups(LanguageObject obj, boolean removeDuplicates, Collection expectedGroups) {
        Collection actualGroups = GroupCollectorVisitor.getGroups(obj, removeDuplicates);
        assertEquals("Actual groups didn't meet expected groups: ", expectedGroups, actualGroups); //$NON-NLS-1$
    }

    public void testGroupSymbol() {
        GroupSymbol gs = exampleGroupSymbol(1);
        Set groups = new HashSet();
        groups.add(gs);
        helpTestGroups(gs, true, groups);
    }

    public void testUnaryFromClause() {
        GroupSymbol gs = exampleGroupSymbol(1);
        UnaryFromClause ufc = new UnaryFromClause(gs);
        Set groups = new HashSet();
        groups.add(gs);
        helpTestGroups(ufc, true, groups);
    }

    public void testJoinPredicate1() {
        GroupSymbol gs1 = exampleGroupSymbol(1);
        GroupSymbol gs2 = exampleGroupSymbol(2);
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(gs1), new UnaryFromClause(gs2), JoinType.JOIN_CROSS);

        Set groups = new HashSet();
        groups.add(gs1);
        groups.add(gs2);
        helpTestGroups(jp, true, groups);
    }

    public void testJoinPredicate2() {
        GroupSymbol gs1 = exampleGroupSymbol(1);
        GroupSymbol gs2 = exampleGroupSymbol(2);
        GroupSymbol gs3 = exampleGroupSymbol(3);
        JoinPredicate jp1 = new JoinPredicate(new UnaryFromClause(gs1), new UnaryFromClause(gs2), JoinType.JOIN_CROSS);
        JoinPredicate jp2 = new JoinPredicate(new UnaryFromClause(gs3), jp1, JoinType.JOIN_CROSS);

        Set groups = new HashSet();
        groups.add(gs1);
        groups.add(gs2);
        groups.add(gs3);
        helpTestGroups(jp2, true, groups);
    }

    public void testFrom1() {
        GroupSymbol gs1 = exampleGroupSymbol(1);
        GroupSymbol gs2 = exampleGroupSymbol(2);
        GroupSymbol gs3 = exampleGroupSymbol(3);
        From from = new From();
        from.addGroup(gs1);
        from.addGroup(gs2);
        from.addGroup(gs3);

        Set groups = new HashSet();
        groups.add(gs1);
        groups.add(gs2);
        groups.add(gs3);
        helpTestGroups(from, true, groups);
    }

    public void testFrom2() {
        GroupSymbol gs1 = exampleGroupSymbol(1);
        GroupSymbol gs2 = exampleGroupSymbol(2);
        GroupSymbol gs3 = exampleGroupSymbol(3);
        From from = new From();
        from.addGroup(gs1);
        from.addGroup(gs2);
        from.addGroup(gs3);

        List groups = new ArrayList();
        groups.add(gs1);
        groups.add(gs2);
        groups.add(gs3);
        helpTestGroups(from, false, groups);
    }

    public void testFrom3() {
        GroupSymbol gs1 = exampleGroupSymbol(1);
        GroupSymbol gs2 = exampleGroupSymbol(2);
        From from = new From();
        from.addGroup(gs1);
        from.addGroup(gs2);
        from.addGroup(gs2);

        Set groups = new HashSet();
        groups.add(gs1);
        groups.add(gs2);
        helpTestGroups(from, true, groups);
    }

    public void testFrom4() {
        GroupSymbol gs1 = exampleGroupSymbol(1);
        GroupSymbol gs2 = exampleGroupSymbol(2);
        From from = new From();
        from.addGroup(gs1);
        from.addGroup(gs2);
        from.addGroup(gs1);

        List groups = new ArrayList();
        groups.add(gs1);
        groups.add(gs2);
        groups.add(gs1);
        helpTestGroups(from, false, groups);
    }

    public void testInsert() {
        GroupSymbol gs1 = exampleGroupSymbol(1);
         Insert insert = new Insert();
         insert.setGroup(gs1);

         Set groups = new HashSet();
         groups.add(gs1);
         helpTestGroups(insert, true, groups);
    }

    public void testUpdate() {
        GroupSymbol gs1 = exampleGroupSymbol(1);
         Update update = new Update();
         update.setGroup(gs1);

         Set groups = new HashSet();
         groups.add(gs1);
         helpTestGroups(update, true, groups);
    }

    public void testDelete() {
        GroupSymbol gs1 = exampleGroupSymbol(1);
         Delete delete = new Delete();
         delete.setGroup(gs1);

         Set groups = new HashSet();
         groups.add(gs1);
         helpTestGroups(delete, true, groups);
    }

    public void testBatchedUpdateCommand() {
        GroupSymbol g1 = exampleGroupSymbol(1);
        GroupSymbol g2 = exampleGroupSymbol(2);
        GroupSymbol g3 = exampleGroupSymbol(3);
        Insert insert = new Insert();
        insert.setGroup(g1);
        Update update = new Update();
        update.setGroup(g2);
        Delete delete = new Delete();
        delete.setGroup(g3);

        List updates = new ArrayList(3);
        updates.add(insert);
        updates.add(update);
        updates.add(delete);

        Set groups = new HashSet();
        groups.add(g1);
        groups.add(g2);
        groups.add(g3);

        helpTestGroups(new BatchedUpdateCommand(updates), true, groups);
    }
}
