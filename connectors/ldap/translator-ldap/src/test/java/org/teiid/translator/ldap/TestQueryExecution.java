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

package org.teiid.translator.ldap;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.naming.ldap.LdapContext;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("nls")
public class TestQueryExecution {

    private static final class SimpleNamingEnumeration<T> implements
            NamingEnumeration<T> {
        private final Iterator<T> iter;

        private SimpleNamingEnumeration(Iterator<T> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasMoreElements() {
            return iter.hasNext();
        }

        @Override
        public T nextElement() {
            return iter.next();
        }

        @Override
        public void close() throws NamingException {

        }

        @Override
        public boolean hasMore() throws NamingException {
            return hasMoreElements();
        }

        @Override
        public T next() throws NamingException {
            return nextElement();
        }
    }

    @Test public void testUnwrap() throws Exception {
        TranslationUtility util = new TranslationUtility(RealMetadataFactory.fromDDL("CREATE FOREIGN TABLE GROUP_PEOPLE (objectClass string options (\"teiid_ldap:unwrap\" true)) OPTIONS(nameinsource 'ou=Infrastructure,ou=Support,o=DEMOCORP,c=AU', updatable true);", "x", "y"));
        Command command = util.parseCommand("select * from group_people");
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        LdapContext connection = Mockito.mock(LdapContext.class);
        LdapContext ctx = Mockito.mock(LdapContext.class);
        Mockito.when(connection.lookup("ou=Infrastructure,ou=Support,o=DEMOCORP,c=AU")).thenReturn(ctx);
        Attributes attribs = Mockito.mock(Attributes.class);
        Attribute attrib = Mockito.mock(Attribute.class);
        Mockito.when(attrib.size()).thenReturn(2);

        NamingEnumeration attribValues = new SimpleNamingEnumeration(Arrays.asList("foo", "bar").iterator());

        Mockito.when(attrib.getAll()).thenReturn(attribValues);

        Mockito.when(attribs.get("objectClass")).thenReturn(attrib);

        final SearchResult sr = new SearchResult("x", null, attribs);

        NamingEnumeration<SearchResult> enumeration = new SimpleNamingEnumeration(Arrays.asList(sr).iterator());

        Mockito.when(ctx.search((String)Mockito.any(), (String)Mockito.any(), (SearchControls)Mockito.any())).thenReturn(enumeration);

        LDAPExecutionFactory lef = new LDAPExecutionFactory();
        lef.start();

        LDAPSyncQueryExecution execution = (LDAPSyncQueryExecution)lef.createExecution(command, ec, rm, connection);
        execution.execute();
        List<?> result = execution.next();
        assertEquals(Arrays.asList("foo"), result);
        result = execution.next();
        assertEquals(Arrays.asList("bar"), result);
        assertNull(execution.next());

        //missing attribute handling
        Mockito.when(attribs.get("objectClass")).thenReturn(null);
        enumeration = new SimpleNamingEnumeration(Arrays.asList(sr).iterator());
        Mockito.when(ctx.search((String)Mockito.any(), (String)Mockito.any(), (SearchControls)Mockito.any())).thenReturn(enumeration);

        execution = (LDAPSyncQueryExecution)lef.createExecution(command, ec, rm, connection);
        execution.execute();
        result = execution.next();
        assertEquals(Collections.singletonList(null), result);
        assertNull(execution.next());

        //empty attribute handling
        attribValues = new SimpleNamingEnumeration(new ArrayList<Object>().iterator());
        Mockito.when(attrib.size()).thenReturn(0);
        Mockito.when(attrib.getAll()).thenReturn(attribValues);
        Mockito.when(attribs.get("objectClass")).thenReturn(attrib);
        enumeration = new SimpleNamingEnumeration(Arrays.asList(sr).iterator());
        Mockito.when(ctx.search((String)Mockito.any(), (String)Mockito.any(), (SearchControls)Mockito.any())).thenReturn(enumeration);

        execution = (LDAPSyncQueryExecution)lef.createExecution(command, ec, rm, connection);
        execution.execute();
        result = execution.next();
        assertEquals(Collections.singletonList(null), result);
        assertNull(execution.next());
    }

    @Test public void testUnwrapExtract() throws Exception {
        TranslationUtility util = new TranslationUtility(RealMetadataFactory.fromDDL("CREATE FOREIGN TABLE GROUP_PEOPLE (\"member\" string options (\"teiid_ldap:unwrap\" true, \"teiid_ldap:rdn_type\" 'uid', \"teiid_ldap:dn_prefix\" 'ou=users')) OPTIONS(nameinsource 'ou=Infrastructure,ou=Support,o=DEMOCORP,c=AU', updatable true);", "x", "y"));
        Command command = util.parseCommand("select * from group_people");
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        LdapContext connection = Mockito.mock(LdapContext.class);
        LdapContext ctx = Mockito.mock(LdapContext.class);
        Mockito.when(connection.lookup("ou=Infrastructure,ou=Support,o=DEMOCORP,c=AU")).thenReturn(ctx);
        BasicAttributes attributes = new BasicAttributes(true);
        BasicAttribute attrib = new BasicAttribute("member");
        attributes.put(attrib);
        attrib.add("uid=foo,ou=users");
        attrib.add("user=bar,ou=users"); //does not match rdn type
        attrib.add("uid=bar"); //does not dn prefix

        final SearchResult sr = new SearchResult("x", null, attributes);

        NamingEnumeration<SearchResult> enumeration = new SimpleNamingEnumeration(Arrays.asList(sr).iterator());

        Mockito.when(ctx.search((String)Mockito.any(), (String)Mockito.any(), (SearchControls)Mockito.any())).thenReturn(enumeration);

        LDAPExecutionFactory lef = new LDAPExecutionFactory();
        lef.start();

        LDAPSyncQueryExecution execution = (LDAPSyncQueryExecution)lef.createExecution(command, ec, rm, connection);
        execution.execute();
        List<?> result = execution.next();
        assertEquals(Arrays.asList("foo"), result);
        assertNull(execution.next());
    }

    @Test public void testMultiAttribute() throws NamingException {
        Column c = new Column();
        c.setDefaultValue(LDAPQueryExecution.MULTIVALUED_CONCAT);
        Attribute a = LDAPUpdateExecution.createBasicAttribute("x", new Literal("a?b?c", String.class), c);
        assertEquals(3, a.size());
        assertEquals("b", Collections.list(a.getAll()).get(1));
    }

}
