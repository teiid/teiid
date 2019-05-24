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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.SortKey;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.cdk.CommandBuilder;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.metadata.Column;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;


/**
 * Test IQueryToLdapSearchParser.
 */
/**
 * @author mdrilling
 *
 */
@SuppressWarnings({"nls"})
public class TestIQueryToLdapSearchParser {

    /**
     * Get Resolved Command using SQL String and metadata.
     */
    public Command getCommand(String sql, QueryMetadataInterface metadata) {
        CommandBuilder builder = new CommandBuilder(metadata);
        return builder.getCommand(sql);
    }

    /**
     * Helper method for testing the provided LDAPSearchDetails against expected values
     * @param searchDetails the LDAPSearchDetails object
     * @param expectedContextName the expected context name
     * @param expectedContextFilter the expected context filter string
     * @param expectedAttrNameList list of expected attribute names
     * @param expectedCountLimit the expected count limit
     * @param expectedSearchScope the expected search scope
     * @param expectedSortKeys the expected sortKeys list.
     */
    public void helpTestSearchDetails(final LDAPSearchDetails searchDetails, final String expectedContextName,
            final String expectedContextFilter, final List<String> expectedAttrNameList, final long expectedCountLimit,
            final int expectedSearchScope, final SortKey[] expectedSortKeys) {

        // Get all of the actual values
        String contextName = searchDetails.getContextName();
        String contextFilter = searchDetails.getContextFilter();
        List<Column> attrList = searchDetails.getElementList();
        long countLimit = searchDetails.getCountLimit();
        int searchScope = searchDetails.getSearchScope();
        SortKey[] sortKeys = searchDetails.getSortKeys();

        // Compare actual with Expected
        assertEquals(expectedContextName, contextName);
        assertEquals(expectedContextFilter, contextFilter);

        assertEquals(attrList.size(),expectedAttrNameList.size());
        Iterator<Column> iter = attrList.iterator();
        Iterator<String> eIter = expectedAttrNameList.iterator();
        while(iter.hasNext()&&eIter.hasNext()) {
            String actualName = iter.next().getSourceName();
            String expectedName = eIter.next();
            assertEquals(actualName, expectedName);
        }

        assertEquals(expectedCountLimit, countLimit);
        assertEquals(expectedSearchScope, searchScope);
        assertArrayEquals(expectedSortKeys, sortKeys);
    }

    /**
     * Test a Query without criteria
     */
    @Test public void testSelectFrom1() throws Exception {
        LDAPSearchDetails searchDetails = helpGetSearchDetails("SELECT UserID, Name FROM LdapModel.People"); //$NON-NLS-1$

        //-----------------------------------
        // Set Expected SearchDetails Values
        //-----------------------------------
        String expectedContextName = "ou=people,dc=metamatrix,dc=com"; //$NON-NLS-1$
        String expectedContextFilter = "(objectClass=*)"; //$NON-NLS-1$

        List<String> expectedAttrNameList = new ArrayList<String>();
        expectedAttrNameList.add("uid"); //$NON-NLS-1$
        expectedAttrNameList.add("cn"); //$NON-NLS-1$

        long expectedCountLimit = -1;
        int expectedSearchScope = SearchControls.ONELEVEL_SCOPE;
        SortKey[] expectedSortKeys = null;

        helpTestSearchDetails(searchDetails, expectedContextName, expectedContextFilter, expectedAttrNameList,
                expectedCountLimit, expectedSearchScope, expectedSortKeys);

    }

    @Test public void testUpdateNull() throws Exception {
        String sql = "update LdapModel.People set userid = 1, name = null where dn = 'x'"; //$NON-NLS-1$

        QueryMetadataInterface metadata = exampleLdap();

        Update query = (Update)getCommand(sql, metadata);

        LDAPExecutionFactory config = new LDAPExecutionFactory();

        LdapContext context = Mockito.mock(LdapContext.class);

        Mockito.stub(context.lookup("")).toReturn(context);

        LDAPUpdateExecution lue = new LDAPUpdateExecution(query, context);

        lue.execute();
        ArgumentCaptor<ModificationItem[]> captor = ArgumentCaptor.forClass(ModificationItem[].class);
        Mockito.verify(context).modifyAttributes(ArgumentCaptor.forClass(String.class).capture(), captor.capture());
        ModificationItem[] modifications = captor.getValue();
        assertEquals(2, modifications.length);
        assertEquals("uid: 1", modifications[0].getAttribute().toString());
        assertEquals("cn: null", modifications[1].getAttribute().toString());
    }

    @Test public void testUpdateArray() throws Exception {
        String sql = "update LdapModel.People set userid = 1, vals = ('a','b') where dn = 'x'"; //$NON-NLS-1$

        QueryMetadataInterface metadata = exampleLdap();

        Update query = (Update)getCommand(sql, metadata);

        LDAPExecutionFactory config = new LDAPExecutionFactory();

        LdapContext context = Mockito.mock(LdapContext.class);

        Mockito.stub(context.lookup("")).toReturn(context);

        LDAPUpdateExecution lue = new LDAPUpdateExecution(query, context);

        lue.execute();
        ArgumentCaptor<ModificationItem[]> captor = ArgumentCaptor.forClass(ModificationItem[].class);
        Mockito.verify(context).modifyAttributes(ArgumentCaptor.forClass(String.class).capture(), captor.capture());
        ModificationItem[] modifications = captor.getValue();
        assertEquals(2, modifications.length);
        assertEquals("uid: 1", modifications[0].getAttribute().toString());
        assertEquals("vals: a, b", modifications[1].getAttribute().toString());
    }

    /**
     * Test a Query with a criteria
     */
    @Test public void testSelectFromWhere1() throws Exception {
        LDAPSearchDetails searchDetails = helpGetSearchDetails("SELECT UserID, Name FROM LdapModel.People WHERE Name = 'R%'"); //$NON-NLS-1$

        //-----------------------------------
        // Set Expected SearchDetails Values
        //-----------------------------------
        String expectedContextName = "ou=people,dc=metamatrix,dc=com"; //$NON-NLS-1$
        String expectedContextFilter = "(cn=R%)"; //$NON-NLS-1$

        List<String> expectedAttrNameList = new ArrayList<String>();
        expectedAttrNameList.add("uid"); //$NON-NLS-1$
        expectedAttrNameList.add("cn"); //$NON-NLS-1$

        long expectedCountLimit = -1;
        int expectedSearchScope = SearchControls.ONELEVEL_SCOPE;
        SortKey[] expectedSortKeys = null;

        helpTestSearchDetails(searchDetails, expectedContextName, expectedContextFilter, expectedAttrNameList,
                expectedCountLimit, expectedSearchScope, expectedSortKeys);

    }

    /**
     * Test a Query with a criteria
     */
    @Test public void testEscaping() throws Exception {
        LDAPSearchDetails searchDetails = helpGetSearchDetails("SELECT UserID, Name FROM LdapModel.People WHERE Name = 'R*'"); //$NON-NLS-1$

        //-----------------------------------
        // Set Expected SearchDetails Values
        //-----------------------------------
        String expectedContextName = "ou=people,dc=metamatrix,dc=com"; //$NON-NLS-1$
        String expectedContextFilter = "(cn=R\\2a)"; //$NON-NLS-1$

        List<String> expectedAttrNameList = new ArrayList<String>();
        expectedAttrNameList.add("uid"); //$NON-NLS-1$
        expectedAttrNameList.add("cn"); //$NON-NLS-1$

        long expectedCountLimit = -1;
        int expectedSearchScope = SearchControls.ONELEVEL_SCOPE;
        SortKey[] expectedSortKeys = null;

        helpTestSearchDetails(searchDetails, expectedContextName, expectedContextFilter, expectedAttrNameList,
                expectedCountLimit, expectedSearchScope, expectedSortKeys);

    }

    @Test public void testNot() throws Exception {
        LDAPSearchDetails searchDetails = helpGetSearchDetails("SELECT UserID, Name FROM LdapModel.People WHERE not (Name like 'R%' or Name like 'S%')"); //$NON-NLS-1$

        //-----------------------------------
        // Set Expected SearchDetails Values
        //-----------------------------------
        String expectedContextName = "ou=people,dc=metamatrix,dc=com"; //$NON-NLS-1$
        String expectedContextFilter = "(&(!(cn=R*))(!(cn=S*)))"; //$NON-NLS-1$

        List<String> expectedAttrNameList = new ArrayList<String>();
        expectedAttrNameList.add("uid"); //$NON-NLS-1$
        expectedAttrNameList.add("cn"); //$NON-NLS-1$

        long expectedCountLimit = -1;
        int expectedSearchScope = SearchControls.ONELEVEL_SCOPE;
        SortKey[] expectedSortKeys = null;

        helpTestSearchDetails(searchDetails, expectedContextName, expectedContextFilter, expectedAttrNameList,
                expectedCountLimit, expectedSearchScope, expectedSortKeys);

    }

    @Test public void testGT() throws Exception {
        LDAPSearchDetails searchDetails = helpGetSearchDetails("SELECT UserID, Name FROM LdapModel.People WHERE Name > 'R'"); //$NON-NLS-1$

        //-----------------------------------
        // Set Expected SearchDetails Values
        //-----------------------------------
        String expectedContextName = "ou=people,dc=metamatrix,dc=com"; //$NON-NLS-1$
        String expectedContextFilter = "(!(cn<=R))"; //$NON-NLS-1$

        List<String> expectedAttrNameList = new ArrayList<String>();
        expectedAttrNameList.add("uid"); //$NON-NLS-1$
        expectedAttrNameList.add("cn"); //$NON-NLS-1$

        long expectedCountLimit = -1;
        int expectedSearchScope = SearchControls.ONELEVEL_SCOPE;
        SortKey[] expectedSortKeys = null;

        helpTestSearchDetails(searchDetails, expectedContextName, expectedContextFilter, expectedAttrNameList,
                expectedCountLimit, expectedSearchScope, expectedSortKeys);
    }

    @Test public void testLT() throws Exception {
        LDAPSearchDetails searchDetails = helpGetSearchDetails("SELECT UserID, Name FROM LdapModel.People WHERE Name < 'R'"); //$NON-NLS-1$

        //-----------------------------------
        // Set Expected SearchDetails Values
        //-----------------------------------
        String expectedContextName = "ou=people,dc=metamatrix,dc=com"; //$NON-NLS-1$
        String expectedContextFilter = "(!(cn>=R))"; //$NON-NLS-1$

        List<String> expectedAttrNameList = new ArrayList<String>();
        expectedAttrNameList.add("uid"); //$NON-NLS-1$
        expectedAttrNameList.add("cn"); //$NON-NLS-1$

        long expectedCountLimit = -1;
        int expectedSearchScope = SearchControls.ONELEVEL_SCOPE;
        SortKey[] expectedSortKeys = null;

        helpTestSearchDetails(searchDetails, expectedContextName, expectedContextFilter, expectedAttrNameList,
                expectedCountLimit, expectedSearchScope, expectedSortKeys);
    }

    private LDAPSearchDetails helpGetSearchDetails(String queryString) throws Exception {
        QueryMetadataInterface metadata = exampleLdap();

        Select query = (Select)getCommand(queryString, metadata);

        LDAPExecutionFactory config = new LDAPExecutionFactory();

        IQueryToLdapSearchParser searchParser = new IQueryToLdapSearchParser(config);

        LDAPSearchDetails searchDetails = searchParser.translateSQLQueryToLDAPSearch(query);
        return searchDetails;
    }

    public static QueryMetadataInterface exampleLdap() throws Exception {
        String ddl = "create foreign table People (UserID string options (nameinsource 'uid'), Name string options (nameinsource 'cn'), dn string, vals string[]) options (nameinsource 'ou=people,dc=metamatrix,dc=com');"
                + "create foreign table People_Groups (user_dn string options (nameinsource 'dn'), groupname string options (nameinsource 'memberOf', \"teiid_ldap:dn_prefix\" 'ou=groups,dc=metamatrix,dc=com', \"teiid_ldap:rdn_type\" 'cn')) options (nameinsource 'ou=people,dc=metamatrix,dc=com')";
        return RealMetadataFactory.fromDDL(ddl, "x", "LdapModel");
    }

    @Test public void testLike() throws Exception {
        String query = "Name like 'R*%'";
        String expectedContextFilter = "(cn=R\\2a*)"; //$NON-NLS-1$

        helpTestLike(query, expectedContextFilter);
    }

    @Test public void testLikeEscaped() throws Exception {
        String query = "Name like 'R%*\\%\\_' escape '\\'";
        String expectedContextFilter = "(cn=R*\\2a%_)"; //$NON-NLS-1$

        helpTestLike(query, expectedContextFilter);
    }

    @Test(expected=TranslatorException.class) public void testLikeUnsupported() throws Exception {
        String query = "Name like 'R*_'";
        String expectedContextFilter = null;

        helpTestLike(query, expectedContextFilter);
    }

    @Test(expected=TranslatorException.class) public void testLikeUnsupported1() throws Exception {
        String query = "Name like 'R\\%_' escape '\\'";
        String expectedContextFilter = null;

        helpTestLike(query, expectedContextFilter);
    }

    private void helpTestLike(String query, String expectedContextFilter)
            throws Exception {
        LDAPSearchDetails searchDetails = helpGetSearchDetails("SELECT UserID FROM LdapModel.People WHERE " + query); //$NON-NLS-1$

        // Set Expected SearchDetails Values
        //-----------------------------------
        String expectedContextName = "ou=people,dc=metamatrix,dc=com"; //$NON-NLS-1$


        List<String> expectedAttrNameList = new ArrayList<String>();
        expectedAttrNameList.add("uid"); //$NON-NLS-1$

        long expectedCountLimit = -1;
        int expectedSearchScope = SearchControls.ONELEVEL_SCOPE;
        SortKey[] expectedSortKeys = null;

        helpTestSearchDetails(searchDetails, expectedContextName, expectedContextFilter, expectedAttrNameList,
                expectedCountLimit, expectedSearchScope, expectedSortKeys);
    }

    @Test public void testJoin() throws Exception {
        LDAPSearchDetails searchDetails = helpGetSearchDetails("SELECT UserID, Name, people_groups.groupname FROM LdapModel.People inner join people_groups on people.dn = people_groups.user_dn where Name = 'R%'"); //$NON-NLS-1$

        //-----------------------------------
        // Set Expected SearchDetails Values
        //-----------------------------------
        String expectedContextName = "ou=people,dc=metamatrix,dc=com"; //$NON-NLS-1$
        String expectedContextFilter = "(cn=R%)"; //$NON-NLS-1$

        List<String> expectedAttrNameList = new ArrayList<String>();
        expectedAttrNameList.add("uid"); //$NON-NLS-1$
        expectedAttrNameList.add("cn"); //$NON-NLS-1$
        expectedAttrNameList.add("memberOf");

        long expectedCountLimit = -1;
        int expectedSearchScope = SearchControls.ONELEVEL_SCOPE;
        SortKey[] expectedSortKeys = null;

        helpTestSearchDetails(searchDetails, expectedContextName, expectedContextFilter, expectedAttrNameList,
                expectedCountLimit, expectedSearchScope, expectedSortKeys);

    }

    @Test public void testRdnTypePredicates() throws Exception {
        LDAPSearchDetails searchDetails = helpGetSearchDetails("SELECT people_groups.groupname FROM people_groups where groupname like 'R%' and groupname < 'q'"); //$NON-NLS-1$

        //-----------------------------------
        // Set Expected SearchDetails Values
        //-----------------------------------
        String expectedContextName = "ou=people,dc=metamatrix,dc=com"; //$NON-NLS-1$
        String expectedContextFilter = "(&(memberOf=cn=R*,ou=groups,dc=metamatrix,dc=com)(!(memberOf>=cn=q,ou=groups,dc=metamatrix,dc=com)))"; //$NON-NLS-1$

        List<String> expectedAttrNameList = new ArrayList<String>();
        expectedAttrNameList.add("memberOf");

        long expectedCountLimit = -1;
        int expectedSearchScope = SearchControls.ONELEVEL_SCOPE;
        SortKey[] expectedSortKeys = null;

        helpTestSearchDetails(searchDetails, expectedContextName, expectedContextFilter, expectedAttrNameList,
                expectedCountLimit, expectedSearchScope, expectedSortKeys);

    }

}

