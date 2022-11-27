package org.teiid.resource.adapter.couchbase;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.resource.ResourceException;

import org.junit.Ignore;
import org.junit.Test;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

@Ignore("Ignore due to this test depend on remote Couchbase server")
public class TestCouchbaseConnection {

    private CouchbaseConnectionImpl sample() throws ResourceException {

        CouchbaseManagedConnectionFactory mcf = new CouchbaseManagedConnectionFactory();
        mcf.setConnectionString("10.66.192.120"); //$NON-NLS-1$
        mcf.setKeyspace("default"); //$NON-NLS-1$
        return mcf.createConnectionFactory().getConnection();
    }

    @Test
    public void testKeyspaces () throws Exception {
        CouchbaseConnectionImpl conn = sample();
        Set<String> keyspaces = new HashSet<>();
        N1qlQueryResult result = conn.execute("SELECT * FROM system:keyspaces"); //$NON-NLS-1$
        Iterator<N1qlQueryRow> rows = result.rows();
        while(rows.hasNext()) {
            N1qlQueryRow row = rows.next();
            JsonObject json = (JsonObject) row.value().get("keyspaces"); //$NON-NLS-1$
            keyspaces.add(json.getString("name")); //$NON-NLS-1$
        }
        assertTrue(keyspaces.contains("default")); //$NON-NLS-1$
        conn.close();
    }
}
