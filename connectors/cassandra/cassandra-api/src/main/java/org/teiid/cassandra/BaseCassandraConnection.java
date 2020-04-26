package org.teiid.cassandra;

import com.datastax.driver.core.*;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.cassandra.CassandraConnection;

import java.util.List;
import java.util.Set;

public abstract class BaseCassandraConnection implements CassandraConnection {

    private Session session = null;
    private Metadata metadata = null;
    private VersionNumber version;

    //to be provided by sub-class

    private Cluster cluster = null;
    private String keyspace;

    public BaseCassandraConnection(Cluster cassandraCluster,String keyspace, Metadata metadata) {
        this.cluster = cassandraCluster;
        this.metadata = metadata;
        this.keyspace = keyspace;
    }

    public BaseCassandraConnection(Cluster cassandraCluster, String keyspace) {
        this.cluster = cassandraCluster;

        this.metadata = cluster.getMetadata();

        this.session = cluster.connect(keyspace);

        this.keyspace = keyspace;

        Set<Host> allHosts = this.cluster.getMetadata().getAllHosts();
        if (!allHosts.isEmpty()) {
            Host host = allHosts.iterator().next();
            this.version = host.getCassandraVersion();
        }
    }

    public BaseCassandraConnection(String keyspace){
        this.keyspace = keyspace;
    }

    @Override
    public ResultSetFuture executeQuery(String query) {
        return session.executeAsync(query);
    }

    @Override
    public KeyspaceMetadata keyspaceInfo() throws TranslatorException {
        KeyspaceMetadata result = metadata.getKeyspace(keyspace);
        if (result == null && keyspace.length() > 2 && keyspace.charAt(0) == '"' && keyspace.charAt(keyspace.length() - 1) == '"') {
            //try unquoted
            keyspace = keyspace.substring(1, keyspace.length() - 1);
            result = metadata.getKeyspace(keyspace);
        }
        if (result == null) {
            throw new TranslatorException(keyspace);
        }
        return result;
    }

    @Override
    public ResultSetFuture executeBatch(List<String> updates) {
        BatchStatement bs = new BatchStatement();
        for (String update : updates) {
            bs.add(new SimpleStatement(update));
        }
        return session.executeAsync(bs);
    }

    @Override
    public ResultSetFuture executeBatch(String update, List<Object[]> values) {
        PreparedStatement ps = session.prepare(update);
        BatchStatement bs = new BatchStatement();
        for (Object[] bindValues : values) {
            BoundStatement bound = ps.bind(bindValues);
            bs.add(bound);
        }
        return session.executeAsync(bs);
    }

    @Override
    public VersionNumber getVersion() {
        return version;
    }

    @Override
    public void close() throws Exception{
        if(cluster != null){
            cluster.close();
        }
    }
}
