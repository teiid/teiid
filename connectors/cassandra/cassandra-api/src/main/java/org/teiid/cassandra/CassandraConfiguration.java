package org.teiid.cassandra;

public interface CassandraConfiguration {

        String getAddress();

        String getKeyspace();

        String getUsername();

        String getPassword();

        Integer getPort();
}
