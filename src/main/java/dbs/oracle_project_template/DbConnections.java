package dbs.oracle_project_template;

import java.sql.Connection;
import com.datastax.oss.driver.api.core.CqlSession;

public class DbConnections {
    private Connection oracleConnection;
    private CqlSession cassandraSession;

    public DbConnections(Connection oracleConnection, CqlSession cassandraSession)
    {
        this.oracleConnection = oracleConnection;
        this.cassandraSession = cassandraSession;
    }

    public Connection getOracleConnection(){
        return oracleConnection;
    }

    public CqlSession getCassandraConnection(){
        return cassandraSession;
    }
}
