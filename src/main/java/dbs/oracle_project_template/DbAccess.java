package dbs.oracle_project_template;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;

// casandra imports
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
//oracledb imports
import oracle.jdbc.OracleDriver;
import oracle.jdbc.pool.OracleDataSource;

public class DbAccess {
    private static QueryModel queryModel;
    private static WriteModel writeModel;
    private static DbConnections dbConnections;

    public static QueryModel queryModel()
    {
        if(queryModel == null)
            queryModel = new QueryModel(getDbConnections());
        return queryModel;
    }

    public static WriteModel writeModel()
    {
        if(writeModel == null)
            writeModel = new WriteModel(getDbConnections());
        return writeModel;
    }

    public static void close()
    {
        try{
            dbConnections.getOracleConnection().close();
        } catch (Exception e) {
        }
        try {
            dbConnections.getCassandraConnection().close();
        } catch (Exception e) {
        }
        dbConnections = null;
    }

    public static DbConnections getDbConnections(){
        if(dbConnections == null)
            dbConnections = new DbConnections(oracleCreateConnection(), cassandraCreateSession());
        return dbConnections;
    }

    private static Connection oracleCreateConnection() {
        String[] config = new String[3];
        try {
            File myObj = new File(".oracleConnection");
            Scanner myReader = new Scanner(myObj);
            int i = 0;
            while (myReader.hasNextLine()) {
                config[i] = myReader.nextLine();
                i = i + 1;
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-2);
        }
        try {
            OracleDataSource ods = new OracleDataSource();
            ods.setURL(config[0]);
            ods.setUser(config[1]);
            ods.setPassword(config[2]);
            Connection connection = ods.getConnection();
            return connection;
        } catch (SQLException sqlEx) {
            System.err.println("SQLException: " + sqlEx.getMessage());
        }
        return null;
    }

    private static CqlSession cassandraCreateSession(){
        CqlSession session = CqlSession
            .builder()
            .addContactPoint(new InetSocketAddress("cassandradb", 9042))
            .withLocalDatacenter("datacenter1")
            .withKeyspace("social_network")
            .build();
        return session;
    }
}
