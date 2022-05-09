package dbs.oracle_project_template;

//oracledb imports
import oracle.jdbc.OracleDriver;
import oracle.jdbc.pool.OracleDataSource;
// casandra imports

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
//java imports
import java.sql.Connection;
// import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import dbs.oracle_project_template.domain.OracleUser;
import dbs.oracle_project_template.domain.CassComment;
import dbs.oracle_project_template.domain.CassPost;
import dbs.oracle_project_template.domain.CassRating;
import dbs.oracle_project_template.domain.CassUser;
import dbs.oracle_project_template.models.Comment;
import dbs.oracle_project_template.models.Post;
import dbs.oracle_project_template.models.Rating;
import dbs.oracle_project_template.models.User;


public class App {

    public static void main(String[] args) throws Exception {
        return;
    }
}
