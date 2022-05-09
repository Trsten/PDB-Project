package dbs.oracle_project_template.domain;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import dbs.oracle_project_template.models.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassUser {

    //templates for CRUD operations for User
    public static final transient String CQL_SELECT_ALL = "SELECT * FROM user"; 
    public static final transient String CQL_SELECT_BY_EMAIL = CQL_SELECT_ALL + " WHERE email = ? ";
    public static final transient String CQL_INSERT = "INSERT INTO user"
        + " (user_id, first_name, last_name, email, password, address, phone, followers)" 
        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    public static final transient String CQL_UPDATE = "UPDATE user"
        +" SET user_id = ?, first_name = ?, last_name = ?, password = ?, address = ?, phone = ?"
        +" WHERE email = ?";
    public static final transient String CQL_DELETE = "DELETE FROM user WHERE email = ?";
    
    //templates for CRUD operations for Followers
    public static final transient String CQL_SELECT_FOLLOWERS = "SELECT * FROM user"
    + " WHERE email IN ( ? )";
    public static final transient String CQL_SELECT_FOLLOWERS_EMAIL = "SELECT followers from user WHERE email = ?";
    public static final transient String CQL_INSERT_FOLLOWER = "UPDATE user"
        + " SET followers = followers + ? WHERE email = ?";
    public static final transient String CQL_DELETE_FOLLOWER = "UPDATE user"
    + " SET followers = followers - ? WHERE email = ?";


    //defined columns of user
    public static final transient String COLUMN_USER_ID = "user_id";
    public static final transient String COLUMN_FIRST_NAME = "first_name";
    public static final transient String COLUMN_LAST_NAME = "last_name";
    public static final transient String COLUMN_EMAIL = "email";
    public static final transient String COLUMN_PASSWORD = "password";
    public static final transient String COLUMN_ADDRESS = "address";
    public static final transient String COLUMN_PHONE = "phone"; 
    public static final transient String COLUMN_FOLLOWERS = "followers"; 

    private static final transient Logger LOGGER = LoggerFactory.getLogger(OracleUser.class);
    private static CqlSession session;

    public CassUser() {
    
    }

    public static void setConnection(CqlSession defaultSession) {
        session = defaultSession;
    }

    public static List<User> loadAll() {
        LOGGER.info("Loading all Cassandra Users");
        List<User> users = new ArrayList<>();
        try {
            List<Row> result = session.execute(CQL_SELECT_ALL).all();    
        for (Row row : result) {
            User user = new User(row.getInt(COLUMN_USER_ID),row.getString(COLUMN_FIRST_NAME),
                row.getString(COLUMN_LAST_NAME),row.getString(COLUMN_EMAIL),row.getString(COLUMN_PASSWORD),
                row.getString(COLUMN_ADDRESS),row.getString(COLUMN_PHONE));
            users.add(user);
            LOGGER.info("Loaded Cassandra User {}",user);
        }        
        } catch( Exception e) {
            LOGGER.error("CQL error when loading all by SELECT.", e);
        }
        return users;
    }

   /**
     * Load cassandra user by email
     * @param email
     * @return User
     */
    public static User load(String email) {
        LOGGER.info("Loading Cassandra User with email {}",email);
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT_BY_EMAIL);
            BoundStatement bound = prepared.bind(email);
            Row row = session.execute(bound).one();    
            if ( row != null ) {
                User user = new User(row.getInt(COLUMN_USER_ID),row.getString(COLUMN_FIRST_NAME),
                    row.getString(COLUMN_LAST_NAME),row.getString(COLUMN_EMAIL),row.getString(COLUMN_PASSWORD),
                    row.getString(COLUMN_ADDRESS),row.getString(COLUMN_PHONE));
                LOGGER.info("Loaded OracleUser {}", user);
                return user;
            } else {
                LOGGER.warn("Cassandra User with EMAIL {} does not exists so it cannot be loaded", email);
                return null;
            }
        } catch( Exception e) {
            LOGGER.error("CQL error when loading by SELECT.", e);
        }
        return null;
    }

    /**
     * Delete user by user_id from cassandra DB
     * @param email
     * @return Boolean, true if operation was aplied
     */
    public static Boolean delete(String email) {
        LOGGER.info("Deleting user with EMAIL {} from Cassandra User",email);
        try {
            PreparedStatement prepared = session.prepare(CQL_DELETE);
            BoundStatement bound = prepared.bind(email);
            ResultSet result = session.execute(bound);
            LOGGER.info("Delete Cassandra User success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when DELETE.", e);
            return false;
        }
    }

    /**
     * Insert user from model to cassandra DB
     * @param user
     * @return Boolean, return true if inserted sucessfuly
     */
    public static Boolean insert(User user) {
        LOGGER.info("Inserting to Cassandra new User");
        try {
            PreparedStatement prepared = session.prepare(CQL_INSERT);
            BoundStatement bound = prepared.bind(user.getUser_id(),user.getFirst_name(),user.getLast_name(),
                user.getEmail(),user.getPassword(),user.getAddress(),user.getPhone(),null);
            ResultSet result = session.execute(bound);
            LOGGER.info("Inserting new Cassandra User success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when INSERT.", e);
            return false;
        }
    }
    
    /**
     * Update user from model to cassandra DB, It updates only user no followers
     * @param user
     * @return Boolean, return true if updated sucessfuly
     */
    public static Boolean update(User user) {
        LOGGER.info("Update user in Cassandra");
        try {
            PreparedStatement prepared = session.prepare(CQL_UPDATE);
            BoundStatement bound = prepared.bind(user.getUser_id(),user.getFirst_name(),user.getLast_name(),
                user.getPassword(),user.getAddress(),user.getPhone(),user.getUser_id());
            ResultSet result = session.execute(bound);
            LOGGER.info("Update Cassandra User success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when UPDATE.", e);
            return false;
        }
    }
    
    /**
     * Load followers from cassandra DB
     * @param email
     * @return Set<String>, list of user_id of followers
     */
    public static Set<String> loadFollowersEmail(String email) {
        LOGGER.info("Loading followers from user with EMAIL {} from cassandra",email);
        Set<String> followers = new HashSet<String>();
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT_FOLLOWERS_EMAIL);
            BoundStatement bound = prepared.bind(email);
            Row row = session.execute(bound).one();
            followers = row.getSet(COLUMN_FOLLOWERS, String.class);
            LOGGER.info("List of followers from cassandra {}", followers);
            return followers;
        } catch( Exception e) {
            LOGGER.error("CQL error when LOAD Followers Emails.", e);
            return followers;
        }
    }


    /**
     * Load followers from cassandra DB
     * @param email
     * @return List<User>, list of user_id of followers
     */
    public static List<User> loadFollowers(String email) {
        LOGGER.info("Loading followers from user with EMAIL {} from cassandra",email);
        List<User> users = new ArrayList<User>();
        Set<String> followers = loadFollowersEmail(email);
        String listFollowers = "";
        if ( !followers.isEmpty() ) {
            for (String followerEmail : followers) {
                listFollowers += "\'"+ followerEmail +"\', ";
            }
            listFollowers = listFollowers.substring(0, listFollowers.length() - 2);
        }
        try {
            List<Row> rows = session.execute(CQL_SELECT_ALL + " WHERE email IN ( " + listFollowers + " )").all();
            for (Row row : rows) {
                User user = new User(row.getInt(COLUMN_USER_ID),row.getString(COLUMN_FIRST_NAME),
                    row.getString(COLUMN_LAST_NAME),row.getString(COLUMN_EMAIL),row.getString(COLUMN_PASSWORD),
                    row.getString(COLUMN_ADDRESS),row.getString(COLUMN_PHONE));
                users.add(user);
                LOGGER.info("user {} has follower {} ",email, user);
            }
        } catch( Exception e) {
            LOGGER.error("CQL error when LOAD Followers.", e);
        }
        return users;
    }

    /**
     * Insert new follower to user in cassandra DB
     * @param userEmail,followerEmail
     * @return Boolean, if inserted succesfuly
     */
    public static Boolean insertFollower(String userEmail, String followerEmail) {
        LOGGER.info("Insert new Follower {} for user {} into cassandra DB",followerEmail,userEmail );
        Set<String> newFollower = new HashSet<>();
        newFollower.add(followerEmail);
        try {
            PreparedStatement prepared = session.prepare(CQL_INSERT_FOLLOWER);
            BoundStatement bound = prepared.bind( newFollower ,userEmail);
            ResultSet result = session.execute(bound);
            LOGGER.info("Inserting new Follower to Cassandra User success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when INSERT Follower.", e);
            return false;
        }
    }

    /**
     * Delete new follower to user in cassandra DB
     * @param userEmail,followerEmail
     * @return Boolean, if deleted succesfuly
     */
    public static Boolean deleteFollower(String userEmail, String followerEmail) {
        LOGGER.info("Delete Follower {} from user {} in cassandra DB",followerEmail,userEmail );
        Set<String> newFollower = new HashSet<>();
        newFollower.add(followerEmail);
        try {
            PreparedStatement prepared = session.prepare(CQL_DELETE_FOLLOWER);
            BoundStatement bound = prepared.bind(newFollower,userEmail);
            ResultSet result = session.execute(bound);
            LOGGER.info("Deleting Follower from Cassandra User success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when DELETE FOLLOWER.", e);
            return false;
        }
    }
}
