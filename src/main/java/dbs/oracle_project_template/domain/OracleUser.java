package dbs.oracle_project_template.domain;

import java.sql.*;
import java.util.ArrayList;

import dbs.oracle_project_template.models.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleUser {

    //templates for CRUD operations for User
    public static final transient String SQL_SELECT_ALL = "SELECT * FROM soc_net_user"; 
    public static final transient String SQL_SELECT_BY_ID = SQL_SELECT_ALL + " WHERE user_id = ?";
    public static final transient String SQL_SELECT_BY_EMAIL = SQL_SELECT_ALL + " WHERE email = ?";
    public static final transient String SQL_INSERT = "INSERT INTO soc_net_user"
        + " (user_id, first_name, last_name, email, password, address, phone)" 
        + " VALUES (NULL, ?, ?, ?, ?, ?, ?)";
    public static final transient String SQL_UPDATE = "UPDATE soc_net_user"
        +" SET first_name = ?, last_name = ?, email = ?, password = ?, address = ?, phone = ?"
        +" WHERE user_id = ? AND (first_name <> ? OR last_name <> ? OR email <> ? OR password <> ? OR address <> ? OR phone <> ?)";
    public static final transient String SQL_DELETE = "DELETE FROM soc_net_user WHERE user_id = ?";
    
    //templates for CRUD operations for Followers
    public static final transient String SQL_SELECT_ALL_FOLLOWERS = "SELECT " 
    + "soc_net_user.user_id, soc_net_user.first_name, soc_net_user.last_name,soc_net_user.email,"
    + "soc_net_user.password, soc_net_user.address,soc_net_user.phone FROM soc_net_follower "
    + "JOIN soc_net_user ON (soc_net_follower.follower_id = soc_net_user.user_id) "
    + "WHERE soc_net_follower.user_id = ?";
    public static final transient String SQL_INSERT_FOLLOWER = "INSERT INTO soc_net_follower"
        + " (user_id, follower_id) VALUES ( ?, ?)";
    public static final transient String SQL_DELETE_FOLLOWER = "DELETE FROM soc_net_follower" 
    + " WHERE (user_id = ? AND follower_id = ?)";

    //defined columns from table soc_net_user
    public static final transient String COLUMN_USER_ID = "user_id";
    public static final transient String COLUMN_FIRST_NAME = "first_name";
    public static final transient String COLUMN_LAST_NAME = "last_name";
    public static final transient String COLUMN_EMAIL = "email";
    public static final transient String COLUMN_PASSWORD = "password";
    public static final transient String COLUMN_ADDRESS = "address";
    public static final transient String COLUMN_PHONE = "phone"; 

    private static final transient Logger LOGGER = LoggerFactory.getLogger(OracleUser.class);
    private static Connection connection;
    
    public OracleUser() {
   
    }

    public static void setConnection(Connection defaultConnection) {
        connection = defaultConnection;
    }

    /**
     * Load all oracle uers
     * @return ArrayList<User>
     */
    public static ArrayList<User> loadAll() {
        LOGGER.info("Loading all OracleUsers");
        ArrayList<User> users = new ArrayList<>();
        try (Statement statment = connection.createStatement()) {
            try (ResultSet resul = statment.executeQuery("select * from soc_net_user")) {
                while (resul.next()) {
                    User user = new User(resul.getInt(COLUMN_USER_ID),resul.getString(COLUMN_FIRST_NAME),
                    resul.getString(COLUMN_LAST_NAME),resul.getString(COLUMN_EMAIL),resul.getString(COLUMN_PASSWORD),
                    resul.getString(COLUMN_ADDRESS),resul.getString(COLUMN_PHONE));
                    LOGGER.info("Loaded OracleUser {}", user );
                    users.add(user);
                }
            } 
        } catch (SQLException sqlEx) {
            LOGGER.error("SQL error when loading by SELECT.", sqlEx);
        }
        return users;
    }

    /**
     * Load oracle user by user_id
     * @param id
     * @return User
     */
    public static User load(Integer id) {
        LOGGER.info("Loading OracleUser with id {}",id);
    
        try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT_BY_ID)) {
            preparedStatement.setInt(1, id);
            try (ResultSet result = preparedStatement.executeQuery()) {
                if ( result.next() ) {
                    User user = new User(result.getInt(COLUMN_USER_ID),result.getString(COLUMN_FIRST_NAME),
                    result.getString(COLUMN_LAST_NAME),result.getString(COLUMN_EMAIL),result.getString(COLUMN_PASSWORD),
                    result.getString(COLUMN_ADDRESS),result.getString(COLUMN_PHONE));
                    LOGGER.info("Loaded OracleUser {}", user);
                    return user;
                } else {
                    LOGGER.warn("OracleUser with ID {} does not exists so it cannot be loaded", id);
                    return null;
                }
            } 
        } catch (SQLException sqlEx) {
            LOGGER.error("SQL error when loading by SELECT.", sqlEx);
        }
        return null;
    }

    /**
     * Load oracle user by email
     * @param email
     * @return User
     */
    public static User load(String email) {
        LOGGER.info("Loading OracleUser with id {}",email);
    
        try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT_BY_EMAIL)) {
            preparedStatement.setString(1, email);
            try (ResultSet result = preparedStatement.executeQuery()) {
                if ( result.next() ) {
                    User user = new User(result.getInt(COLUMN_USER_ID),result.getString(COLUMN_FIRST_NAME),
                    result.getString(COLUMN_LAST_NAME),result.getString(COLUMN_EMAIL),result.getString(COLUMN_PASSWORD),
                    result.getString(COLUMN_ADDRESS),result.getString(COLUMN_PHONE));
                    LOGGER.info("Loaded OracleUser {}", user);
                    return user;
                } else {
                    LOGGER.warn("OracleUser with EMAIL {} does not exists so it cannot be loaded", email);
                    return null;
                }
            } 
        } catch (SQLException sqlEx) {
            LOGGER.error("SQL error when loading by SELECT.", sqlEx);
        }
        return null;
    }

    /**
     * Delete user by user_id
     * @param id
     * @return number of deleted rows 
     */
    public static Integer delete(Integer id) {
        LOGGER.info("Deleting oracle User {}", id);
        try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_DELETE)) {
            preparedStatement.setInt(1, id);
            Integer deletedRows = preparedStatement.executeUpdate();
            LOGGER.info("{} oracle Users deleted", deletedRows);
            return deletedRows;
        } catch (SQLException e) {
            LOGGER.error("SQL error when deleting by DELETE.", e);
            return 0;
        }
    }

    /**
     * Insert user from model to oracle DB
     * @param user
     * @return Integer, number of inserted rows 
     * @throws SQLException ( checking unique email )
     */
    public static Integer insert(User user) throws SQLException {
        LOGGER.info("Insert new User into a oracle DB {}", user);
        try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setString(1, user.getFirst_name());
            preparedStatement.setString(2, user.getLast_name());
            preparedStatement.setString(3, user.getEmail());
            preparedStatement.setString(4, user.getPassword());
            preparedStatement.setString(5, user.getAddress());
            preparedStatement.setString(6, user.getPhone());
            Integer insertedRows = preparedStatement.executeUpdate();
            LOGGER.info("{} oracle Users inserted", insertedRows);
            // load new id. doesnt work. workaround: make query by email
            // if(insertedRows == 0)
            //     return insertedRows;
            // ResultSet rs = preparedStatement.getGeneratedKeys();
            // if (rs.next()) {
            //     user.setUser_id(Integer.valueOf(rs.getInt(1)));
            // }else{
            //     LOGGER.error("no id returned while inserting new user");
            // }
            return insertedRows;
        } 
    } 

    /**
     * Update user from model to oracle DB
     * @param user
     * @return Integer number of updated rows
     * @throws SQLException
     */
    public static Integer update(User user) throws SQLException {
        LOGGER.info("Update oracle User {}", user);
        try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_UPDATE)) {
            preparedStatement.setString(1, user.getFirst_name());
            preparedStatement.setString(2, user.getLast_name());
            preparedStatement.setString(3, user.getEmail());
            preparedStatement.setString(4, user.getPassword());
            preparedStatement.setString(5, user.getAddress());
            preparedStatement.setString(6, user.getPhone());
            preparedStatement.setInt(7, user.getUser_id());
            preparedStatement.setString(8, user.getFirst_name());
            preparedStatement.setString(9, user.getLast_name());
            preparedStatement.setString(10, user.getEmail());
            preparedStatement.setString(11, user.getPassword());
            preparedStatement.setString(12, user.getAddress());
            preparedStatement.setString(13, user.getPhone());
            Integer rowsUpdated = preparedStatement.executeUpdate();
            LOGGER.info("{} oracle Users updated", rowsUpdated);
            return rowsUpdated;
        }
    }

    /**
     * Load all followers of user
     * @param id user id to select follwers
     * @return ArrayList<User> followers of user
     */
    public static ArrayList<User> loadFollowersOfUser(Integer id) {
        LOGGER.info("Loading all followers of user id {}", id);
        ArrayList<User> followers = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT_ALL_FOLLOWERS)) {
            preparedStatement.setInt(1,id);
            try (ResultSet result = preparedStatement.executeQuery()) {
                while (result.next()) {
                    User follower = new User(result.getInt(COLUMN_USER_ID),result.getString(COLUMN_FIRST_NAME),
                    result.getString(COLUMN_LAST_NAME),result.getString(COLUMN_EMAIL),result.getString(COLUMN_PASSWORD),
                    result.getString(COLUMN_ADDRESS),result.getString(COLUMN_PHONE));
                    LOGGER.info("Loaded Follower {}", follower);
                    followers.add(follower);
                }
            }
        } catch(SQLException e) {
            LOGGER.error("SQL error when SELECTING FOLLOWERS.", e);
        }
        return followers;
    }
    
    /**
     * add follower to user
     * @param userID
     * @param followerID
     * @return number of inserted rows
     * @throws SQLException ( max 5 followers )
     */
    public static Integer insertFollower(Integer userID,Integer followerID) throws SQLException {
        LOGGER.info("Insert new Follower {} for user {} into oracle DB",followerID,userID );
        try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_INSERT_FOLLOWER)) {
            preparedStatement.setInt(1, userID);
            preparedStatement.setInt(2, followerID);
            Integer insertedRows = preparedStatement.executeUpdate();
            LOGGER.info("{} oracle Users inserted", insertedRows);
            return insertedRows;
        } 
    } 

    /**
     * unfollower user
     * @param userID
     * @param followerID
     * @return number of returned followers
     */
    public static Integer deleteFollower(Integer userID,Integer followerID)  {
        LOGGER.info("Delete Follower {} from user {} in oracle DB",followerID,userID );
        try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_DELETE_FOLLOWER)) {
            preparedStatement.setInt(1, userID);
            preparedStatement.setInt(2, followerID);
            Integer deletedRows = preparedStatement.executeUpdate();
            LOGGER.info("{} oracle Users deleted", deletedRows);
            return deletedRows;
        } catch (SQLException e) {
            LOGGER.error("SQL error when deleting followers by DELETE.", e);
            return 0;
        }
    } 
}