package dbs.oracle_project_template.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dbs.oracle_project_template.models.Post;
import dbs.oracle_project_template.models.Rating;

public class CassRating {
   
    private static final transient Logger LOGGER = LoggerFactory.getLogger(OracleUser.class);
    private static CqlSession session;

    //defined columns of comments
    public static final transient String COLUMN_POST_ID = "post_id";
    public static final transient String COLUMN_VALUE = "value";
    public static final transient String COLUMN_USER_EMAIL = "user_email";
        
    //templates for CRUD operations for Post
    public static final transient String CQL_SELECT_ALL = "SELECT * FROM rating"; 
    public static final transient String CQL_SELECT_AVG_RATING = "SELECT AVG(value) AS average FROM rating"
        + " WHERE post_id = ?";
    public static final transient String CQL_SELECT_COUNT_RATING = "SELECT CAST(COUNT(*) AS int) FROM rating"
        + " WHERE post_id = ?";
    public static final transient String CQL_SELECT = CQL_SELECT_ALL + " WHERE post_id = ?";
    public static final transient String CQL_SELECT_ONE = CQL_SELECT_ALL + " WHERE post_id = ? AND user_email = ?";
    public static final transient String CQL_INSERT = "INSERT INTO rating"
        + " (post_id, value, user_email) VALUES (?, ?, ?)";
    public static final transient String CQL_UPDATE = "UPDATE rating"
        +" SET value = ? WHERE post_id = ? AND user_email = ?";
    public static final transient String CQL_DELETE = "DELETE FROM rating WHERE post_id = ? AND user_email = ?";

    public static void setConnection(CqlSession defaultSession) {
        session = defaultSession;
    }

    /**
     * Load cassandra rating of selected user email and post id
     * @param postID user email
     * @return List<Rating>
     */
    public static List<Rating> load(UUID postID) {
        LOGGER.info("Loading Cassandra Rating with POST_ID {}",postID);
        List<Rating> ratings = new ArrayList<>();
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT);
            BoundStatement bound = prepared.bind(postID);
            List<Row> result = session.execute(bound).all();
            for (Row row : result) {
                Rating rating = new Rating(row.getUuid(COLUMN_POST_ID),
                    (row.getDouble(COLUMN_VALUE)),row.getString(COLUMN_USER_EMAIL));
                ratings.add(rating);
                LOGGER.info("Loaded Cassandra Comment {}", rating);
            }        
        } catch( Exception e) {
            LOGGER.error("CQL error when loading by SELECT.", e);
        }
        return ratings;
    }

    /**
     * Load cassandra rating of selected user email and post id
     * @param postID,email
     * @return Rating
     */
    public static Rating load(UUID postID,String email) {
        LOGGER.info("Loading Cassandra Rating with POST_ID {} AND EMAIL {}",postID,email);
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT_ONE);
            BoundStatement bound = prepared.bind(postID,email);
            Row result = session.execute(bound).one();
            
            if ( result != null ) {
                Rating rating = new Rating(result.getUuid(COLUMN_POST_ID),
                    (result.getDouble(COLUMN_VALUE)),result.getString(COLUMN_USER_EMAIL));
                LOGGER.info("Loaded Rating {}", rating);
                return rating;
            } else {
                LOGGER.warn("Cassandra Rating with EMAIL {} AND POST_ID {} does not exists so it cannot be loaded", email,postID);
                return null;
            }      
        } catch( Exception e) {
            LOGGER.error("CQL error when loading by SELECT.", e);
        }
        return null;
    }

    /**
     * Insert rating from model to cassandra DB
     * @param rating
     * @return Boolean, return true if inserted sucessfuly
     */
    public static Boolean insert(Rating rating) {
        LOGGER.info("Inserting to Cassandra new Rating");
        try {
            PreparedStatement prepared = session.prepare(CQL_INSERT);
            BoundStatement bound = prepared.bind(rating.getPost_id(),rating.getValue(),rating.getUser_email());
            ResultSet result = session.execute(bound);
            LOGGER.info("Inserting new Cassandra Rating success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when INSERT.", e);
            return false;
        }
    }

    /**
     * Delete rating by postID and email from cassandra DB
     * @param postID,email
     * @return Boolean, true if operation was aplied
     */
    public static Boolean delete(UUID postID, String email) {
        LOGGER.info("Deleting rating with POST_ID {} , EMAIL {} from Cassandra Rating",postID,email);
        try {
            PreparedStatement prepared = session.prepare(CQL_DELETE);
            BoundStatement bound = prepared.bind(postID,email);
            ResultSet result = session.execute(bound);
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when DELETE.", e);
            return false;
        }
    }

    /**
     * Update rating from model to cassandra DB
     * @param rating
     * @return Boolean, return true if updated sucessfuly
     */
    public static Boolean update(Rating rating) {
        LOGGER.info("Update rating in Cassandra ");
        try {
            PreparedStatement prepared = session.prepare(CQL_UPDATE);
            BoundStatement bound = prepared.bind(rating.getValue(),rating.getPost_id(),
                rating.getUser_email());
            ResultSet result = session.execute(bound);
            LOGGER.info("Update Cassandra rating success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when UPDATE.", e);
            return false;
        }
    }

    /**
     * Update rating from model to cassandra DB
     * @param postID
     * @return Double, return average rating of selected post
     */
    public static Double getRatingOfPost(UUID postID) {
        LOGGER.info("Get average rating of selected post ID = {} in Cassandra ",postID);
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT_AVG_RATING);
            BoundStatement bound = prepared.bind(postID);
            Double result = session.execute(bound).one().getDouble(0);
            LOGGER.info("Returned average rating is {}", result);
            return result;
        } catch( Exception e) {
            LOGGER.error("CQL error when SELECT AVERAGE.", e);
            return 0.0;
        }
    }

         
    /**
     * get number of ratings of selected post
     * @param userEmail,time user email
     * @return Integer
     */
    public static Integer getRatingCount(UUID postID) {
        LOGGER.info("Get NUMBER OF Ratings of Post",postID);
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT_COUNT_RATING);
            BoundStatement bound = prepared.bind(postID);
            Integer result = session.execute(bound).one().getInt(0);
            LOGGER.info("Returned number of ratings is {}", result);
            return result;
        } catch( Exception e) {
            LOGGER.error("CQL error when SELECT COUNT rating.", e);
            return null;
        }
    }

}
