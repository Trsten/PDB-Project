package dbs.oracle_project_template.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.internal.core.type.codec.BigIntCodec;

import dbs.oracle_project_template.models.*;

public class CassPost {
    

    private static final transient Logger LOGGER = LoggerFactory.getLogger(OracleUser.class);
    private static CqlSession session;
    
    public static void setConnection(CqlSession defaultSession) {
        session = defaultSession;
    }

    //templates for CRUD operations for Post
    public static final transient String CQL_SELECT_ALL = "SELECT * FROM post"; 
    public static final transient String CQL_SELECT = CQL_SELECT_ALL + " WHERE user_email = ? ORDER BY time";
    public static final transient String CQL_SELECT_POPULAR = "SELECT" 
        + " user_email, time, title, content, rating, comment_count, rating_count, max(comment_count)"
        + " FROM post WHERE user_email = ?";
        public static final transient String CQL_SELECT_COUNT = "SELECT" 
        + " CAST (COUNT(*) AS int) FROM post WHERE user_email = ? AND time = ?";
    public static final transient String CQL_SELECT_ONE = CQL_SELECT_ALL + " WHERE user_email = ? AND time = ? ORDER BY time";
    public static final transient String CQL_INSERT = "INSERT INTO post"
        + " (user_email, time, title, content, rating, comment_count, rating_count)" 
        + " VALUES (?, ?, ?, ?, ?, ?, ?)";
    public static final transient String CQL_UPDATE = "UPDATE post"
        +" SET title = ?, content = ?, rating = ?, comment_count = ?, rating_count = ?"
        +" WHERE user_email = ? AND time = ?";
    public static final transient String CQL_DELETE = "DELETE FROM post WHERE user_email = ? AND time = ?";

    public static final transient String CQL_INSERT_TOP_RATING = "INSERT INTO top_rating (post_id, rating, user_email)"
        +" VALUES ( ?, ?, ? ) USING TTL 86400";
        public static final transient String CQL_SLECT_TOP_RATING = "SELECT * FROM top_rating LIMIT 1";
    public static final transient String CQL_SELECT_POPULAR_RATING = "SELECT" 
    + " user_email, time, title, content, rating, comment_count, rating_count, max(rating_count)"
    + " FROM post WHERE user_email = ?";

    //defined columns of post
    public static final transient String COLUMN_EMAIL = "user_email";
    public static final transient String COLUMN_TIME = "time";
    public static final transient String COLUMN_TITLE = "title";
    public static final transient String COLUMN_CONTENT = "content";
    public static final transient String COLUMN_RATING = "rating";
    public static final transient String COLUMN_COMMENT_COUNT = "comment_count";
    public static final transient String COLUMN_RATING_COUNT = "rating_count"; 

    //defined columns of top_rating
    public static final transient String COLUMN_POST_ID = "post_id";

    /**
     * Load cassandra post of selected user email
     * @param userEmail user email
     * @return List<Post>
     */
    public static List<Post> load(String userEmail) {
        LOGGER.info("Loading Cassandra Post with EMAIL {}",userEmail);
        List<Post> posts = new ArrayList<>();
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT);
            BoundStatement bound = prepared.bind(userEmail);
            List<Row> result = session.execute(bound).all();
            for (Row row : result) {
                Post post = new Post(row.getString(COLUMN_EMAIL),row.getUuid(COLUMN_TIME),
                    row.getString(COLUMN_TITLE),row.getString(COLUMN_CONTENT),row.getDouble(COLUMN_RATING),
                    row.getInt(COLUMN_COMMENT_COUNT),row.getInt(COLUMN_RATING_COUNT));
                posts.add(post);
                LOGGER.info("Loaded Cassandra Post {}", post);
            }        
        } catch( Exception e) {
            LOGGER.error("CQL error when loading by SELECT.", e);
        }
        return posts;
    }

    /**
     * Load cassandra post of selected user email and his followers
     * @param userEmail user email
     * @return List<Post>
     */
    public static List<Post> loadFollowersPosts(String userEmail) {
        LOGGER.info("Loading Cassandra Post with EMAIL {} and his followers",userEmail);
        List<Post> posts = new ArrayList<Post>();
        Set<String> followers = CassUser.loadFollowersEmail(userEmail);
        String listFollowers = "";
        if ( !followers.isEmpty() ) {
            for (String followerEmail : followers) {
                listFollowers += "\'"+ followerEmail +"\', ";
            }
            listFollowers = listFollowers.substring(0, listFollowers.length() - 2);
        }
        try {
            List<Row> result = session.execute(CQL_SELECT_ALL + " WHERE user_email IN ( " + listFollowers + " )").all();
            for (Row row : result) {
                Post post = new Post(row.getString(COLUMN_EMAIL),row.getUuid(COLUMN_TIME),
                    row.getString(COLUMN_TITLE),row.getString(COLUMN_CONTENT),row.getDouble(COLUMN_RATING),
                    row.getInt(COLUMN_COMMENT_COUNT),row.getInt(COLUMN_RATING_COUNT));
                posts.add(post);
                LOGGER.info("Loaded Cassandra Post {}", post);
            }        
        } catch( Exception e) {
            LOGGER.error("CQL error when loading by SELECT.", e);
        }
        return posts;
    }

    /**
     * Load cassandra post of selected user email and his followers
     * @param userEmail,time user email
     * @return Post
     */
    public static Post load(String userEmail,UUID time) {
        LOGGER.info("Loading Cassandra Post with EMAIL {} AND TIME {} and his followers",userEmail,time);
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT_ONE);
            BoundStatement bound = prepared.bind(userEmail,time);
            Row result = session.execute(bound).one();
            if ( result != null ) {
                Post post = new Post(result.getString(COLUMN_EMAIL),result.getUuid(COLUMN_TIME),
                    result.getString(COLUMN_TITLE),result.getString(COLUMN_CONTENT),result.getDouble(COLUMN_RATING),
                    result.getInt(COLUMN_COMMENT_COUNT),result.getInt(COLUMN_RATING_COUNT));
                LOGGER.info("Loaded Cassandra Post {}", post);
                return post;
            } else {
                LOGGER.warn("Cassandra Post with EMAIL {} AND TIME {} does not exists so it cannot be loaded", userEmail,time);
                return null;
            }
                        
        } catch( Exception e) {
            LOGGER.error("CQL error when loading by SELECT.", e);
        }
        return null;
    }

     /**
     * Insert post from model to cassandra DB
     * @param post
     * @return Boolean, return true if inserted sucessfuly
     */
    public static Boolean insert(Post post) {
        LOGGER.info("Inserting to Cassandra new Post");
        try {
            PreparedStatement prepared = session.prepare(CQL_INSERT);
            BoundStatement bound = prepared.bind(post.getUser_email(),post.getTime(),post.getTitle(),
                post.getContent(),post.getRating(),post.getComment_count(),post.getRating_count());
            ResultSet result = session.execute(bound);
            LOGGER.info("Inserting new Cassandra Post success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when INSERT.", e);
            return false;
        }
    }

    /**
     * Delete post by user_email from cassandra DB
     * @param email,time
     * @return Boolean, true if operation was aplied
     */
    public static Boolean delete(String email, UUID time) {
        LOGGER.info("Deleting post with EMAIL {} AND TIME {} from Cassandra Post",email,time);
        try {
            PreparedStatement prepared = session.prepare(CQL_DELETE);
            BoundStatement bound = prepared.bind(email,time);
            ResultSet result = session.execute(bound);
            LOGGER.info("Delete Cassandra User success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when DELETE.", e);
            return false;
        }
    }

    /**
     * Update post from model to cassandra DB
     * @param post
     * @return Boolean, return true if updated sucessfuly
     */
    public static Boolean update(Post post) {
        LOGGER.info("Update post in Cassandra");
        try {
            PreparedStatement prepared = session.prepare(CQL_UPDATE);
            BoundStatement bound = prepared.bind(post.getTitle(),post.getContent(),
                post.getRating(),post.getComment_count(),post.getRating_count(),
                post.getUser_email(),post.getTime());
            ResultSet result = session.execute(bound);
            LOGGER.info("Update Cassandra Post success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when UPDATE.", e);
            return false;
        }
    }

     /**
     * Load cassandra post of selected user email and his followers
     * @param userEmail,time user email
     * @return Post
     */
    public static Post loadPopular(String userEmail) {
        LOGGER.info("Loading Cassandra Post with EMAIL {} and his followers",userEmail);
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT_POPULAR);
            BoundStatement bound = prepared.bind(userEmail);
            Row result = session.execute(bound).one();
            if ( result != null ) {
                Post post = new Post(result.getString(COLUMN_EMAIL),result.getUuid(COLUMN_TIME),
                    result.getString(COLUMN_TITLE),result.getString(COLUMN_CONTENT),result.getDouble(COLUMN_RATING),
                    result.getInt(COLUMN_COMMENT_COUNT),result.getInt(COLUMN_RATING_COUNT));
                LOGGER.info("Loaded Cassandra popular Post {}", post);
                return post;
            } else {
                LOGGER.warn("Cassandra popular Post with EMAIL {} does not exists so it cannot be loaded", userEmail);
                return null;
            }
        } catch( Exception e) {
            LOGGER.error("CQL error when loading by SELECT.", e);
        }
        return null;
    }

     /**
     * bet number of commnets of selected post
     * @param userEmail,time user email
     * @return Integer
     */
    public static Integer getNumberOfComments(String userEmail,UUID time) {
        LOGGER.info("Loading NUMBER OF with EMAIL {} AND TIME {} and his followers",userEmail,time);
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT_COUNT);
            BoundStatement bound = prepared.bind(userEmail,time);
            Integer result = session.execute(bound).one().getInt(0);
            LOGGER.info("Returned number of commnets is {}", result);
            return result;
        } catch( Exception e) {
            LOGGER.error("CQL error when SELECT COUNT.", e);
            return null;
        }
    }

    /**
     * bet number of commnets of selected post
     * @param userEmail,time user email
     * @return Integer
     */
    public static Post getTopRankedPost() {
        LOGGER.info("Loading Top Ranked post");
        try {
            Row result = session.execute(CQL_SLECT_TOP_RATING).one();
            if ( result != null ) {
                Post post = CassPost.load(result.getString(COLUMN_EMAIL), result.getUuid(COLUMN_POST_ID));
                LOGGER.info("Top ranked post {}", post.toString());
                return post;
            } else {
                LOGGER.error("CQL error when SELECT TOP RANKING no record found.");
                return null;
            }
        } catch( Exception e) {
            LOGGER.error("CQL error when SELECT TOP RANKING.", e);
            return null;
        }
    }

    /**
     * Insert top rank
     * @param email,postID,rating user email
     * @return Boolean
     */
    public static Boolean InsertTopRank(String email, UUID postID,Double rating) {
        LOGGER.info("Inserting Top Rank");
        try {
            PreparedStatement prepared = session.prepare(CQL_INSERT_TOP_RATING);
            BoundStatement bound = prepared.bind(postID,rating,email);
            ResultSet result = session.execute(bound);
            LOGGER.info("Inserting new Cassandra Post success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when INSERT TOP RATING.", e);
            return false;
        }
    }


    /**
     * Load cassandra post of selected user email and his followers
     * @param userEmail user email
     * @return Post
     */
    public static Post loadPopularByRank(String userEmail) {
        LOGGER.info("Loading Cassandra Post with EMAIL {} and his followers",userEmail);
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT_POPULAR_RATING);
            BoundStatement bound = prepared.bind(userEmail);
            Row result = session.execute(bound).one();
            if ( result != null ) {
                Post post = new Post(result.getString(COLUMN_EMAIL),result.getUuid(COLUMN_TIME),
                    result.getString(COLUMN_TITLE),result.getString(COLUMN_CONTENT),result.getDouble(COLUMN_RATING),
                    result.getInt(COLUMN_COMMENT_COUNT),result.getInt(COLUMN_RATING_COUNT));
                LOGGER.info("Loaded Cassandra popular Post {}", post);
                return post;
            } else {
                LOGGER.warn("Cassandra popular Post with EMAIL {} does not exists so it cannot be loaded", userEmail);
                return null;
            }
        } catch( Exception e) {
            LOGGER.error("CQL error when loading by SELECT.", e);
        }
        return null;
    }

}