package dbs.oracle_project_template.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dbs.oracle_project_template.models.Comment;

public class CassComment {
    
    private static final transient Logger LOGGER = LoggerFactory.getLogger(OracleUser.class);
    private static CqlSession session;

    //defined columns of comments
    public static final transient String COLUMN_POST_ID = "post_id";
    public static final transient String COLUMN_TIME = "time";
    public static final transient String COLUMN_USER_EMAIL = "user_email";
    public static final transient String COLUMN_CONTENT = "content";
        
    //templates for CRUD operations for Post
    public static final transient String CQL_SELECT_ALL = "SELECT * FROM comment"; 
    public static final transient String CQL_SELECT = CQL_SELECT_ALL + " WHERE post_id = ? ORDER BY time";
    public static final transient String CQL_INSERT = "INSERT INTO comment"
        + " (post_id, time, user_email, content)" 
        + " VALUES (?, ?, ?, ?)";
    public static final transient String CQL_UPDATE = "UPDATE comment"
        +" SET content = ? WHERE post_id = ? AND user_email = ? AND time = ?";
    public static final transient String CQL_DELETE = "DELETE FROM comment WHERE post_id = ? AND user_email = ? AND time = ?";

    public static void setConnection(CqlSession defaultSession) {
        session = defaultSession;
    }


    /**
     * Load cassandra comments of selected user email and post id
     * @param postID user email
     * @return List<Comment>
     */
    public static List<Comment> load(UUID postID) {
        LOGGER.info("Loading Cassandra Comment with ID {}",postID);
        
        List<Comment> comments = new ArrayList<>();
        try {
            PreparedStatement prepared = session.prepare(CQL_SELECT);
            BoundStatement bound = prepared.bind(postID);
            List<Row> result = session.execute(bound).all();
            for (Row row : result) {
                Comment comment = new Comment(row.getUuid(COLUMN_POST_ID),
                    (row.getInstant(COLUMN_TIME)),row.getString(COLUMN_USER_EMAIL),row.getString(COLUMN_CONTENT));
                comments.add(comment);
                LOGGER.info("Loaded Cassandra Comment {}", comment);
            }        
        } catch( Exception e) {
            LOGGER.error("CQL error when loading by SELECT.", e);
        }
        return comments;
    }

    /**
     * Insert comment from model to cassandra DB
     * @param comment
     * @return Boolean, return true if inserted sucessfuly
     */
    public static Boolean insert(Comment comment) {
        //TODO: update post.comment_count
        LOGGER.info("Inserting to Cassandra new Comment");
        try {
            PreparedStatement prepared = session.prepare(CQL_INSERT);
            BoundStatement bound = prepared.bind(comment.getPost_id(),comment.getTime(),comment.getUser_email(),
                comment.getContent());
            ResultSet result = session.execute(bound);
            LOGGER.info("Inserting new Cassandra Comment success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when INSERT.", e);
            return false;
        }
    }

    /**
     * Delete comment by user_email from cassandra DB
     * @param postID,email,time 
     * @return Boolean, true if operation was aplied
     */
    public static Boolean delete(UUID postID, String email, Instant time) {
        //TODO: update post.comment_count
        LOGGER.info("Deleting comment with EMAIL {} , post ID {} , time {} from Cassandra Comment",email,postID, time);
        try {
            PreparedStatement prepared = session.prepare(CQL_DELETE);
            BoundStatement bound = prepared.bind(postID,email,time);
            ResultSet result = session.execute(bound);
            LOGGER.info("Delete Cassandra Comment success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when DELETE.", e);
            return false;
        }
    }

    /**
     * Update comment from model to cassandra DB
     * @param comment
     * @return Boolean, return true if updated sucessfuly
     */
    public static Boolean update(Comment comment) {
        LOGGER.info("Update comment in Cassandra ");
        try {
            PreparedStatement prepared = session.prepare(CQL_UPDATE);
            BoundStatement bound = prepared.bind(comment.getContent(),comment.getPost_id(),
                comment.getUser_email(),comment.getTime());
            ResultSet result = session.execute(bound);
            LOGGER.info("Update Cassandra Comment success {}", result.wasApplied());
            return result.wasApplied();
        } catch( Exception e) {
            LOGGER.error("CQL error when UPDATE.", e);
            return false;
        }
    }
}
