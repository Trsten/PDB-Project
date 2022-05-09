package dbs.oracle_project_template;

import java.sql.SQLException;
import java.time.Instant;

import com.datastax.oss.driver.api.core.uuid.Uuids;

import dbs.oracle_project_template.domain.CassComment;
import dbs.oracle_project_template.domain.CassPost;
import dbs.oracle_project_template.domain.CassRating;
import dbs.oracle_project_template.domain.CassUser;
import dbs.oracle_project_template.domain.OracleUser;
import dbs.oracle_project_template.models.Comment;
import dbs.oracle_project_template.models.Post;
import dbs.oracle_project_template.models.Rating;
import dbs.oracle_project_template.models.User;

public class WriteModel {
    private DbConnections dbConnections;

    public WriteModel(DbConnections dbConnections)
    {
        this.dbConnections = dbConnections;
        OracleUser.setConnection(dbConnections.getOracleConnection());
        CassUser.setConnection(dbConnections.getCassandraConnection());
        CassPost.setConnection(dbConnections.getCassandraConnection());
        CassComment.setConnection(dbConnections.getCassandraConnection());
        CassRating.setConnection(dbConnections.getCassandraConnection());
    }

    /*********************************
     * Users
     *********************************/

    public void createUser(User newUser){
        try{
            OracleUser.insert(newUser);
            User insertedUser = OracleUser.load(newUser.getEmail());
            newUser.setUser_id(insertedUser.getUser_id());
            CassUser.insert(newUser);
        }catch(SQLException e){
            System.err.println(e);
        }
    }

    public void editUser(User authenticatedUser, String first_name, String last_name, String email, String password, String address, String phone){
        authenticatedUser.setFirst_name(first_name);
        authenticatedUser.setLast_name(last_name);
        authenticatedUser.setEmail(email);
        authenticatedUser.setPassword(password);
        authenticatedUser.setAddress(address);
        authenticatedUser.setPhone(phone);
        try {
            OracleUser.update(authenticatedUser);
            CassUser.update(authenticatedUser);            
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    public void deleteUser(User authenticatedUser){
        OracleUser.delete(authenticatedUser.getUser_id());
        CassUser.delete(authenticatedUser.getEmail());
    }

    public void makeFollow(User authenticatedUser, User followed){
        try {
            OracleUser.insertFollower(authenticatedUser.getUser_id(), followed.getUser_id());
            CassUser.insertFollower(authenticatedUser.getEmail(), followed.getEmail());
        } catch (SQLException e) {
            System.err.println(e);
        }
    }

    public void deleteFollow(User authenticatedUser, User followed){
        OracleUser.deleteFollower(authenticatedUser.getUser_id(), followed.getUser_id());
        CassUser.deleteFollower(authenticatedUser.getEmail(), followed.getEmail());
    }

    /*********************************
     * Posts
     *********************************/
    
    public void createPost(User authenticatedUser, String title, String content){
        Post newPost = new Post(authenticatedUser.getEmail(), Uuids.timeBased(), title, content, 0.0, 0, 0);
        CassPost.insert(newPost);
    }

    public void editPost(User authenticatedUser, Post post, String title, String content){
        if(authenticatedUser.getEmail() != post.getUser_email())
            return;
        post.setTitle(title);
        post.setContent(content);
        CassPost.update(post);
    }

    public void deletePost(Post post){
        CassPost.delete(post.getUser_email(), post.getTime());
    }

    /*********************************
     * comments
     *********************************/

    public void createComment(User authenticatedUser, Post post, String content){
        Comment comment = new Comment(post.getTime(), Instant.now(), authenticatedUser.getEmail(), content);
        CassComment.insert(comment);
        Integer commentsCount = CassPost.getNumberOfComments(post.getUser_email(), post.getTime());
        post.setComment_count(commentsCount);
        CassPost.update(post);
    }

    public void editComment(User authenticatedUser, Comment comment, String content){
        if(authenticatedUser.getEmail() != comment.getUser_email())
            return;
        comment.setContent(content);
        CassComment.update(comment);
    }

    public void deleteComment(User user, Post post, Comment comment){
        // if(post.getTime() != comment.getPost_id())
        //     return;
        CassComment.delete(comment.getPost_id(), user.getEmail(), comment.getTime());
        Integer commentsCount = CassPost.getNumberOfComments(post.getUser_email(), post.getTime());
        post.setComment_count(commentsCount);
        CassPost.update(post);
    }

    /*********************************
     * ratings
     *********************************/

    public void createRating(User user, Post post, int value){
        Rating rating = new Rating(post.getTime(), (double)value, user.getEmail());
        CassRating.insert(rating);
        updatePostRating(post);
    }
    
    public void editRating(User authenticatedUser, Rating rating, Double value){
        if(authenticatedUser.getEmail() != rating.getUser_email())
            return;
        rating.setValue(value);
        CassRating.update(rating);
    }

    public void deleteRating(Rating rating, Post post){
        CassRating.delete(rating.getPost_id(), rating.getUser_email());
        updatePostRating(post);
    }


    private void updatePostRating(Post post) {
        Post updatedPost = CassPost.load(post.getUser_email(), post.getTime());
        Double ratingValue = CassRating.getRatingOfPost(updatedPost.getTime());
        Integer ratingCount = CassRating.getRatingCount(updatedPost.getTime());
        updatedPost.setRating(ratingValue);
        updatedPost.setRating_count(ratingCount);
        CassPost.update(updatedPost);
        CassPost.InsertTopRank(updatedPost.getUser_email(), updatedPost.getTime(), updatedPost.getRating());
    }
}
