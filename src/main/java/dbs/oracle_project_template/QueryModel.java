package dbs.oracle_project_template;

import java.util.List;

import dbs.oracle_project_template.domain.CassComment;
import dbs.oracle_project_template.domain.CassPost;
import dbs.oracle_project_template.domain.CassRating;
import dbs.oracle_project_template.domain.CassUser;
import dbs.oracle_project_template.models.Comment;
import dbs.oracle_project_template.models.Post;
import dbs.oracle_project_template.models.Rating;
import dbs.oracle_project_template.models.User;

public class QueryModel {
    private DbConnections dbConnections;

    public QueryModel(DbConnections dbConnections)
    {
        this.dbConnections = dbConnections;
        CassUser.setConnection(dbConnections.getCassandraConnection());
        CassComment.setConnection(dbConnections.getCassandraConnection());
        CassRating.setConnection(dbConnections.getCassandraConnection());
    }

    /**
     * vrátit uživatele
     */
    public User findUser(String email){
        return CassUser.load(email);
    }

    /**
     * vratit komentáře příspěvek
     */
    public List<Comment> getComments(Post post){
        return CassComment.load(post.getTime());
    }

    /**
     * vratit hodnoceni
     */
    public Rating getRating(User user, Post post){
        return CassRating.load(post.getTime(), user.getEmail());
    }

    /**
     * vrátit seznam sledovaných uživatelů
     */
    public List<User> follow(User user){
        return CassUser.loadFollowers(user.getEmail());
    }

    /**
     * vrátit příspěvky přátel
     */
    public List<Post> followsPosts(User user){
        return CassPost.loadFollowersPosts(user.getEmail());
    }

    /**
     * vrátit mé příspěvky
     */
    public List<Post> userPosts(User user){
        return CassPost.load(user.getEmail());
    }

    /**
     * vrátit příspěvky podle nejlepšího ohodnocení
     */
    public Post bestRatedPost(){
        return CassPost.getTopRankedPost();
    }

    /**
     * vrátit příspěvek podle popularity (kolik obsahuje ohodnocení)
     */
    public Post popularPost(User user){
        return CassPost.loadPopularByRank(user.getEmail());
    }

    /**
     * vrátit příspěvky podle množství komentářů
     */
    public Post mostCommentedPosts(User user){
        return CassPost.loadPopular(user.getEmail());
    }
}
