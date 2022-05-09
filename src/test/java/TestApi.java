import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import dbs.oracle_project_template.DbAccess;
import dbs.oracle_project_template.QueryModel;
import dbs.oracle_project_template.WriteModel;
import dbs.oracle_project_template.domain.CassPost;
import dbs.oracle_project_template.models.Comment;
import dbs.oracle_project_template.models.Post;
import dbs.oracle_project_template.models.Rating;
import dbs.oracle_project_template.models.User;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

public class TestApi {
   
   static QueryModel qmodel;
   static WriteModel wmodel;
   User user1;
   User user2;
   String userMail1 = "email1";
   String userMail2 = "email2";

   @BeforeClass
   public static void openConnetrions()
   {
      qmodel = DbAccess.queryModel();
      wmodel = DbAccess.writeModel();
   }

   @AfterClass
   public static void closeConnections()
   {
      DbAccess.close();
   }

   @Before
   public void insertUsers()
   {
      User user = makeTestUser1();
      wmodel.createUser(user);
      user1 = qmodel.findUser(userMail1);

      user = makeTestUser2();
      wmodel.createUser(user);
      user2 = qmodel.findUser(userMail2);
   }

   @After
   public void deleteUsers()
   {
      wmodel.deleteUser(user1);
      wmodel.deleteUser(user2);
   }

   @Test
   public void testCreateUserAndDelete() {
      //insert
      String testUserEmail1 = "userEmail";
      String testUserEmail2 = "userEmai2";
      User user = new User(null, "first_name", "last_name", testUserEmail1, "password", "address", "phone");
      wmodel.createUser(user);
      assertNotNull(user.getUser_id());
      user = qmodel.findUser(testUserEmail1);
      assertNotNull(user);
      assertEquals(testUserEmail1, user.getEmail());

      user = new User(null, "first_name", "last_name", testUserEmail2, "password", "address", "phone");
      wmodel.createUser(user);
      assertNotNull(user.getUser_id());
      user = qmodel.findUser(testUserEmail2);
      assertNotNull(user);
      assertEquals(testUserEmail2, user.getEmail());

      //delete
      user = qmodel.findUser(testUserEmail1);
      assertNotNull(user);
      assertNotNull(user.getUser_id());
      wmodel.deleteUser(user);
      user = qmodel.findUser(testUserEmail1);
      assertNull(user);

      user = qmodel.findUser(testUserEmail2);
      assertNotNull(user);
      assertNotNull(user.getUser_id());
      wmodel.deleteUser(user);
      user = qmodel.findUser(testUserEmail2);
      assertNull(user);
   }

   @Test
   public void testFindUserNonExistent() {
      User user = qmodel.findUser("fakeMail");
      assertNull(user);
   }

   /* follow tests */

   @Test
   public void testFollowNoFollows() {
      User user = qmodel.findUser(userMail1);
      List<User> followed = qmodel.follow(user);
      assertEquals(0, followed.size());
   }

   @Test
   public void testfollow() {
      //new follow
      wmodel.makeFollow(user1, user2);
      List<User> followed = qmodel.follow(user1);
      assertEquals(1, followed.size());
      assertEquals(userMail2, followed.get(0).getEmail());

      //delete follow
      wmodel.deleteFollow(user1, user2);
      followed = qmodel.follow(user1);
      assertEquals(0, followed.size());
   }

   @Test
   public void testCreateDeletePost() {
      List<Post> posts = qmodel.userPosts(user1);
      assertEquals(0, posts.size());

      wmodel.createPost(user1, "title", "content");
      posts = qmodel.userPosts(user1);
      assertEquals(1, posts.size());
      assertEquals("title", posts.get(0).getTitle());
      assertEquals("content", posts.get(0).getContent());

      wmodel.deletePost(posts.get(0));
      posts = qmodel.userPosts(user1);
      assertEquals(0, posts.size());
   }

   @Test
   public void testCreateDeleteComment() {
      wmodel.createPost(user1, "title", "content");
      Post post = qmodel.userPosts(user1).get(0);

      wmodel.createComment(user1, post, "comment");
      List<Comment> comments = qmodel.getComments(post);
      assertEquals(1, comments.size());
      assertEquals("comment", comments.get(0).getContent());

      wmodel.deleteComment(user1, post, comments.get(0));
      comments = qmodel.getComments(post);
      assertEquals(0, comments.size());

      wmodel.deletePost(post);
   }

   @Test
   public void testCreateDeleteRating() {
      wmodel.createPost(user1, "title", "content");
      Post post = qmodel.userPosts(user1).get(0);

      wmodel.createRating(user1, post, 5);
      post = qmodel.userPosts(user1).get(0);
      Rating rating = qmodel.getRating(user1, post);

      wmodel.deleteRating(rating, post);
      wmodel.deletePost(post);
      
      assertEquals(Double.valueOf(5.0), rating.getValue());
      assertEquals(Double.valueOf(5.0), post.getRating());
      assertEquals(1, (int)post.getRating_count());
   }

   @Test
   public void testFolowerPosts() {
      wmodel.createPost(user2, "title", "content");
      
      List<Post> posts1 = qmodel.followsPosts(user1);
      wmodel.makeFollow(user1, user2);
      List<Post> posts2 = qmodel.followsPosts(user1);

      wmodel.deletePost(qmodel.userPosts(user2).get(0));

      assertEquals(0, posts1.size());
      assertEquals(1, posts2.size());
   }

   @Test
   public void testTopRated() {
      wmodel.createPost(user1, "title", "content");
      Post post = qmodel.userPosts(user1).get(0);
      wmodel.createRating(user1, post, 5);
      post = qmodel.userPosts(user1).get(0);
      Rating rating = qmodel.getRating(user1, post);

      Post bestPost = qmodel.bestRatedPost();
      
      wmodel.deleteRating(rating, post);
      wmodel.deletePost(post);

      assertNotNull(bestPost);
      assertEquals("title", bestPost.getTitle());
   }

   @Test
   public void testUserMostCommented() {
      wmodel.createPost(user1, "title", "content");
      Post post = qmodel.userPosts(user1).get(0);
      wmodel.createComment(user1, post, "comment");
      List<Comment> comments = qmodel.getComments(post);

      Post commentedPost = qmodel.mostCommentedPosts(user1);

      wmodel.deleteComment(user1, post, comments.get(0));
      wmodel.deletePost(post);

      assertNotNull(commentedPost);
      assertEquals("title", commentedPost.getTitle());
   }

   @Test
   public void testUserPopularPost() {
      wmodel.createPost(user1, "title", "content");
      Post post = qmodel.userPosts(user1).get(0);
      wmodel.createRating(user1, post, 5);
      post = qmodel.userPosts(user1).get(0);
      Rating rating = qmodel.getRating(user1, post);

      Post popular = qmodel.popularPost(user1);

      wmodel.deleteRating(rating, post);
      wmodel.deletePost(post);

      assertNotNull(popular);
      assertEquals("title", popular.getTitle());
   }


   private User makeTestUser1(){
      return new User(null, "first_name", "last_name", userMail1, "password", "address", "phone");
   }

   private User makeTestUser2(){
      return new User(null, "first_name2", "last_name2", userMail2, "password2", "address2", "phone2");
   }
}