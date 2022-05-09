package dbs.oracle_project_template.models;


import java.util.UUID;
import java.sql.Date;
import java.sql.Time;

public class Post {
    private String user_email;
    private UUID time;
    private String title;
    private String content;
	private Double rating;
	private Integer comment_count;
	private Integer rating_count;

    public Post(String user_email, UUID time,String title, String content,Double rating,
        Integer comment_count,Integer rating_count) {

        this.user_email = user_email;
        this.time = time;
        this.title = title;
        this.content = content;
        this.rating = rating;
        this.comment_count = comment_count;
        this.rating_count = rating_count;
    }

    public String getUser_email() {
        return user_email;
    }

    public void setUser_email(String user_email) {
        this.user_email = user_email;
    }

    public UUID getTime() {
        return time;
    }

    public void setTime(UUID time) {
        this.time = time;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }

    public Integer getComment_count() {
        return comment_count;
    }

    public void setComment_count(Integer comment_count) {
        this.comment_count = comment_count;
    }

    public Integer getRating_count() {
        return rating_count;
    }

    public void setRating_count(Integer rating_count) {
        this.rating_count = rating_count;
    }

    @Override
    public String toString() {
        long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
        long time = (this.time.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
        Time time2 = new Time(time);
        Date date = new Date(time);

        return "user_email: " + this.user_email + ", time: " + date.toLocalDate() +":"+ time2 + ", title: " + this.title
            + ", content: " + this.content + ", rating: " + this.rating + ", comment_count: " + this.comment_count
            + ", rating_count: " + this.rating_count;
    }

}
