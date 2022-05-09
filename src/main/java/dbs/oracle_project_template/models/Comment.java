package dbs.oracle_project_template.models;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.UUID;

public class Comment {
    private UUID post_id;
    private Instant time;
    public String user_email;
    public String content;

    public Comment(UUID post_id, Instant time, String user_email, String content) {
        this.post_id = post_id;
        this.time = time;
        this.user_email = user_email;
        this.content = content;
    }

    public UUID getPost_id() {
        return post_id;
    }

    public void setPost_id(UUID post_id) {
        this.post_id = post_id;
    }

    public Instant getTime() {
        return time;
    }

    public void setTime(Instant time) {
        this.time = time;
    }

    public String getUser_email() {
        return user_email;
    }

    public void setUser_email(String user_email) {
        this.user_email = user_email;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "post_id: " + this.post_id + ", time: " + this.time + ", user_email: " + this.user_email
            + ", content: " + this.content;
    }

}
