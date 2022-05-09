package dbs.oracle_project_template.models;

import java.util.UUID;

public class Rating {
    private UUID post_id;
    private Double value;
    private String user_email;

    public Rating(UUID post_id, Double value, String user_email) {
        this.post_id = post_id;
        this.value = value;
        if(value > 10)
            this.value = 10.0;
        if(value < 0)
            this.value = 0.0;
        this.user_email = user_email;
    }

    public UUID getPost_id() {
        return post_id;
    }

    public void setPost_id(UUID post_id) {
        this.post_id = post_id;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public String getUser_email() {
        return user_email;
    }

    public void setUser_email(String user_email) {
        this.user_email = user_email;
    }

    @Override
    public String toString() {
        return "post_id: " + this.post_id + ", value: " + this.value + ", user_email: " + this.user_email;
    }    
}
