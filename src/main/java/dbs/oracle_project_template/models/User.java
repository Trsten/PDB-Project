package dbs.oracle_project_template.models;

public class User {
    
    private Integer user_id;
    private String first_name;
    private String last_name;
    private String email;
    private String password;
    private String address;
    private String phone;

    public User() {
        
    }

    public User(Integer user_id,String first_name, String last_name, String email, String password, String address, String phone) {
        this.user_id = user_id;
        this.first_name = first_name;
        this.last_name = last_name;
        this.email = email;
        this.password = password;
        this.address = address;
        this.phone = phone;
    }

    public Integer getUser_id() {
        return user_id;
    }

    public void setUser_id(Integer user_id) {
        this.user_id = user_id;
    }

    public String getFirst_name() {
        return first_name;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;
    }

    public String getLast_name() {
        return last_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    @Override
    public String toString() {
        return "user_id: " + this.user_id + ", first_name: " + this.first_name + ", last_name: " + this.last_name
            + ", email: " + this.email + ", password: " + this.password + ", address: " + this.address
            + ", phone: " + this.phone;
    }
}