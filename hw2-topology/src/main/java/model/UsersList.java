package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.List;

 /*
 Класс для сохранения списков заблокированных пользователей в хранилище
  */

@ToString
public class UsersList {

    @JsonProperty
    private List<String> users;

    public UsersList() {
    }

    public List<String> getUsers() {
        return users;
    }

    public void setUsers(List<String> users) {
        this.users = users;
    }
}
