package com.byterun.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserModel {
    private String name;
    private int age;
    private String desc;
    private String level;
    private String experience;
    private String entry_time;

}
