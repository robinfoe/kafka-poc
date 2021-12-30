package com.rfoe.sample.kafka.scalar;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public class Staff {

    @Getter @Setter
    private String name;

    @Getter @Setter
    private String email;
    

    public Staff(String name, String email){
        this.name = name;
        this.email = email;
    }

}
