package demo.main;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.UUID;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.cloud.Date;
import com.google.cloud.spanner.*;

import lombok.Data;

@Data
public class Ship implements Serializable {
    private String name;
    private String type;
    private LocalDate birthday;

    public static Ship parse(String[] input) {
        Ship ship = new Ship();
        if (input.length != 3)
            throw new IllegalArgumentException(String.format("Line length not 3 (%d)", input.length));
        ship.name = input[0];
        ship.type = input[1];
        ship.birthday = LocalDate.parse(input[2]);
        return ship;
    }

    public static DoFn<String, Ship> parse() {
        return new DoFn<String, Ship>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                String[] input = context.element().split(",");
                Ship ship = Ship.parse(input);
                context.output(ship);
            }
        };
    }

    public static DoFn<Ship, Mutation> toMutation(String table) {
        return new DoFn<Ship, Mutation>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                Ship ship = context.element();
                Mutation mutation = Mutation
                        .newInsertOrUpdateBuilder(table)
                        .set("id").to(UUID.randomUUID().toString())
                        .set("name").to(ship.name)
                        .set("type").to(ship.type)
                        .set("birthday").to(Date.parseDate(ship.birthday.toString()))
                        .build();
                context.output(mutation);
            }
        };
    }

    public static Ship of(ResultSet result) {
        Ship ship = new Ship();
        ship.name = result.getString("name");
        ship.type = result.getString("type");
        ship.birthday = LocalDate.parse(result.getDate("birthday").toString());
        return ship;
    }
}
