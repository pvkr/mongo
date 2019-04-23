package com.github.pvkr.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.Arrays;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;

@Slf4j
public class MongoPojoDemo {

    public static void main(String[] args) {
        log.info("Create code registry");
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build())
        );

        log.info("Create client");
        MongoClient client = MongoClients.create(
                MongoClientSettings.builder()
                        .codecRegistry(codecRegistry)
                        .applyConnectionString(new ConnectionString("mongodb://localhost:27017"))
                        .build()
        );
        MongoDatabase db = client.getDatabase("local");

        log.info("Create collection 'persons'");
        MongoCollection<Document> prevUsers = db.getCollection("persons");
        if (prevUsers != null) {
            prevUsers.drop();
        }
        db.createCollection("persons");

        MongoCollection<Person> persons = db.getCollection("persons", Person.class);

        log.info("Insert some data");
        persons.insertOne(new Person("Ada", "Byron", 20, new Address("St James Square", "London", "W1")));
        persons.insertMany(Arrays.asList(
                new Person("Arnold", "Schwarzenegger", 71, new Address("3110 Main Street", "Santa Monica", "CA 90406")),
                new Person("Sylvester", "Stallone", 72, new Address("30 Beverly Park Terrace", "Beverly Hills", "CA 90210"))
        ));

        log.info("Read all persons");
        persons.find().map(Person::toString).forEach((Consumer<? super String>) log::info);
        log.info("Read some persons");
        persons.find(gt("age", 70)).map(Person::toString).forEach((Consumer<? super String>) log::info);

        log.info("Update single person");
        persons.updateOne(eq("firstName", "Ada"), combine(set("address.zip", "W2"), inc("age", 1)));
        persons.find(eq("firstName", "Ada")).map(Person::toString).forEach((Consumer<? super String>) log::info);

        log.info("Delete");
        DeleteResult deleteResult = persons.deleteMany(eq("address.city", "London"));
        log.info("count {}", deleteResult.getDeletedCount());
        log.info("acknowledged {}", deleteResult.wasAcknowledged());
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    @ToString(of = {"firstName", "secondName", "age", "address"})
    public static class Person {
        private String firstName;
        private String secondName;
        private int age;
        private Address address;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    @ToString(of = {"city", "zip"})
    public static class Address {
        private String street;
        private String city;
        private String zip;
    }
}
