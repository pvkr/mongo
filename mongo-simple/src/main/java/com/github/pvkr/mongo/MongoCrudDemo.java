package com.github.pvkr.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.Arrays;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Updates.inc;

@Slf4j
public class MongoCrudDemo {

    public static void main(String[] args) {
        MongoClient client = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase db = client.getDatabase("local");

        MongoCollection<Document> prevUsers = db.getCollection("users");
        if (prevUsers != null) {
            log.warn("Found collection users, drop it");
            prevUsers.drop();
        }

        log.info("Create collection users");
        db.createCollection("users");

        log.info("Get collection");
        MongoCollection<Document> users = db.getCollection("users");

        log.info("Create documents");
        users.insertOne(new Document("name", "John").append("fullname", "John Doe").append("credits", 5));
        users.insertMany(Arrays.asList(
                new Document("name", "Arnold").append("fullname", "Arnold Schwarzenegger").append("credits", 100),
                new Document("name", "Sylvester").append("fullname", "Sylvester Stallone").append("credits", 50)
        ));

        log.info("Read all documents");
        users.find().map(Document::toJson).forEach((Consumer<? super String>) log::info);
        log.info("Read some documents");
        users.find(gt("credits", 50)).map(Document::toJson).forEach((Consumer<? super String>) log::info);

        log.info("Update single document");
        users.updateOne(eq("name", "John"), new Document("$set", new Document("fullname", "John Conor")));
        users.find(eq("name", "John")).map(Document::toJson).forEach((Consumer<? super String>) log::info);

        log.info("Update many document");
        users.updateMany(gt("credits", 10), inc("credits", 10));
        users.find(gt("credits", 10)).map(Document::toJson).forEach((Consumer<? super String>) log::info);

        log.info("Delete document");
        users.deleteMany(gt("credits", 10));
        users.find().map(Document::toJson).forEach((Consumer<? super String>) log::info);
    }
}
