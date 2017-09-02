package org.hello.london.db;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.types.Binary;
import org.hello.london.core.Message;

import java.util.ArrayList;
import java.util.List;

public class Messages {

    private MongoCollection<Document> mongo;

    public Messages(MongoClient mongo) {
        MongoDatabase london = mongo.getDatabase("london");
        this.mongo = london.getCollection("messages", Document.class);
    }

    public void append(Message msg) {
        Document notice = new Document("topic", msg.topic).append("msgid", msg.msgId).append("msg", new Binary(msg.payload));
        mongo.insertOne(notice);
    }

    public List<Message> get(OfflineMessagesMeta meta) {
        Document query = new Document("topic", meta.topic).append("msgid", new Document().append("$gt", meta.start).append("$lte", meta.end));
        FindIterable<Document> list = this.mongo.find(query).sort(Sorts.ascending("msgid"));
        List<Message> messages = new ArrayList<>();
        for (Document doc : list) {
            messages.add(new Message(doc.getString("topic"), doc.getLong("msgid"), ((Binary) doc.get("msg")).getData()));
        }
        return messages;
    }
}
