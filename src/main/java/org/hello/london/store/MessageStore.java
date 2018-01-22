package org.hello.london.store;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.types.Binary;
import org.hello.london.core.Message;

import java.util.ArrayList;
import java.util.List;

public class MessageStore {

    private MongoDatabase london;

    public MessageStore(MongoClient mongo) {
        this.london = mongo.getDatabase("london");
    }

    public void append(Message msg) {
        Document doc = new Document("msgid", msg.msgId).append("msg", new Binary(msg.payload));
        london.getCollection(msg.topic, Document.class).insertOne(doc);
    }

    public List<Message> get(MessagesRange range) {
        Document query = new Document("msgid", new Document().append("$gt", range.start).append("$lte", range.end));
        FindIterable<Document> list = london.getCollection(range.topic, Document.class).find(query).sort(Sorts.ascending("msgid"));
        List<Message> messages = new ArrayList<>();
        for (Document doc : list) {
            messages.add(new Message(range.topic, doc.getLong("msgid"), ((Binary) doc.get("msg")).getData()));
        }
        return messages;
    }
}
