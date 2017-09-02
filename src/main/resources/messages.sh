use london

db.createCollection("messages")

db.messages.ensureIndex({"topic" : 1, "msgid" : 1})