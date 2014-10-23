import io.vertx.ceylon.core {
  Vertx
}
import ceylon.promise {
  Promise
}
import ceylon.json {
  JsonObject=Object,
  JsonArray=Array,
  Value
}
import io.vertx.ceylon.platform {
  Platform
}
import io.vertx.ceylon.core.eventbus {
  Message
}

Promise<AsyncDbc> deployAsyncDbc(Platform plf) {
  return plf.deploy(dbModuleId, JsonObject {
    // Config here
    // "url" -> "jdbc:hsqldb:file:test.db"
  }, 1).compose((String id) => AsyncDbc(plf.vertx));
}

class AsyncDbc(Vertx vertx) {
  
  shared Promise<AsyncDbc> execute(String statement) {
    print("executing");
    value action = JsonObject {
      "action"-> "execute",
      "stmt"->  statement
    };
    value message = vertx.eventBus.send<JsonObject>("com.bloidonia.jdbcpersistor", action);
    value that = this;
    return message.compose {
      AsyncDbc onFulfilled(Message<JsonObject> result) {
        assert(exists status = result.body["status"]);
        if (status == "ok") {
          return that;
        } else {
          throw Exception("Could not create table");
        }
      }
    };
  }
  
  shared Promise<Data[][]> select(String statement) {
    value action = JsonObject {
      "action"-> "select",
      "stmt"-> statement
    };
    value message = vertx.eventBus.send<JsonObject>("com.bloidonia.jdbcpersistor", action);
    return message.compose {
      Data[][] onFulfilled(Message<JsonObject> msg) {
        value result = msg.body["result"];
        print(result);
        assert(is JsonArray rows = result);
        Data cast(Value o) {
          assert(is Data o);
          return o;
        }
        Data[] bb(Value row) {
          assert(is JsonObject row); 
          return [ for (a in row) cast(a.item)  ];
        }
        return [ for (row in rows) bb(row) ];
      }
    };
  }

  shared Promise<AsyncDbc> insert(String statement, Data[][] rows) {
    value action = JsonObject {
      "action"-> "insert",
      "stmt"->  statement,
      "values"-> JsonArray { for (row in rows) JsonArray(row) }
    };
    value message = vertx.eventBus.send<JsonObject>("com.bloidonia.jdbcpersistor", action);
    value that = this;
    return message.compose {
      AsyncDbc onFulfilled(Message<JsonObject> result) {
        assert(exists status = result.body["status"]);
        if (status == "ok") {
          return that;
        } else {
          assert(exists msg = result.body["message"]);
          throw Exception("Could not insert in table ``msg``");
        }
      }
    };
  }
  
  
}