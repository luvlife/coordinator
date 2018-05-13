import com.mongodb.*;

import java.util.Arrays;
import java.util.List;

/**
 * Created by vladimir on 13.05.18.
 */
public class DomainListManager {
    public static void main(String[] args) {
            DbService.writeToDb();
    }

    synchronized public List<String> getDomains() {

        return DbService.getDomains();
    }

    synchronized public void putDomains(){

    }


    private static class DbService {

        public static void writeToDb(){
            MongoClient mongoClient = new MongoClient("172.17.0.2", 27017);
            DB database = mongoClient.getDB("crawl_data");

            DBCollection collection = database.getCollection("sites");
            List<BasicDBObject> docs = Arrays.asList(new BasicDBObject("id", "1"),new BasicDBObject("id", "4"));
            collection.insert(docs, new InsertOptions().continueOnError(true));
        }

        public static List<String> getDomains() {
            return null;
        }
    }


}
