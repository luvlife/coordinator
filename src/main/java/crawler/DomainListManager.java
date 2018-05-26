package crawler;

import com.google.common.collect.Sets;
import com.mongodb.*;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


@Component
public class DomainListManager {
//    public static void main(String[] args) {
//        DomainListManager dm = new DomainListManager();
//        dm.putDomains(Sets.newHashSet("autoconsulting.com.ua, lea-shans.kharkov.ua, soft.i.ua, easycom.com.ua, all4nod.ru, umj.com.ua, links.i.ua, womenbox.net, zdorov-info.com.ua, autocentre.ua, afp.com.ua, gorod.dp.ua, board.i.ua, shop.i.ua, news.i.ua, files.i.ua, interdalnoboy.com, narod.i.ua, 4pda.info, medinfo.com.ua, apteka.ua, photo.i.ua, radio.i.ua, maxmebel.com.ua, ityre.com, health-ua.org, forum.avtoindex.com, ru.vitalux.ua, gruz-inform.interdalnoboy.com, old-game.org, 100realty.ua"
//        .split(", ")));
//    }

     public List<String> getDomains(Integer count) {
        return DbService.getNextDomainsBatch(count);
    }

     public void putDomains(Set<String> domains) {
        List<BasicDBObject> mongoDomains = domains.stream()
                .map( this::newDomain).collect(Collectors.toList());
        try {
            DbService.writeToDb(mongoDomains);
        } catch (Exception e) {}
    }

    private BasicDBObject newDomain(String domain) {
        BasicDBObject ob = new BasicDBObject("state", DbService.NEW);
        ob.put("domain", domain);
        return ob;
    }


    private static class DbService {

         static final MongoClient MONGO_CLIENT = new MongoClient("172.17.0.2", 27017);
         static final DB DATABASE = MONGO_CLIENT.getDB("crawl_data");
         static final DBCollection COLLECTION = DATABASE.getCollection("sites");
         private static final int NEW = 0;
         private static final int INPROGESS = 1;

        static void writeToDb(List<BasicDBObject> docs) {
            try {
              COLLECTION.insert(docs, new InsertOptions().continueOnError(true));
            } catch (MongoException e) {
                e.printStackTrace();
            }
        }

         static List<BasicDBObject> getDomains(int state, int count) {
            DBCursor cursor = COLLECTION.find(new BasicDBObject("state", state),
                    new BasicDBObject("domain", true)).limit(count);
            List<BasicDBObject> results = new ArrayList<>();
            while (cursor.hasNext()) {
                results.add ((BasicDBObject) cursor.next());
            }
            return results;
        }


        static List<String> getNextDomainsBatch(int count) {
            List<BasicDBObject> domainObjects = getDomains(NEW, count);
            BasicDBList idList = new BasicDBList();
            idList.addAll(
                    domainObjects.stream().map( d -> d.get("_id")).collect(Collectors.toList())
            );

            BulkWriteOperation bop = COLLECTION.initializeOrderedBulkOperation();
            bop.find(new BasicDBObject("_id", new BasicDBObject("$in", idList)))
                    .update(new BasicDBObject("$set", new BasicDBObject("state", INPROGESS)));
            bop.execute();

            return domainObjects.stream().map(d -> (String) d.get("domain")).collect(Collectors.toList());
        }

    }


}

