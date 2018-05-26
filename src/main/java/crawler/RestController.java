package crawler;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.*;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@EnableAutoConfiguration(exclude = { MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@ComponentScan
public class RestController {

    @Autowired
    private
    DomainListManager dm;

    @RequestMapping("/get/{count}")
    @ResponseBody
    HashMap<String, Object> getBatch(@PathVariable Integer count) {
        HashMap<String, Object> map = new HashMap<>();
        map.put("domains", dm.getDomains(count));
        return map;
    }

    @RequestMapping(
            value = "/put",
            method = RequestMethod.POST,
            consumes="application/json")
    @ResponseBody
    HashMap<String, Object> putBatch(@RequestBody Map<String, List<String>> domains) {
        dm.putDomains(Sets.newHashSet(domains.get("domains")));
        return Maps.newHashMap();
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(RestController.class, args);
    }
}