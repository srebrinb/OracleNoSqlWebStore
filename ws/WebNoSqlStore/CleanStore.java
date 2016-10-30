package WebNoSqlStore;


import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author sbalabanov
 */
public class CleanStore extends Thread{
    private final Store store;
    private KVStore kvstore;
    CleanStore(Store pStore){
        store=pStore;
        kvstore=store.getKVStore();
    }
    public int clean() {
        long curTimeMin = System.currentTimeMillis() / 1000 / 60;
        ArrayList<String> majorComponents = new ArrayList<String>();
        majorComponents.add("session");
        majorComponents.add("expireTime");
        Key kay = Key.createKey(majorComponents);
        KeyRange kr = new KeyRange("0", true, String.valueOf(curTimeMin), true);
        Iterator<KeyValueVersion> i =
                kvstore.multiGetIterator(Direction.FORWARD, 100,
                kay, kr, null);
        int deleted=0;
        while (i.hasNext()) {
            KeyValueVersion kv = i.next();
            //   Value v = i.next().getValue();
            Key k = kv.getKey();
            File f = new File(k.toString());
            String sessID = f.getName();
            System.out.println("CLEAN "+sessID);
            Value v = kv.getValue();
            try {
              //  long expTime = new Long(new String(v.getValue()));
               // if (expTime != 0 && expTime < curTimeMin) {
                    kvstore.delete(Key.createKey(new String(v.getValue())));
                    kvstore.delete(k);
       //             System.out.println((curTimeMin-expTime)+"\t"+sessID+"\t"+k.toString());
                    deleted++;
              //  }
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
            // Do some work with the Value here
        }
        return deleted;
    }
    @Override
    public void run() {
        clean();
    }
    
}
