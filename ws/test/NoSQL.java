/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.RequestTimeoutException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.lob.InputStreamVersion;

/**
 *
 * @author sbalabanov
 */
public class NoSQL {

    private final KVStore kvstore;

    NoSQL() {
        String storeName = "kvstore";
     //   String hostName = "10.1.20.57";
        String hostName = "10.1.20.54";
        String hostPort = "5000";
        kvstore = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
        System.out.println("connced to " + hostName + ":" + hostPort);
    }

    public void insert(ArrayList<String> majorComponents, ArrayList<String> minorComponents, String data) {
        Key myKey = null;
        if (minorComponents != null) {
            myKey = Key.createKey(majorComponents, minorComponents);
        } else {
            myKey = Key.createKey(majorComponents);
        }
        Value myValue = Value.createValue(data.getBytes());
        kvstore.put(myKey, myValue);
    }

    public String get(ArrayList<String> majorComponents, ArrayList<String> minorComponents) {
        Key myKey = null;
        String res = "";
        if (minorComponents != null && !minorComponents.isEmpty()) {
            myKey = Key.createKey(majorComponents, minorComponents);
        } else {
            myKey = Key.createKey(majorComponents);
        }

        SortedMap<Key, ValueVersion> myRecords = null;
        try {
//            myRecords = kvstore.multiGet(myKey, null, null);
//            for (Map.Entry<Key, ValueVersion> entry : myRecords.entrySet()) {
//                ValueVersion vv = entry.getValue();
//                System.out.println(vv.getVersion().getVersion());
//                Value v = vv.getValue();
//                // Do some work with the Value here
//                res += new String(v.getValue()) + " ";
//            }
            Iterator<KeyValueVersion> i =
                    kvstore.multiGetIterator(Direction.FORWARD, 0,
                    myKey, null, null);
            while (i.hasNext()) {
                KeyValueVersion kvv = i.next();
                System.out.println(kvv.getKey().toString());
                Value v = kvv.getValue();
                res += new String(v.getValue()) + " ";
                // Do some work with the Value here
            }
        } catch (ConsistencyException ce) {
            // The consistency guarantee was not met
        } catch (RequestTimeoutException re) {
            // The operation was not completed within the 
            // timeout value
        }
        return res;
    }

    public String getKR(ArrayList<String> majorComponents, ArrayList<String> minorComponents) {
        Key myKey = null;
        String res = "";
        if (minorComponents != null && !minorComponents.isEmpty()) {
            myKey = Key.createKey(majorComponents, minorComponents);
        } else {
            myKey = Key.createKey(majorComponents);
        }

        SortedMap<Key, ValueVersion> myRecords = null;
        try {
//            myRecords = kvstore.multiGet(myKey, null, null);
//            for (Map.Entry<Key, ValueVersion> entry : myRecords.entrySet()) {
//                ValueVersion vv = entry.getValue();
//                System.out.println(vv.getVersion().getVersion());
//                Value v = vv.getValue();
//                // Do some work with the Value here
//                res += new String(v.getValue()) + " ";
//            }
            long curTimeMin = System.currentTimeMillis() / 1000 / 60;
            System.out.println("curTimeMin:" + curTimeMin);
            KeyRange kr = new KeyRange("0", false, String.valueOf(curTimeMin + 50), true);
            Iterator<KeyValueVersion> i =
                    kvstore.multiGetIterator(Direction.FORWARD, 0,
                    myKey, kr, null);
            while (i.hasNext()) {
                KeyValueVersion kvv = i.next();
                System.out.println(kvv.getKey().toString());
                Value v = kvv.getValue();
                System.out.println(new String(v.getValue()));
                res += new String(v.getValue()) + " ";
                // long expTime = new Long(new String(v.getValue()));
                // if (expTime != 0 && expTime < curTimeMin) {
                //      System.out.println("del");
                // }
                // Do some work with the Value here
            }
        } catch (ConsistencyException ce) {
            // The consistency guarantee was not met
        } catch (RequestTimeoutException re) {
            // The operation was not completed within the 
            // timeout value
        }
        return res;
    }

    public void coontobj() {
        Iterator<Key> i = kvstore.storeKeysIterator(Direction.UNORDERED, 100);
        int count = 0;

        while (i.hasNext()) {
            i.next();
            count++;
        }
        System.out.println("count:" + count);
    }

    public void insertLog(Key key, InputStream fis) {
        try {
            Version version;
            version = kvstore.putLOB(key, fis,
                    Durability.COMMIT_SYNC,
                    5, TimeUnit.SECONDS);
        } catch (DurabilityException ex) {
            Logger.getLogger(NoSQL.class.getName()).log(Level.SEVERE, null, ex);
        } catch (RequestTimeoutException ex) {
            Logger.getLogger(NoSQL.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ConcurrentModificationException ex) {
            Logger.getLogger(NoSQL.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FaultException ex) {
            Logger.getLogger(NoSQL.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(NoSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void pipe(InputStream is, OutputStream os) throws IOException {
        //  FileChannel source = ((FileInputStream) is).getChannel();
        //  FileChannel destnation = ((FileOutputStream) os).getChannel();
        //  destnation.transferFrom(source, 0, source.size());
        byte[] buffer = new byte[1024];
        while (is.read(buffer) > -1) {
            os.write(buffer);
        }
    }

    public void getLob(Key key, OutputStream os) {
        try {
            InputStreamVersion istreamVersion =
                    kvstore.getLOB(key,
                    Consistency.ABSOLUTE,
                    5, TimeUnit.SECONDS);
            InputStream stream = istreamVersion.getInputStream();
            pipe(stream, os);
        } catch (IOException ex) {
            Logger.getLogger(NoSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void main(String[] args) throws Exception {
        NoSQL nosql = new NoSQL();
        nosql.coontobj();
        
        ArrayList<String> majorComponents = new ArrayList<String>();
        ArrayList<String> minorComponents = new ArrayList<String>();
        majorComponents.add("obj");
        majorComponents.add("pdf");
        minorComponents.add("1");
        minorComponents.add("2");
        nosql.insert(majorComponents, minorComponents, "val in 1.2");
        majorComponents = new ArrayList<String>();
        minorComponents = new ArrayList<String>();
        majorComponents.add("obj");
        majorComponents.add("pdf");
        minorComponents.add("1");
        minorComponents.add("2");
        minorComponents.add("3");
        nosql.insert(majorComponents, minorComponents, "val in 1.2.3");
        minorComponents = new ArrayList<String>();
        minorComponents.add("2");
        minorComponents.add("21");
        nosql.insert(majorComponents, minorComponents, "2.21");
        minorComponents = new ArrayList<String>();
        minorComponents.add("3");
        minorComponents.add("31");
        nosql.insert(majorComponents, minorComponents, "3.31");
        minorComponents.add("32");
        nosql.insert(majorComponents, minorComponents, "3.32");
        //   majorComponents = new ArrayList<String>();
        //   majorComponents.add("obj");
        minorComponents = new ArrayList<String>();
        minorComponents.add("1");
        String val1 = nosql.get(majorComponents, minorComponents);
        System.out.println("val1:" + val1);
        minorComponents = new ArrayList<String>();
        minorComponents.add("2");
        String val2 = nosql.get(majorComponents, minorComponents);
        System.out.println("val2:" + val2);
        minorComponents = new ArrayList<String>();

        minorComponents.add("3");
        minorComponents.add("31");
        majorComponents = new ArrayList<String>();
        majorComponents.add("session");
        majorComponents.add("expireTime");
        //String val31 = nosql.get(majorComponents, null);
        // System.out.println("val31:" + val31);
        majorComponents = new ArrayList<String>();
        majorComponents.add("session");
        majorComponents.add("expireTime");
        // String val31KR = nosql.getKR(majorComponents, null);
        //  System.out.println("val31:" + val31KR);
        String pdfPath = "f:/books_off/";
        File file = new File(pdfPath);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isFile();
            }
        });
       long startAll = System.currentTimeMillis();
        for (int i = 0; i < directories.length; i++) {
            String pfile = pdfPath + directories[i];
            //System.out.println(pfile);
            // sig.SignPDF(pfile);    
            if (pfile.endsWith(".pdf")) {
                File lobFile = new File(pfile);
                Key key = Key.createKey(Arrays.asList("test", "lob", lobFile.getName()+ ".lob"));                
       /*         
                
                FileInputStream fis = new FileInputStream(lobFile);
                long start = System.currentTimeMillis();
                nosql.insertLog(key, fis);
                long end = System.currentTimeMillis();
                fis.close();
                System.out.println("insertLog "+lobFile.getName()+" Execution Time = " + (end - start) + " millis");
        */ 
                OutputStream os = new FileOutputStream("D:\\"+lobFile.getName());
                long start = System.currentTimeMillis();
                nosql.getLob(key, os);
                long end = System.currentTimeMillis();
                System.out.println("getLob Execution Time = " + (end - start) + " millis");
                os.close();
            }
        }
        long endAll = System.currentTimeMillis();
        System.out.println("insertLog ALL Execution Time = " + (endAll - startAll) + " millis");
//        OutputStream os = new FileOutputStream("D:\\out.zip");
//        start = System.currentTimeMillis();
//        nosql.getLob(key, os);
//        end = System.currentTimeMillis();
//        System.out.println("getLob Execution Time = " + (end - start) + " millis");
//        os.close();
    }
}
