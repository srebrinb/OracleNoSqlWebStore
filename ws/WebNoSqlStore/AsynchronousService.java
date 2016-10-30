package WebNoSqlStore;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.kv.util.kvlite.KVLite;
import org.apache.commons.io.IOUtils;
import org.simpleframework.http.Part;
import org.simpleframework.http.Path;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.simpleframework.http.core.ContainerServer;
import org.simpleframework.transport.connect.SocketConnection;

public class AsynchronousService implements Container {

    private static KVLite kvlite = null;
    private static ContainerServer server = null;
    private static SocketConnection connection = null;

    private final Store store;
//    private final Monitor monitor;

    private class Monitor {

        public int gets = 0;
        public int puts = 0;
        public int alls = 0;
        public int outs = 0;

        Monitor() {
            // ht=new HashMap();
        }

        public void addGet() {
            gets += 1;
        }

        public void addPut() {
            puts += 1;
        }

        public void addAlls() {
            alls += 1;

        }

        public void print() {
            int totals = outs;
            if (totals % 1000 == 0) {
                StringBuilder sb=new StringBuilder("gets: \t" + gets);
                sb.append("\tputs: \t").append(puts);
                sb.append("\talls: \t").append(alls);
                sb.append("\tdelta: \t").append(alls - (gets + puts));
                sb.append("\touts: \t").append (outs);
                sb.append("\tdelta: \t").append(outs - (alls));
                Logger.getLogger(Monitor.class.getName()).log(Level.FINE,sb.toString() );
            }
        }

        public void addOut() {
            outs += 1;
            print();
        }
    }

    public static class Task implements Runnable {

        private Response response;
        private Request request;
        private final Store inStore;
        private Monitor monitor;

        public Task(Request request, Response response, Store store) {
            //     System.out.println("Task");
            this.response = response;
            this.request = request;
            this.inStore = store;
            //    hendleVerifier = new HendleVerifier();
            //hendlerSigner = new HendlerSigner();

        }

        @Override
        public void run() {
            final String orgName = Thread.currentThread().getName();
            long time = System.currentTimeMillis();
            Thread.currentThread().setName(orgName + " " + time);
            try {
                System.setProperty("opt.skipEncodedTsaCert", "On");

                response.setValue("Server", "NoSQL Store WS/1.0");
                response.setDate("Date", time);
                response.setDate("Last-Modified", time);
                response.setValue("Content-Type", "text/plan");
                /*
            
                 String directory = path.getDirectory();
                 String pathname = path.getName(); 
            
                 System.out.println("directory:"+directory);
                 System.out.println("pathname:"+pathname);
                 for(String segment : segments) {
                 System.out.println("segment:"+segment);
                 }
                 */
                PrintStream body;
                try {
                    body = response.getPrintStream();
                } catch (IOException ex) {
                    Logger.getLogger(AsynchronousService.class.getName()).log(Level.SEVERE, null, ex);
                    return;
                }
                String method = request.getMethod();
                Path path = request.getPath();
                String[] segments = path.getSegments();
                /*if (segments.length < 1) {
                 try {
                 response.close();
                 } catch (IOException ex) {
                 System.out.println("handle");
                 Logger.getLogger(AsynchronousService.class.getName()).log(Level.SEVERE, null, ex);
                 }
                 }
                 System.out.println("segment:" + segments[0]);
                 */
                List<Part> list = request.getParts();
                Runtime runtime = Runtime.getRuntime();
                int mb = 1024 * 1024;
                String threadNum = request.getValue("threadNum");
                long timeThread = System.currentTimeMillis();
                //     monitor.addAlls();
                if (method.equalsIgnoreCase("GET")) {
                    if (segments.length > 0) {
                        //         monitor.addGet();
                        try {
                            OutputStream baos = response.getOutputStream();
                            byte[] value = null;
                            synchronized (inStore) {
                                value = inStore.get(segments[0]);
                            }
                            if (value == null) {
                                response.setCode(404);
                                body.print("not found object:" + segments[0]);
                            } else {
                                // byte[] tmp = inStore.get(segments[0] + "_Content-Type");
                                String ContentType = inStore.getMeta(segments[0], "Content-Type");
                                if (ContentType != null) {
                                    
                                    if (ContentType != null) {
                                        response.setValue("Content-Type", ContentType);
                                    }
                                }
                                String CreateTimestamp = inStore.getMeta(segments[0], "Create-Timestamp");
                                if (CreateTimestamp != null) {
                                    response.setValue("Create-Timestamp", CreateTimestamp);
                                }
                                baos.write(value, 0, value.length);
                            }
                        } catch (Exception e) {
                            response.setCode(500);
                            body.println("response get error");
                            body.println("Exception:" + e.getMessage());

                        } finally {
                            response.close();
                            //   return;
                        }
                    } else {
                        //OutputStream baos = response.getOutputStream();
                        response.setCode(200);
                        response.setValue("Content-Type", "text/html; charset=windows-1251");
                        List<String> files = new ArrayList();
                        synchronized (inStore) {
                            files = inStore.list();

                        }
                        body.println("<html><pre>");
                        for (String file : files) {
                            body.println("<a href='" + URLEncoder.encode(file, "UTF-8") + "'>" + file + "</a>");
                        }
                        body.println("</pre></html>");
                        response.close();
                    }

                }
                String savedKey = null;
                if (method.equalsIgnoreCase("DELETE") && segments.length > 0) {
                    inStore.delete(segments[0]);
                }
                if (method.equalsIgnoreCase("CLEAN")) {
                    // System.out.println("CLEAN");
                    // inStore.clean();
                    CleanStore cleaner = new CleanStore(inStore);
                    if (segments.length > 0 && segments[0].equals("sync")) {
                        int i = cleaner.clean();
                        Logger.getLogger(Monitor.class.getName()).log(Level.FINE,"deleted " + i + " obj" );
                        body.println("deleted " + i + " obj");
                        response.setCode(200);

                    } else {
                        new Thread(cleaner).start();
                    }

                }
                if (method.equalsIgnoreCase("PUT")) {
                    //          monitor.addPut();
                    try {
                        InputStream bais = request.getInputStream();
                        //        byte[] array = new byte[bais.available()];
                        //        bais.read(array);
                        byte[] array = IOUtils.toByteArray(bais);

                        int lifeTime = 0;
                        try {
                            String tmpLifeTime = request.getValue("lifeTime");
                            lifeTime = Integer.parseInt(tmpLifeTime);
                        } catch (Exception e) {
                        }
                        //   synchronized (inStore) {
                        if (segments.length > 0) {
                            inStore.put(segments[0], array, lifeTime);
                            savedKey = segments[0];
                            body.println("segments[0]" + savedKey);
                        } else {
                            savedKey = inStore.put(array);
                            //    body.println(savedKey+"\r\n");
                        }
                        String metaData;
                        //   System.out.println(request.getNames());
                        // inStore.put(savedKey + "_Content-Type", request.getValue("Content-Type").getBytes(), lifeTime);
                        inStore.putMeta(savedKey, "Content-Type", request.getValue("Content-Type"));
                        inStore.putMeta(savedKey, "Create-Timestamp", Long.toString(System.currentTimeMillis()));
                        response.setCode(200);
                        //     }
                        body.println(savedKey);
                    } catch (Exception e) {
                        response.setCode(500);
                        body.println("response put error");
                        body.println("Exception:" + e.getMessage());
                        body.println(savedKey);
                        e.printStackTrace();
                    } finally {
                        //  return;
                    }
                }
                if (method.equalsIgnoreCase("OPTIONS")) {

                    response.setCode(200);
                    response.setValue("Access-Control-Allow-Origin", "*");
                    response.setValue("Access-Control-Allow-Methods", "GET, PUT, POST, TRACE, OPTIONS");

                }
                String out = method;// "threadNum: " + threadNum ;
                if (segments.length > 0) {
                    out += "\t" + segments[0];
                }
                out += "\t" + request.getClientAddress().toString() + "\t" + (System.currentTimeMillis() - timeThread);

                // 
                out += "\t" + (time - request.getRequestTime());
                out += "\t" + (runtime.totalMemory() - runtime.freeMemory()) / mb;
                //out += "\t" + (request.getRequestTime()) ;
                //out += "\t" + (time) ;                
                out += "\t" + (runtime.freeMemory()) / mb;
                out += "\t" + (runtime.totalMemory()) / mb;
                out += "\t" + (runtime.maxMemory()) / mb;
                Logger.getLogger(Monitor.class.getName()).log(Level.FINE,out );
            } catch (Exception ex) {
                Logger.getLogger(AsynchronousService.class.getName()).log(Level.SEVERE, null, ex);
                ex.printStackTrace();
            } finally {
                try {
                    response.close();
                    //        monitor.addOut();
                    Thread.currentThread().setName(orgName);
                } catch (IOException ex) {
                    Logger.getLogger(AsynchronousService.class.getName()).log(Level.SEVERE, null, ex);
                    ex.printStackTrace();
                }
            }
        }

        private void setMonitor(Monitor monitor) {
            this.monitor = monitor;
        }
    }
    private final Executor executor;

    public AsynchronousService(int size) throws SQLException {
        this.executor = Executors.newFixedThreadPool(size);
        store = new Store();
        //     monitor = new Monitor();
        // this.executor = Executors.newCachedThreadPool();// .newFixedThreadPool(size);
    }

    public void handle(Request request, Response response) {
        Task task;

        //    System.out.println("handle");
        task = new Task(request, response, store);
//        task.setMonitor(monitor);
        Thread thread = new Thread(task);

        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss:SSS");
        Date rundate = new Date(System.currentTimeMillis());
        thread.setName("handle " + sdf.format(rundate));
        thread.start();
        //  executor.execute(task);

    }

    public static void main(String[] args) throws Exception {

        String host = "localhost";
        String kvroot = "./kvroot";
        String kvstore = "mystore";
        int kvport = 5000;
        int adminPort = 5001;

        kvlite = new KVLite(kvroot, kvstore, kvport,
                adminPort, host, null, null, 0, null, true);
        kvlite.setVerbose(true);
        kvlite.start(true);
        
        Thread.sleep(5 * 1000);

        Container container = new AsynchronousService(50);
        server = new ContainerServer(container);

        connection = new SocketConnection(server);
        int port = 5055;
        if (args.length >= 1) {
            port = Integer.parseInt(args[1]);
        }
        SocketAddress address = new InetSocketAddress(port);

        connection.connect(address);

        Logger.getLogger(AsynchronousService.class.getName()).log(Level.INFO, "run on port:" + port);
    }

    public static void stop() throws IOException {

        if (connection != null) {
            connection.close();
        }
        if (server != null) {
            server.stop();
        }
        if (kvlite != null) {
            kvlite.stop(false);
        }

    }
}
