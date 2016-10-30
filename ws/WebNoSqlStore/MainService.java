package WebNoSqlStore;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;
import static org.tanukisoftware.wrapper.WrapperManager.getRes;

/**
 *
 * @author sbalabanov
 */
public class MainService implements WrapperListener {

    private ActionRunner m_actionRunner;

    private class ActionRunner
            implements Runnable {

        private String m_action;
        private boolean m_alive;

        public ActionRunner(String action) {
            this.m_action = action;
            this.m_alive = true;
        }

        public void run() {
            System.out.println(getRes().getString("OnSQL WS Service: run..."+(this.m_alive?" true":" false")));
            
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
            }

            while (this.m_alive) {
                try {
                    Thread.sleep(500L);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(getRes().getString(e.getMessage()));
                }
            }
            System.out.println(getRes().getString("OnSQL WS Service: Exit..."));
        }

        public void endThread() {
            this.m_alive = false;
        }
    }

    public static void main(String[] args) {
        
        System.out.println(getRes().getString("OnSQL WS Service: Initializing..."));

        WrapperManager.start(new MainService(), args);
    }

    @Override
    public Integer start(String[] args) {
        try {
                 
            AsynchronousService.main(args);
            System.out.println(getRes().getString("OnSQL WS Service: Started..."));
            this.m_actionRunner = new ActionRunner("OnSQLws");
            Thread actionThread = new Thread(this.m_actionRunner);
            actionThread.start();
            
        } catch (Exception ex) {
            Logger.getLogger(MainService.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public int stop(int exitCode) {
        System.out.println(getRes().getString("OnSQL WS Service: stop({0})", new Integer(exitCode)));
        try {
            AsynchronousService.stop();
        } catch (IOException ex) {
            Logger.getLogger(MainService.class.getName()).log(Level.SEVERE, null, ex);
        }
        return exitCode;
    }

    @Override
    public void controlEvent(int event) {
        System.out.println(getRes().getString("OnSQL WS Service: controlEvent({0})", new Integer(event)));
    }
}
