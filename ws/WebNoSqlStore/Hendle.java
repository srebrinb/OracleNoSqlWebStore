package WebNoSqlStore;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import org.simpleframework.http.Request;
import org.simpleframework.http.Response;

/**
 *
 * @author sbalabanov
 */
public class Hendle {
    private Response response;
    private Request request;
    public void hendle(Request request, Response response) {
         this.response = response;
         this.request = request;
    }
}
