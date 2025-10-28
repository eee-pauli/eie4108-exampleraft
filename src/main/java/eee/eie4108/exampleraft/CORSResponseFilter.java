package eee.eie4108.exampleraft;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.MultivaluedMap;

import java.io.IOException;

public class CORSResponseFilter implements ContainerResponseFilter {
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    
    MultivaluedMap<String, Object> headers = responseContext.getHeaders();
    
    headers.add("Access-Control-Allow-Origin", "*");//allows CORS requests only coming from abcd.org
    headers.add("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
    headers.add("Access-Control-Allow-Headers", "X-Requested-With, Content-Type");
  }
}
