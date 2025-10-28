package eee.eie4108.exampleraft;

import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.inject.hk2.AbstractBinder;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class MainAppV2 {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MainAppV2.class);
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: java -jar ./MainApp.jar <peer-port-1> <peer-port-2> ... <start-port> [v1|v2]");
      System.exit(1);
    }
    
    String thisPort = args[args.length - 1];
    List<String> allPorts = Arrays.asList(args);
    
    List<String> peerUrls = allPorts.stream()
                                    .filter(p -> !p.equals(thisPort))
                                    .map(p -> "http://localhost:" + p)
                                    .toList();
    
    String nodeId = "node-" + thisPort;
    String baseUri = "http://localhost:" + thisPort;
    
    RaftNodeV2 node = new RaftNodeV2(nodeId, peerUrls);
    
    // Use a binder to register a specific INSTANCE (singleton)
    // This tells Jersey to use this exact object for all @Path requests
    AbstractBinder binder = new AbstractBinder() {
      @Override
      protected void configure() {
        bind(node).to(RaftNodeV2.class);
      }
    };
    
    try {
      final ResourceConfig config = new ResourceConfig()
                                        .register(CORSResponseFilter.class)
                                        // Add your resource class below
                                        .register(HelloResource.class)
                                        .register(binder)
                                        .register(RaftNodeV2.class);
      
      config.property(ServerProperties.WADL_FEATURE_DISABLE, true);
      String format = "%{client}a - %u %t '%r' %s %O '%{Referer}i' '%{User-Agent}i' '%C'";
      Server server = JettyHttpContainerFactory.createServer(URI.create(baseUri), config, false);
      RequestLog requestLog = new CustomRequestLog("request.log", format);
      server.setRequestLog(requestLog);
      server.start();
      
      // Start Raft Node
      node.start();
      
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          logger.info("Shutting down the application...");
          server.stop();
          logger.info("Done, exit.");
        } catch (Exception e) {
          logger.error(null, e);
        }
      }));
      
      logger.info("Application started. Stop the application using CTRL+C");
      
      // block and wait shut down signal, like CTRL+C
      Thread.currentThread().join();
      
    } catch (Exception ex) {
      logger.error(null, ex);
    }
    
  }
}
