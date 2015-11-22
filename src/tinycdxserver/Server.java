package tinycdxserver;


import org.archive.accesscontrol.AccessControlClient;
import org.archive.accesscontrol.RobotsUnavailableException;
import org.archive.accesscontrol.RuleOracleUnavailableException;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.*;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.channels.Channel;
import java.nio.channels.ServerSocketChannel;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static tinycdxserver.NanoHTTPD.Method.GET;
import static tinycdxserver.NanoHTTPD.Method.POST;
import static tinycdxserver.NanoHTTPD.Response.Status.*;

public class Server extends NanoHTTPD {
    private final DataStore manager;
    boolean verbose = false;

    public Server(DataStore manager, String hostname, int port) {
        super(hostname, port);
        this.manager = manager;
    }

    public Server(DataStore manager, ServerSocket socket) {
        super(socket);
        this.manager = manager;
    }

    String extension(String filename) {
        int i = filename.lastIndexOf('.');
        if (i != -1) {
            return filename.substring(i + 1);
        } else {
            return null;
        }
    }

    String guessContentType(String filename) {
        switch (extension(filename)) {
            case "js": return "application/javascript";
            case "css": return "text/css";
            case "html": return "text/html";
            case "tag": return "riot/tag";
            default: return "application/octet-stream";
        }
    }

    InputStream openResource(String filename) {
        return getClass().getResourceAsStream(getClass().getPackage() + "/" + filename);
    }

    Response notFound() {
        return new Response(NOT_FOUND, "text/plain", "Not found\n");
    }

    @Override
    public Response serve(IHTTPSession session) {
        try {
            String uri = session.getUri();
            Method method = session.getMethod();
            if (uri.equals("/")) {
                return collectionList();
            } else if (method.equals(GET) && uri.startsWith("/static/") && !uri.contains("..")) {
                URL resource = getClass().getResource(uri.substring(1));
                if (resource != null) {
                    Response response = new Response(OK, guessContentType(uri), resource.openStream());
                    response.addHeader("Cache-Control", "max-age=31536000");
                    return response;
                }
            } else if (method.equals(GET)) {
                return query(session);
            } else if (method.equals(POST)) {
                return post(session);
            }
            return notFound();
        } catch (Exception e) {
            e.printStackTrace();
            return new Response(INTERNAL_ERROR, "text/plain", e.toString() + "\n");
        }
    }

    Response post(IHTTPSession session) throws IOException {
        String collection = session.getUri().substring(1);
        final Index index = manager.getIndex(collection, true);
        BufferedReader in = new BufferedReader(new InputStreamReader(session.getInputStream()));
        long added = 0;

        try (Index.Batch batch = index.beginUpdate()) {
            while (true) {
                String line = in.readLine();
                if (verbose) {
                    System.out.println(line);
                }
                if (line == null) break;
                if (line.startsWith(" CDX")) continue;

                try {
                    if (line.startsWith("@alias ")) {
                        String[] fields = line.split(" ");
                        String aliasSurt = UrlCanonicalizer.surtCanonicalize(fields[1]);
                        String targetSurt = UrlCanonicalizer.surtCanonicalize(fields[2]);
                        batch.putAlias(aliasSurt, targetSurt);
                    } else {
                        batch.putCapture(Capture.fromCdxLine(line));
                    }
                    added++;
                } catch (Exception e) {
                    return new Response(Response.Status.BAD_REQUEST, "text/plain", e.toString() + "\nAt line: " + line);
                }
            }

            batch.commit();
        }
        return new Response(OK, "text/plain", "Added " + added + " records\n");
    }

    Response query(IHTTPSession session) throws IOException {
        String collection = session.getUri().substring(1);
        final Index index = manager.getIndex(collection);
        if (index == null) {
            return new Response(NOT_FOUND, "text/plain", "Collection does not exist\n");
        }

        Map<String,String> params = session.getParms();
        if (params.containsKey("q")) {
            return XmlQuery.query(session, index);
        } else if (params.containsKey("url")) {
            return textQuery(index, params.get("url"));
        } else {
            return collectionDetails(index.db);
        }

    }

    private String slurp(InputStream stream) throws IOException {
        StringBuilder sb = new StringBuilder();
        char buf[] = new char[8192];
        try (InputStreamReader reader = new InputStreamReader(stream)) {
            for (; ; ) {
                int n = reader.read(buf);
                if (n < 0) break;
                sb.append(buf, 0, n);
            }
        }
        return sb.toString();
    }

    private Response collectionList() throws IOException {
        String page = "<!doctype html><h1>tinycdxserver</h1>";

        List<String> collections = manager.listCollections();

        if (collections.isEmpty()) {
            page += "No collections.";
        } else {
            page += "<ul>";
            for (String collection : manager.listCollections()) {
                page += "<li><a href=" + collection + ">" + collection + "</a>";
            }
            page += "</ul>";
        }
        page += slurp(Server.class.getClassLoader().getResourceAsStream("tinycdxserver/usage.html"));
        return new Response(OK, "text/html", page);
    }

    private Response collectionDetails(RocksDB db) {
        String page = "<form>URL: <input name=url type=url><button type=submit>Query</button></form>\n<pre>";
        try {
            page += db.getProperty("rocksdb.stats");
            page += "\nEstimated number of records: " + db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            page += e.toString();
            e.printStackTrace();
        }
        return new Response(OK, "text/html", page);
    }

    private Response textQuery(final Index index, String url) {
        final String canonUrl = UrlCanonicalizer.surtCanonicalize(url);
        return new Response(OK, "text/plain", outputStream -> {
            Writer out = new BufferedWriter(new OutputStreamWriter(outputStream));
            for (Capture capture : index.query(canonUrl)) {
                if (!capture.urlkey.equals(canonUrl)) break;
                out.append(capture.toString()).append('\n');
            }
            out.flush();
        });
    }

    public static void usage() {
        System.err.println("Usage: java " + Server.class.getName() + " [options...]");
        System.err.println("");
        System.err.println("  -a url        Use a wayback access control oracle");
        System.err.println("  -b bindaddr   Bind to a particular IP address");
        System.err.println("  -d datadir    Directory to store index data under");
        System.err.println("  -i            Inherit the server socket via STDIN (for use with systemd, inetd etc)");
        System.err.println("  -p port       Local port to listen on");
        System.err.println("  -v            Verbose logging");
        System.exit(1);
    }

    public static void main(String args[]) {
        String host = null;
        int port = 8080;
        boolean inheritSocket = false;
        File dataPath = new File("data");
        boolean verbose = false;
        Predicate<Capture> filter = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-a":
                    filter = accessControlFilter(args[++i]);
                    break;
                case "-p":
                    port = Integer.parseInt(args[++i]);
                    break;
                case "-b":
                    host = args[++i];
                    break;
                case "-i":
                    inheritSocket = true;
                    break;
                case "-d":
                    dataPath = new File(args[++i]);
                    break;
                case "-v":
                    verbose = true;
                    break;
                default:
                    usage();
                    break;
            }
        }

        try (DataStore dataStore = new DataStore(dataPath, filter)) {
            final Server server;
            Channel channel = System.inheritedChannel();
            if (inheritSocket && channel != null && channel instanceof ServerSocketChannel) {
                server = new Server(dataStore, ((ServerSocketChannel) channel).socket());
            } else {
                server = new Server(dataStore, host, port);
            }
            server.verbose = verbose;
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop();
                dataStore.close();
            }));
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private static Predicate<Capture> accessControlFilter(String oracleUrl) {
        AccessControlClient client = new AccessControlClient(oracleUrl);
        return capture -> {
            try {
                return "allow".equals(client.getPolicy(capture.original, new Date(capture.timestamp), new Date(), "public"));
            } catch (RobotsUnavailableException | RuleOracleUnavailableException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
