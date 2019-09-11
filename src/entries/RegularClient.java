package entries;

import ceph.CephTerminal;
import commands.CommonCommand;
import commands.RingCommand;
import commonmodels.PhysicalNode;
import commonmodels.Terminal;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import elastic.ElasticTerminal;
import org.apache.commons.lang3.time.StopWatch;
import req.gen.ClientRequestGenerator;
import req.gen.RequestGenerator;
import req.RequestService;
import req.RequestThread;
import req.StaticTree;
import ring.RingTerminal;
import socket.SocketClient;
import util.Config;
import util.MathX;
import util.SimpleLog;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class RegularClient {

    private SocketClient socketClient;

    private Terminal terminal;

    private Semaphore semaphore = new Semaphore(0);

    private SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
        @Override
        public void onResponse(Request request, Response o) {
            if (o.getAttachment() != null) {
                onTableUpdated(o.getAttachment());
            }
        }

        @Override
        public void onFailure(Request request, String error) {
            SimpleLog.v(error);
        }
    };

    private RequestThread.RequestGenerateThreadCallBack requestGenerateThreadCallBack = (request, client) -> {
        PhysicalNode server = choseServer(request.getAttachment());
        client.send(server.getFullAddress(), request, callBack);
    };

    public static void main(String[] args) {
        RegularClient regularClient;

        try {
            SimpleLog.with("RegularClient", 1);
            regularClient = new RegularClient();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        if (args.length == 0) {
            regularClient.launchTerminal();
        }
        else if (args[0].equals("-r")) {
            if (args.length >= 2) {
                regularClient.launchRequestGenerator(args);
            }
            else {
                System.out.println ("Usage: RegularClient -r <filename> [number of requests]");
            }
        }
        else if (args[0].equals("-f")) {
            if (args.length == 4) {
                regularClient.launchFileRequestGenerator(args);
            }
            else {
                System.out.println ("Usage: RegularClient -f <file in> <file out> <number of requests>");
            }
        }
        else {
            regularClient.launchTerminal(args);
        }
    }

    public RegularClient() throws Exception {
        socketClient = SocketClient.getInstance();
        String scheme = Config.getInstance().getScheme();

        switch (scheme) {
            case Config.SCHEME_RING:
                terminal = new RingTerminal();
                break;
            case Config.SCHEME_ELASTIC:
                terminal = new ElasticTerminal();
                break;
            case Config.SCHEME_CEPH:
                terminal = new CephTerminal();
                break;
            default:
                throw new Exception("Invalid DHT type");
        }

        // terminal.initialize();
    }

    private void connect(InetSocketAddress address) {
        Scanner in = new Scanner(System.in);
        String command = in.nextLine();

        while (!command.equalsIgnoreCase("exit")){
            try {
                Request request = terminal.translate(command);
                if (request.getReceiver() == null)
                    socketClient.send(address, request, callBack);
                else
                    socketClient.send(request.getReceiver(), request, callBack);
                command = in.nextLine();
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            }
        }
    }

    private void launchTerminal() {
        run(new SocketClient.ServerCallBack() {
            @Override
            public void onResponse(Request request, Response response) {
                if (response.getAttachment() != null) {
                    onTableUpdated(response.getAttachment());
                    connect();
                }
            }

            @Override
            public void onFailure(Request request, String error) {
                SimpleLog.v(error);
            }
        });
    }

    private void launchTerminal(String[] args) {
        String[] params = args[0].split(":");
        InetSocketAddress address;

        if (params.length == 2) {
            address = new InetSocketAddress(params[0], Integer.valueOf(params[1]));
        }
        else if (params.length == 1) {
            address = new InetSocketAddress("localhost", Integer.valueOf(params[0]));
        }
        else {
            System.out.println ("Usage: RegularClient [ip:]<port>");
            return;
        }

        connect(address);
    }

    private void launchRequestGenerator(String[] args) {
        run(new SocketClient.ServerCallBack() {
            @Override
            public void onResponse(Request request, Response response) {
                if (response.getAttachment() != null) {
                    onTableUpdated(response.getAttachment());
                    int numOfRequests = Config.getInstance().getNumberOfRequests();
                    int delayToStopAll = Config.getInstance().getDelayToStopAll();
                    if (args.length >= 3) numOfRequests = Integer.valueOf(args[2]);
                    if (args.length == 4) delayToStopAll = Integer.valueOf(args[3]);
                    generateRequest(args[1], numOfRequests, delayToStopAll);
                }
            }

            @Override
            public void onFailure(Request request, String error) {
                SimpleLog.v(error);
            }
        });
    }

    private void launchFileRequestGenerator(String[] args) {
        generateRequestFile(args[0], args[1], Integer.valueOf(args[3]));
    }

    private void connect() {
        Scanner in = new Scanner(System.in);
        String command = in.nextLine();

        while (!command.equalsIgnoreCase("exit")){
            try {
                Request request = terminal.translate(command);
                if (request.getHeader().equals(RingCommand.READ.name()) ||
                        request.getHeader().equals(RingCommand.WRITE.name())) {
                    request.withEpoch(terminal.getEpoch());
                    PhysicalNode server = choseServer(request.getAttachment());
                    socketClient.send(server.getFullAddress(), request, callBack);
                }
                else if (request.getReceiver() != null) {
                    socketClient.send(request.getReceiver(), request, callBack);
                }
                else {
                    Response response = terminal.process(request);
                    SimpleLog.v(String.valueOf(response));
                }

                command = in.nextLine();
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            }
        }
    }

    private void run(SocketClient.ServerCallBack serverCallBack) {
        SimpleLog.v("Fetching table...");

        if (Config.getInstance().getSeeds().size() > 0) {
            Request request = new Request().withHeader(CommonCommand.FETCH.name());
            socketClient.send(Config.getInstance().getSeeds().get(0), request, serverCallBack);
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        else {
            SimpleLog.v("No seed/proxy info found!");
        }
    }

    private void onFinished(){
        socketClient.stop();
        terminal.destroy();
    }

    private void generateRequestFile(String filename, String fileOut, int numOfRequests) {
        try {
            StaticTree tree = StaticTree.getStaticTree(filename);
            FileWriter w = new FileWriter(fileOut);
            BufferedWriter bw = new BufferedWriter(w);
            PrintWriter wr = new PrintWriter(bw, true);

            RequestGenerator generator = new ClientRequestGenerator(tree, terminal);
            int numThreads = Config.getInstance().getNumberOfThreads();
            RequestService service = new RequestService(1,
                    0,
                    numOfRequests * numThreads,
                    generator,
                    (request, client) -> wr.write(request.toCommand()));

            service.start();
            onFinished();
            wr.close();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    private void generateRequest(String filename, int numOfRequests, int delayToStopAll) {
        StaticTree tree;
        try {
            tree = StaticTree.getStaticTree(filename);
        } catch (IOException e) {
            System.out.println("Failed to load file");
            return;
        }

        RequestGenerator generator = new ClientRequestGenerator(tree, terminal);
        int numThreads = Config.getInstance().getNumberOfThreads();
        RequestService service = new RequestService(numThreads,
                Config.getInstance().getReadWriteInterArrivalTime(),
                numOfRequests,
                generator,
                requestGenerateThreadCallBack);

        StopWatch watch = new StopWatch();
        watch.start();
        service.start();
        onFinished();
        try {
            if (delayToStopAll > 0) {
                Thread.sleep(delayToStopAll * 1000);
                watch.stop();
                SimpleLog.v("Time Elapsed: " + watch.getTime(TimeUnit.MINUTES));

//                String[] cmd = new String[]{"/bin/sh", ResourcesLoader.getRelativeFileName(ScriptGenerator.FILE_STOP_ALL_BUT_CLIENT)};
//                Runtime.getRuntime().exec(cmd);
            }
        } catch (Exception ignored) {
            watch.stop();
            SimpleLog.v("Time Elapsed: " + watch.getTime(TimeUnit.MINUTES));
        }
        semaphore.release();
        System.exit(0);
    }

    private void onTableUpdated(Object table) {
        Request request = new Request()
                .withHeader(CommonCommand.UPDATE.name())
                .withLargeAttachment(table);

        Response response = terminal.process(request);
        SimpleLog.v("**************************\nRequest:\n" + request.getLargeAttachment() + "\n" + response + "\n**************************\n");
    }

    private PhysicalNode choseServer(String attachment) {
        String[] filename = attachment.split(" ");
        Request request = new Request()
                    .withHeader(RingCommand.LOOKUP.name())
                    .withAttachment(filename[0]);
        Response response = terminal.process(request);

        @SuppressWarnings("unchecked")
        List<PhysicalNode> pnodes = (List<PhysicalNode>) response.getAttachment();
        return pnodes.get(MathX.nextInt(pnodes.size()));
    }
}
