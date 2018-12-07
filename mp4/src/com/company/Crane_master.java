package com.company;

import java.io.BufferedReader;
import java.io.*;
import java.net.*;
import java.util.*;


/* Crane_master
 * Functionality: A crane master receives the topology and
 * function implementations from the client, route the bolts
 * to different workers and spawn all spouts need for a job
 */
public class Crane_master extends Thread {
    private Vector<InetAddress> member_list;
    private String curr_job;
    private ServerSocket sock;
    public List<String> ip_array;
    private Hashtable<String, Vector> curr_job_in;
    private Hashtable<String, Vector> curr_job_out;
    private ArrayList<Thread> spout_list;

    public Crane_master(Vector<InetAddress> mem_list) throws IOException {
        System.out.println("master");
        sock = new ServerSocket(5000);
        member_list = mem_list;
        curr_job_in = new Hashtable<String, Vector>();
        curr_job_out = new Hashtable<String, Vector>();
        ip_array = Arrays.asList("172.22.156.75", "172.22.158.75", "172.22.154.76", "172.22.156.76", "172.22.158.76",  //the list of iparrays of vms
                "172.22.154.77", "172.22.156.77", "172.22.158.77", "172.22.154.78", "172.22.156.78");
        spout_list = new ArrayList<>();
    }

    /* bolt_schedule()
     * Functionality: This function schedule bolts based on the
     * submitted topology to all the available worker.
     */
    public Hashtable bolt_schedule() throws IOException{
        /* Start scheduling bolts to clusters */

        Vector<InetAddress> worker_list = new Vector(member_list);       // generate a list of workers

        if(worker_list.contains(InetAddress.getByName(ip_array.get(0))))    // not include master
            worker_list.remove(InetAddress.getByName(ip_array.get(0)));
        if(worker_list.contains(InetAddress.getByName(ip_array.get(1))))    // not include backup master
            worker_list.remove(InetAddress.getByName(ip_array.get(1)));
        if(worker_list.contains(InetAddress.getByName(ip_array.get(2))))    // not include client
            worker_list.remove(InetAddress.getByName(ip_array.get(2)));

        Hashtable bolt_map = new Hashtable();
        Hashtable<InetAddress, Vector<Integer>> node_port = new Hashtable();

        for(InetAddress node: worker_list){
            node_port.put(node, new Vector());
        }

        int i = 0;
        for(String key: curr_job_in.keySet()){
            int port;
            InetAddress addr = worker_list.get(i);
            if(node_port.get(addr).size() == 0){
                port = 5100;
                node_port.get(addr).addElement(port);
            }
            else{
                port = node_port.get(addr).lastElement() + 1;
                node_port.get(addr).addElement(port);
            }
            Vector pair = new Vector<>();
            pair.addElement(addr);
            pair.addElement(port);
            bolt_map.put(key, pair);
            i = (i + 1 == worker_list.size() ? 0: i + 1);
        }
        System.out.println(bolt_map);
        return bolt_map;
    }

    /* spout_schedule()
     * Functionality: This function schedule spouts based on the
     * submitted topology to send to all the available worker.
     */
    public Hashtable spout_schedule(){
        Hashtable spout_map = new Hashtable();
        int idx = 0;
        for(String key: curr_job_out.keySet()){
            if(!curr_job_in.containsKey(key) && !key.contains(".db")){
                // check if a new spout
                spout_map.put(key, 5100 + idx);
                idx++;
            }
            else
                continue;
        }
        return spout_map;
    }

    /* make_jobdir(String dirname)
     * This function create a new directory store all
     * files of a give job
     */
    public void make_jobdir(String dirname){
        File directory = new File(dirname);
        if (!directory.exists())
            directory.mkdir();
        else{
            String[] entries = directory.list();
            for(String s: entries){
                File currentFile = new File(directory.getPath(),s);
                //System.out.println(s);
                if(currentFile.isDirectory()){
                    String[] subdir = currentFile.list();
                    for(String f: subdir){
                        File currentsubFile = new File(currentFile.getPath(),f);
                        currentsubFile.delete();
                    }
                    currentFile.delete();
                }
                else
                    currentFile.delete();
            }
        }
    }

    /* get_own_ip()
     * Functionality: This function returns the ip address
     * of the node.
     */
    public String get_own_ip(){
        String ret= "";
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ret = socket.getLocalAddress().getHostAddress();
        }
        catch (Exception e0){ e0.printStackTrace();}
        return ret;
    }

    /* rerun()
     * Functionality: In case of worker failure, this function is called
     * to rerun the entire application
     */
    public int rerun() throws IOException{
        curr_job_in = new Hashtable<>();
        curr_job_out = new Hashtable<>();
        System.out.println("Rerun job: " + curr_job);

        File dir = new File("./" + curr_job);
        String[] entries = dir.list();
        String topo = "./" + curr_job;
        for(String s: entries){
            if(s.contains(".topo")){
                topo += "/" + s;
                break;
            }
        }
        File f = new File(topo);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        String line = reader.readLine();
        while(line != null){
            System.out.println(line);

            String u = line.split(",\\s+")[0];
            String v = line.split(",\\s+")[1];
            if(!curr_job_out.containsKey(u)){
                Vector list = new Vector();
                list.addElement(v);
                curr_job_out.put(u, list);
            }
            else{
                curr_job_out.get(u).addElement(v);
            }
            if(!curr_job_in.containsKey(v)){
                Vector list = new Vector();
                list.addElement(u);
                curr_job_in.put(v, list);
            }
            else{
                curr_job_in.get(v).addElement(u);
            }
            line = reader.readLine();
        }

        System.out.println("In list:");
        System.out.println(curr_job_in);
        System.out.println("Out list:");
        System.out.println(curr_job_out);
        reader.close();

        /* allocate resources for spouts and bolts */
        Hashtable<String, Vector> bolt_map = bolt_schedule();
        Hashtable<String, Vector> spout_map = spout_schedule();
        Hashtable<String, Vector> spout_to = new Hashtable<>();

        for(String key: spout_map.keySet()){
            spout_to.put(key, new Vector());
        }

        for(Object key: bolt_map.keySet()){
            String param = "";

            System.out.print(key);
            Vector sending = curr_job_out.get(key);
            System.out.println(" sending to ");
            if(sending != null){
                for(Object u: sending){
                    System.out.println(bolt_map.get(u));
                    param = param + bolt_map.get(u).get(0) + ":" + bolt_map.get(u).get(1) + " ";
                }
            }

            Vector recving = curr_job_in.get(key);
            System.out.println(" recving from ");
            if(recving != null) {
                for(Object u: recving){
                    if(bolt_map.get(u) != null){
                        System.out.println(bolt_map.get(u));
                        param = param + bolt_map.get(u).get(0) + ";" + bolt_map.get(u).get(1) + " ";
                    }
                    else{
                        if(spout_map.containsKey(u)){   // read from a spout
                            System.out.println(u + " at " + ip_array.get(0) + ";" + spout_map.get(u));
                            spout_to.get(u).addElement(bolt_map.get(key));
                            param = param + ip_array.get(0) + ";" + spout_map.get(u) + " ";
                        }
                        else{   // read from a database
                            System.out.println("read from db: " + u);
                            param = param + u + " ";
                        }
                    }
                }
            }
            System.out.println("spout schedule: ");
            System.out.println(spout_to);

            InetAddress addr = (InetAddress)bolt_map.get(key).get(0);
            int port = (int)bolt_map.get(key).get(1);
            Socket worker_sock = new Socket(addr, 5000);
            OutputStream os = worker_sock.getOutputStream();
            DataOutputStream out = new DataOutputStream(os);

            out.writeUTF(key + " " + port + " " + param);
            File bolt_prog = new File(curr_job + "/" + key + ".cpp");
            FileInputStream fis = new FileInputStream(bolt_prog);
            BufferedInputStream bis = new BufferedInputStream(fis);
            byte[] mybytearray = new byte[5000];
            int count;
            while((count = bis.read(mybytearray)) > 0){
                os.write(mybytearray, 0, count);
            }

            worker_sock.close();

            System.out.println("param to send to addr: " + addr + " is: " + key + " " + port + " " + param);
        }
        /* spawn all the spout thread on master */
        for(String key: spout_to.keySet()){
            Vector<Vector> list = spout_to.get(key);
            for(Vector inet: list){
                Spout s = new Spout(curr_job + "/" + key, (InetAddress)inet.get(0), (int)inet.get(1));
                spout_list.add(s);
                s.start();
            }
        }

        int num_ack = 0;
        int num_fail = 0;
        while(num_ack < curr_job_in.keySet().size()){
            try{
                if(num_fail >= 2)
                    break;
                Socket replySocket = sock.accept();
                InputStream is = replySocket.getInputStream();
                DataInputStream dis = new DataInputStream(is);
                String msg = dis.readUTF();
                is.close();
                replySocket.close();
                if(msg.equals("Done")){
                    num_ack++;
                }
                if(msg.equals("Fail")){
                    num_fail++;
                    sock.setSoTimeout(10000);
                }
            }
            catch (IOException e){
                break;
            }
        }

        sock.setSoTimeout(0);

        if(num_ack < curr_job_in.keySet().size())
            return -1;

        System.out.println("Rerun successful");
        return 0;
    }


    public void run() {
        while (true) {
            try{
                curr_job_in = new Hashtable<>();
                curr_job_out = new Hashtable<>();
                Socket clientSocket = sock.accept();
                InputStream is = clientSocket.getInputStream();
                DataInputStream clientData = new DataInputStream(is);
                String jobname = clientData.readUTF();
                curr_job = jobname;

                if(jobname.equals("Fail") || jobname.equals("masterfail")){
                    clientSocket.close();
                    is.close();
                    clientData.close();
                    continue;
                }

                System.out.println("got job from: " + clientSocket.getInetAddress() + " at:" + clientSocket.getPort());
                int file_num = clientData.readInt();
                make_jobdir(jobname);
                for(int i = 0; i < file_num; i++){
                    String filePath = "./" + jobname + "/" + clientData.readUTF();
                    Long size = clientData.readLong();
                    OutputStream output = new FileOutputStream(filePath);
                    BufferedOutputStream out = new BufferedOutputStream(output);
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    while (size > 0 && (bytesRead = is.read(buffer, 0, (int)Math.min(buffer.length, size))) != -1) {
                        out.write(buffer, 0, bytesRead);
                        size -= bytesRead;
                    }
                    out.flush();
                }
                System.out.println("Job received.");
                is.close();
                clientSocket.close();

                String master = member_list.contains(InetAddress.getByName(ip_array.get(0))) ? ip_array.get(0) : ip_array.get(1);

                if(!get_own_ip().equals(ip_array.get(0))){
                    if(member_list.contains(InetAddress.getByName(ip_array.get(0)))){
                        int finished = 0;
                        while(true){
                            Socket reply_sock = sock.accept();
                            is = reply_sock.getInputStream();
                            DataInputStream replyData = new DataInputStream(is);
                            String reply = replyData.readUTF();
                            if(reply.equals("finished")){
                                System.out.println("Job finished.");
                                reply_sock.close();
                                curr_job = "";
                                spout_list.clear();
                                finished = 1;
                                break;
                            }
                            if(reply.equals("masterfail")){
                                System.out.println("Master failed.");
                                Thread.sleep(10000);
                                master = ip_array.get(1);
                                break;
                            }
                        }

                        if(finished == 1)
                            continue;
                    }
                }

                File dir = new File("./" + jobname);
                String[] entries = dir.list();
                String topo = "./" + jobname;
                for(String s: entries){
                    if(s.contains(".topo")){
                        topo += "/" + s;
                        break;
                    }
                }
                File f = new File(topo);
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
                String line = reader.readLine();
                while(line != null){
                    System.out.println(line);

                    String u = line.split(",\\s+")[0];
                    String v = line.split(",\\s+")[1];
                    if(!curr_job_out.containsKey(u)){
                        Vector list = new Vector();
                        list.addElement(v);
                        curr_job_out.put(u, list);
                    }
                    else{
                        curr_job_out.get(u).addElement(v);
                    }
                    if(!curr_job_in.containsKey(v)){
                        Vector list = new Vector();
                        list.addElement(u);
                        curr_job_in.put(v, list);
                    }
                    else{
                        curr_job_in.get(v).addElement(u);
                    }
                    line = reader.readLine();
                }
                System.out.println("In list:");
                System.out.println(curr_job_in);
                System.out.println("Out list:");
                System.out.println(curr_job_out);
                reader.close();


                Hashtable<String, Vector> bolt_map = bolt_schedule();
                Hashtable<String, Vector> spout_map = spout_schedule();
                Hashtable<String, Vector> spout_to = new Hashtable<>();

                for(String key: spout_map.keySet()){
                    spout_to.put(key, new Vector());
                }

                for(Object key: bolt_map.keySet()){
                    String param = "";

                    System.out.print(key);
                    Vector sending = curr_job_out.get(key);
                    System.out.println(" sending to ");
                    if(sending != null){
                        for(Object u: sending){
                            System.out.println(bolt_map.get(u));
                            param = param + bolt_map.get(u).get(0) + ":" + bolt_map.get(u).get(1) + " ";
                        }
                    }

                    Vector recving = curr_job_in.get(key);
                    System.out.println(" recving from ");
                    if(recving != null) {
                        for(Object u: recving){
                            if(bolt_map.get(u) != null){
                                System.out.println(bolt_map.get(u));
                                param = param + bolt_map.get(u).get(0) + ";" + bolt_map.get(u).get(1) + " ";
                            }
                            else{
                                if(spout_map.containsKey(u)){   // read from a spout
                                    System.out.println(u + " at " + master + ";" + spout_map.get(u));
                                    spout_to.get(u).addElement(bolt_map.get(key));
                                    param = param + master + ";" + spout_map.get(u) + " ";
                                }
                                else{   // read from a database
                                    System.out.println("read from db: " + u);
                                    param = param + u + " ";
                                }
                            }
                        }
                    }
                    System.out.println("spout schedule: ");
                    System.out.println(spout_to);

                    InetAddress addr = (InetAddress)bolt_map.get(key).get(0);
                    int port = (int)bolt_map.get(key).get(1);
                    Socket worker_sock = new Socket(addr, 5000);
                    OutputStream os = worker_sock.getOutputStream();
                    DataOutputStream out = new DataOutputStream(os);

                    out.writeUTF(key + " " + port + " " + param);
                    File bolt_prog = new File(jobname + "/" + key + ".cpp");
                    FileInputStream fis = new FileInputStream(bolt_prog);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    byte[] mybytearray = new byte[5000];
                    int count;
                    while((count = bis.read(mybytearray)) > 0){
                        os.write(mybytearray, 0, count);
                    }

                    worker_sock.close();

                    System.out.println("param to send to addr: " + addr + " is: " + key + " " + port + " " + param);
                }
                /* spawn all the spout thread on master */
                for(String key: spout_to.keySet()){
                    Vector<Vector> list = spout_to.get(key);
                    for(Vector inet: list){
                        Spout s = new Spout(jobname + "/" + key, (InetAddress)inet.get(0), (int)inet.get(1));
                        spout_list.add(s);
                        s.start();
                    }
                }

                int num_ack = 0;
                int num_fail = 0;
                while(num_ack < curr_job_in.keySet().size()){
                    try{
                        if(num_fail >= 2)
                            break;
                        Socket replySocket = sock.accept();
                        is = replySocket.getInputStream();
                        DataInputStream dis = new DataInputStream(is);
                        String msg = dis.readUTF();
                        is.close();
                        replySocket.close();
                        if(msg.equals("Done")){
                            num_ack++;
                        }
                        if(msg.equals("Fail")){
                            num_fail++;
                            System.out.println("got failure");
                            sock.setSoTimeout(10000);
                        }
                    }
                    catch (IOException e){
                        break;
                    }
                }
                sock.setSoTimeout(0);

                if(num_ack < curr_job_in.keySet().size()){
                    System.out.println("Failed node detected");
                    for(Thread t: spout_list){
                        System.out.println("Killing " + t);
                        while(t.isAlive()){
                            t.interrupt();
                        }
                    }
                    System.out.println("Spouts cleaned");
                    spout_list.clear();
                    Thread.sleep(10000
                    );
                    while(rerun() != 0){Thread.sleep(10000);}

                }
                spout_list.clear();
                curr_job = "";
                if(master.equals(ip_array.get(0))){
                    if(member_list.contains(InetAddress.getByName(ip_array.get(1)))){
                        Socket backup = new Socket(InetAddress.getByName(ip_array.get(1)), 5000);
                        DataOutputStream dos = new DataOutputStream(backup.getOutputStream());
                        dos.writeUTF("finished");
                        dos.close();
                        backup.close();
                    }
                }
                /* send reply message to client */
                Socket client = new Socket(InetAddress.getByName(ip_array.get(2)), 5000);
                DataOutputStream dos = new DataOutputStream(client.getOutputStream());
                dos.writeUTF("finished");
                dos.close();
                client.close();

                System.out.println("Job done.");
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }
}
