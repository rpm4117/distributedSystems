package com.company;

import java.net.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.*;

/* A SWIM-style failure detector with only direct pinging. */
public class Failure_detector_thread extends Thread{
    public List<String> ip_array;
    private Vector<InetAddress> member_list;
    private Vector ts_list;
    private DatagramSocket fd_socket;
    private FileWriter logfile;
    private Hashtable<String, ArrayList<Integer>> file_info;
    private String ip;
    private Clock clock;
    private long last_failure;
    /* Failure detector constructor */
    public Failure_detector_thread(int T, Vector target, Vector target_ts, Hashtable f_info, FileWriter fw) throws IOException{
        file_info = f_info;
        member_list = target;
        ts_list = target_ts;
        fd_socket = new DatagramSocket(3493);
        logfile = fw;
        clock = Clock.systemDefaultZone();
        last_failure = new Long(0);
        ip_array = Arrays.asList("/172.22.156.75", "/172.22.158.75", "/172.22.154.76", "/172.22.156.76", "/172.22.158.76",  //the list of iparrays of vms
                "/172.22.154.77", "/172.22.156.77", "/172.22.158.77", "/172.22.154.78", "/172.22.156.78");

        ip = null;
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ip = socket.getLocalAddress().getHostAddress();
        }
        catch(SocketException e){}
        catch(UnknownHostException e){}
    }

    public String get_own_ip(){
        String ret= "";
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ret = socket.getLocalAddress().getHostAddress();
        }
        catch (Exception e0){ e0.printStackTrace();}
        return ret;
    }

    /*
    ping_target(InetAddress target)
    This function takes in an address to ping, send the ping message
    and wait for pingack reply. If no reply received, send again and
    mark it as failure if still no reply.
    */
    public int ping_target(InetAddress target){
        //System.out.println(target + " replied: ");
        if(target == null){
            System.out.println("Target not valid.");
            return 1;
        }
        String ping_msg = new String("ping");
        byte[] msg = ping_msg.getBytes();
        DatagramPacket packet = new DatagramPacket(msg, msg.length, target, 3600);
        try{
            /* Send ping message */
            fd_socket.send(packet);
            fd_socket.setSoTimeout(100);
            /* Waiting for reply */
            msg = new byte[128];
            packet = new DatagramPacket(msg, msg.length);
            try{
                fd_socket.receive(packet);
            }
            catch(SocketTimeoutException e){
                /* In case we didn't receive response, suspect it by sending another ping */
                try{
                    Thread.sleep(1000);
                }
                catch(InterruptedException ie){}
                try{
                    packet = new DatagramPacket(msg, msg.length, target, 3600);
                    fd_socket.send(packet);
                    fd_socket.receive(packet);
                }
                catch(SocketTimeoutException ex){
                    synchronized (member_list){
                        System.out.println("Failure detected at: " + target + ", index: " + ip_array.indexOf(target.toString()));
                        ts_list.remove(member_list.indexOf(target));
                        member_list.remove(target);
                        /* update the file info */
                        for(String key: file_info.keySet()){
                            ArrayList<Integer> file_rep = file_info.get(key);
                            if(file_rep.contains(ip_array.indexOf(target.toString()))){
                                Integer rmv = new Integer(ip_array.indexOf(target.toString()));
                                System.out.println("removing: " + rmv + " in file: " + key);
                                file_rep.remove(rmv);
                            }
                        }
                        last_failure = clock.millis();
                        /* In case of worker, tell worker to stop all bolts */
                        /* In case of master, tell master to restart the job */
                        if(ip_array.indexOf("/" + get_own_ip()) != 2 && ip_array.indexOf("/" + get_own_ip()) != 1){
                            System.out.println("Declare emergency");
                            Socket reply_sock = new Socket(InetAddress.getByName(get_own_ip()), 5000);
                            reply_sock.setSoTimeout(200);
                            OutputStream os = reply_sock.getOutputStream();
                            DataOutputStream dos = new DataOutputStream(os);
                            dos.writeUTF("Fail");
                            os.close();
                            reply_sock.close();
                        }
                        /* For back-up master, once it becomes the only master, send failure message in case of failure */
                        if(ip_array.indexOf("/" + get_own_ip()) == 1
                                && !member_list.contains(InetAddress.getByName(ip_array.get(0).substring(1)))){
                            System.out.println("Declare emergency");
                            Socket reply_sock = new Socket(InetAddress.getByName(get_own_ip()), 5000);
                            reply_sock.setSoTimeout(200);
                            OutputStream os = reply_sock.getOutputStream();
                            DataOutputStream dos = new DataOutputStream(os);
                            dos.writeUTF("Fail");
                            os.close();
                            reply_sock.close();
                        }

                        if(ip_array.indexOf("/" + get_own_ip()) == 1
                                && target.equals(InetAddress.getByName(ip_array.get(0).substring(1)))){
                            System.out.println("Declare emergency");
                            Socket reply_sock = new Socket(InetAddress.getByName(get_own_ip()), 5000);
                            reply_sock.setSoTimeout(200);
                            OutputStream os = reply_sock.getOutputStream();
                            DataOutputStream dos = new DataOutputStream(os);
                            dos.writeUTF("masterfail");
                            os.close();
                            reply_sock.close();
                        }

                        System.out.println("Updated list: ");
                        for(int i = 0; i < member_list.size(); i++){
                            System.out.println("IP" + member_list.get(i)+ ", timestamp: " + ts_list.get(i));
                        }
                    }
                    synchronized (logfile){
                        logfile.append(target + " failed.\n");
                        logfile.flush();
                    }
                    return -1;
                }
            }

            String recved = new String(packet.getData(), 0,7);
            if(recved.equals("pingack")){
                return 0;
            }
            else
                return -1;
        }
        catch(IOException e){}
        return 0;
    }


    /*
     public int reroute_replica(String filename)
     public function of failure detector
     call this function upon failure
     reroute the replica to store the lost replica on the failed machine
     return the node to reroute to
     */

    public int reroute_replica(String filename){
        Hashtable<Integer, Integer> machine_state = new Hashtable();
        for(int i = 0; i < member_list.size(); i++)
            machine_state.put(ip_array.indexOf(member_list.get(i).toString()), 0);
        //System.out.println(file_info);
        for(String key: file_info.keySet()){
            ArrayList<Integer> list = file_info.get(key);
            for(int i = 0; i < list.size(); i++){
                int index = list.get(i);
                //System.out.println(index);
                int orig = machine_state.get(index);
                machine_state.replace(list.get(i), orig + 1);
            }
        }

        int min_len = -1;
        int node = -1;
        for(int key: machine_state.keySet()){
            if(min_len == -1 || machine_state.get(key) < min_len){
                if(!file_info.get(filename).contains(key)){
                    min_len = machine_state.get(key);
                    node = key;
                }
            }
        }
//        ArrayList list = file_info.get(filename);
//        list.add(node);
        System.out.println("reroute to: " + node);
        return node;
    }
/*
    public String pack_file_info()
    public function of the failure detector
    pack up the file info data structure to a string
    in order to prepare to send out the fileinfo data structure
    file info data structure is how master knows of the file system, so it is vital that the data structure is available
    so that when master fails, other nodes can proceed to be master
   */
    public String pack_file_info(){
        byte[] empty = new byte[0];
        String pack = new String(empty, StandardCharsets.UTF_8);
        pack += "fi/";
        for(String key: file_info.keySet()){
            pack += key;
            pack += ",";
            int i;
            for(i = 0; i < file_info.get(key).size(); i++){
                pack += Integer.toString(file_info.get(key).get(i));
            }
            pack = pack.replace("\0", "");
            pack += "/";
        }
        //System.out.println(pack);
        return pack;
    }

    /*
    run()
    run function of failure detector thread. After every 200 ms, this thread
    choose a member randomly from list and ping it
    */
    public void run(){
        System.out.println("Initiating failure detector");
        Random rand = new Random();
        /* Choose a random member to check for failure */
        while(true){
            if(member_list.size() == 0)
                continue;
            int index = rand.nextInt(member_list.size());
            InetAddress target = member_list.get(index);
            if(member_list.get(0).toString().equals("/" + ip) && !(last_failure == 0) && (clock.millis() > last_failure + 20000)){
                if((member_list.size() < 4) || (file_info.size() == 0)){
                    last_failure = new Long(0);
                    continue;
                }
                Clock clock = Clock.systemDefaultZone();
                long start = clock.millis();
                System.out.println("begin replication routine");
                System.out.println("Current file info list:");
                System.out.println(file_info);
                try{
                    last_failure = new Long(0);
                    /* send the replica request to the non-failure node */
                    for(String key: file_info.keySet()){
                        ArrayList<Integer> file_rep = file_info.get(key);
                        if(file_rep.size() < 4){
                            int i = 0;
                            int num_rep = 4 - file_rep.size();
                            System.out.println("need " + num_rep + "replicas");
                            while(i < num_rep){
                                int rep_index = reroute_replica(key);
                                String tosend = "rerep " + key + " " + Integer.toString(rep_index);
                                byte[] sending_msg = tosend.getBytes(StandardCharsets.UTF_8);
                                InetAddress rep_target = InetAddress.getByName(ip_array.get(file_rep.get(0)).substring(1));
                                DatagramPacket sending = new DatagramPacket(sending_msg, sending_msg.length, rep_target, 3490);
                                fd_socket.send(sending);
                                byte[] msg = new byte[5];
                                DatagramPacket packet = new DatagramPacket(msg, msg.length);
                                try{
                                    fd_socket.setSoTimeout(5000);
                                    fd_socket.receive(packet);
                                }
                                catch (IOException ex){
                                    System.out.println("didn't receive confirm");
                                    continue;
                                }
                                if(new String(packet.getData()).substring(0,5).equals("reped")){
                                    i++;
                                    file_info.get(key).add(rep_index);
                                }
                                else
                                    continue;
                            }
                        }
                    }
                    String tosend = pack_file_info();
                    //System.out.println(tosend);
                    byte[] sending_msg = tosend.getBytes(StandardCharsets.UTF_8);

                    for(int i = 0; i < member_list.size(); i++){
                        DatagramPacket sending = new DatagramPacket(sending_msg, sending_msg.length, member_list.get(i), 3490);
                        fd_socket.send(sending);
                    }
                    long end = clock.millis();
                    System.out.println("Re-replication done: " + (end - start) + " ms");
                }
                catch(Exception e){}
            }
            if(target != null && !target.toString().equals("/" + ip)){
                ping_target(target);
            }
            try{
                Thread.sleep(400);
            }
            catch(InterruptedException e){}
        }
    }
}
