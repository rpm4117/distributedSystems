package com.company;

import java.net.*;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Random;
import java.time.Clock;
import java.util.Hashtable;

//server thread class for the system
//does the gossiping in the dissemination process
//writes to log files
//sends the ack signal

public class Server_thread extends Thread{
    public List<String> ip_array;
    private Vector<InetAddress> member_list;      //the membership list
    private Vector ts_list;                       //the timestamp list, corresponsding to membership list
    private Vector gossiped_list;                //the already gossiped messages
    private DatagramSocket serverSocket;
    private DatagramSocket gossipSocket;
    private Clock clk;
    private FileWriter logfile;                   //the logfile
    private Hashtable<String, ArrayList<Integer>> file_info;
    private Hashtable<String, ArrayList<Long>> vdict;


    /*
    Server_thread(int port, Vector target, Vector target_ts, FileWriter fw)
    the constructor of the server thread
    initialize the member_list from call in main function
    initialize everything
    */

    public Server_thread(int port, Vector target, Vector target_ts, FileWriter fw, Hashtable f_info, Hashtable verdict) throws IOException {
        file_info = f_info;
        vdict = verdict;
        clk = Clock.systemDefaultZone();
        serverSocket = new DatagramSocket(port);
        gossipSocket = new DatagramSocket(3492);
        member_list = target;
        ts_list = target_ts;
        logfile = fw;
        gossiped_list = new Vector();
        ip_array = Arrays.asList("/172.22.156.75", "/172.22.158.75", "/172.22.154.76", "/172.22.156.76", "/172.22.158.76",  //the list of iparrays of vms
                                 "/172.22.154.77", "/172.22.156.77", "/172.22.158.77", "/172.22.154.78", "/172.22.156.78");

    }

    /*

    String parseString(String S, String ip, long timestamp)
    process the plain join and leave messages
    add timestamp and ipaddress
    to prepare for gossiping
    */

    public String parseString(String S, String ip, long timestamp){
        //long timestamp = clk.millis();
        String ts = Long.toString(timestamp);
        int index = ip_array.indexOf(ip);
        String ip_index = Integer.toString(index);
        if (S.charAt(0) == 'j') {
            return "ga" + ip_index  + ts;   //ga stands for add, one char index, followed by timestamp
        }
        else{
            return "gs" + ip_index  + ts;   //gs stands for leave, one char index, followed by timestamp
        }
    }

    /*
    int gossip(String S)
    gossips the message
    picks a random machine from the member list
    send the string to that machine
    */

    public int gossip(String S){
        try {
            int length = member_list.size();
            if (length <= 0){
                return -1;    //refuse to gossip in not in list
            }
            Random generator = new Random();
            int rnd = generator.nextInt(length);
            InetAddress lucky = member_list.get(rnd);
            byte[] gossip_message = S.getBytes();

            DatagramPacket gossip = new DatagramPacket(gossip_message, gossip_message.length, lucky, 3490);

            gossipSocket.send(gossip);
            return 0;
        }
        catch(IOException e){
            e.printStackTrace();
            return 0;
        }
    }


    /*
    public String pack_file_info()
    pack up the fileinfo
    to a string, in order to send out
    we need fileinfo to know about the filesystem
    useful when master is down
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
    unpack_fileinfo
    unpack the sent string of file info
    put them into the class object file_info
    useful when receiving the file_info
     */

    public void unpack_file_info(String recved){
        String recved_fi = recved.substring(2);
        recved_fi = recved_fi.replace("\0", "");
        //System.out.println(recved_fi.length() + "recved fi: " + recved_fi);
        file_info.clear();
        for(int i = 0; i < recved_fi.length(); i++){
            if(recved_fi.charAt(i) == '/' && i != recved_fi.length() - 1){
                String fn = "";
                ArrayList al = new ArrayList();
                int dig = -1;
                for(int j = i + 1; j < recved_fi.length(); j++){
                    if(recved_fi.charAt(j) == ','){
                        fn = recved_fi.substring(i + 1, j);
                        dig = j + 1;
                        continue;
                    }
                    if(dig != -1){
                        if(recved_fi.charAt(j) == '/')
                            break;
                        al.add(Integer.parseInt(String.valueOf(recved_fi.charAt(j))));
                    }
                }
                file_info.put(fn, al);
            }
        }
    }

    // public String routing_replica(int num_replica, String filename)
    //routing to a replica
    //using the number of replicas and the filename
    //find the replicas of the least load
    //and return the node numbers as a string
    public String routing_replica(int num_replica, String filename){
        /* get count of replicas stored on each machine */
        Hashtable<Integer, Integer> machine_state = new Hashtable();
        for(int i = 0; i < member_list.size(); i++)
            machine_state.put(ip_array.indexOf(member_list.get(i).toString()), 0);
        for(String key: file_info.keySet()){
            ArrayList<Integer> list = file_info.get(key);
            for(int i = 0; i < list.size(); i++){
                int orig = machine_state.get(list.get(i));
                machine_state.replace(list.get(i), orig + 1);
            }
        }

        ArrayList list = new ArrayList();
        String ret = "";

        while(ret.length() < num_replica){
            int min_len = -1;
            int node = -1;
            for(int key: machine_state.keySet()){
                if(min_len == -1 || machine_state.get(key) < min_len){
                    min_len = machine_state.get(key);
                    node = key;
                }
            }
            ret += Integer.toString(node);
            machine_state.remove(node);
            list.add(node);
        }
        //System.out.println("route to: " + ret);
        file_info.put(filename, list);
        return ret;
    }



    /*
    run()
    run function of server thread
    receives the message from other vms
    parse the message and decide what to do
    message could include: join plain messages, gossip messages, or ping messages
    in charge of calling gossip function when needed
    */

    public void run(){
        System.out.println("Start server thread...");
        while(true){
            try{
                byte[] msg = new byte[200];
                DatagramPacket packet = new DatagramPacket(msg, msg.length);
                serverSocket.receive(packet);
                InetAddress clientAddress = packet.getAddress();         //get the address of the sender
                int port = packet.getPort();
                int flag;
                String recved = new String(packet.getData(), StandardCharsets.UTF_8);
                //System.out.println(recved);

                if(recved.split("\\s+")[0].equals("get_rep")){     //get_replica command
                    recved = recved.replace("\0", "");
                    String sdfsname = recved.split("\\s+")[1];
                    file_sender fs = new file_sender(clientAddress, "./SDFS/" + sdfsname, sdfsname, 3494);
                    fs.flag = 0;
                    fs.start();
                    continue;
                }
                /* get-versions sdfsfilename numversions localfilename */
                if(recved.substring(0,3).equals("ver")){            //ver message, which is generated by get-versions
                    String numstr = recved.substring(recved.indexOf('\n')+1, recved.indexOf('\0'));
                    int ver_num = Integer.parseInt(numstr);
                    String fileToGet = recved.substring(3,recved.indexOf('\n'));
                    System.out.println("File to get: " + fileToGet);

                    if(vdict.containsKey(fileToGet)){
                        ArrayList<Long> list = vdict.get(fileToGet);
                        if(ver_num > list.size()){System.out.println("too many versions");continue;}
                        ArrayList<String> filelist = new ArrayList();
                        for(int i = list.size()-ver_num; i<list.size();i++){
                            String local_addr = "./SDFS/"+fileToGet+"d/"+fileToGet+Long.toString(list.get(i));
                            filelist.add(local_addr);
                        }
                        System.out.println(filelist.toString());
                        file_sender fs = new file_sender(clientAddress, 3496, filelist, clientAddress);
                        fs.flag = 1;
                        fs.start();

                    }


                    continue;
                }

                /* write format: put localname sdfsname */
                if(recved.split("\\s+")[0].equals("put")){    //put command
                    Long timestamp = clk.millis();
                    /* update file info table and distribute */
                    recved = recved.replace("\0", "");
                    String localname = recved.split("\\s+")[1];
                    String sdfsname = recved.split("\\s+")[2];
                    /*  In case of new file */
                    if(!file_info.containsKey(sdfsname)){
                        System.out.println("New file: " + sdfsname);
                        String reply;
                        if(member_list.size() >= 4)
                            reply = routing_replica(4, sdfsname);
                        else
                            reply = routing_replica(member_list.size(), sdfsname);
                        reply = reply + 'n' + timestamp.toString();
                        //System.out.println("the string is"+reply);
                        byte[] rb = reply.getBytes();
                        packet = new DatagramPacket(rb, rb.length, clientAddress, port);
                        serverSocket.send(packet);

                        /* store 4 replicates */
                        //System.out.println(file_info);
                        String tosend = pack_file_info();
                        //System.out.println(tosend);
                        byte[] sending_msg = tosend.getBytes(StandardCharsets.UTF_8);
                        synchronized (member_list){
                            for(int i = 0; i < member_list.size(); i++){
                                DatagramPacket sending = new DatagramPacket(sending_msg, sending_msg.length, member_list.get(i), 3490);
                                serverSocket.send(sending);
                            }
                        }
                        continue;
                    }
                    /* if the file already exists */
                    else{
                        String rep_str = "";
                        ArrayList<Integer> rep_list = file_info.get(sdfsname);
                        if (rep_list == null){
                            rep_str = "-1";
                        }
                        else{
                            System.out.println("Writing to existed file: " + sdfsname);
                            for(int i = 0; i < rep_list.size(); i++){
                                rep_str += Integer.toString(rep_list.get(i));
                            }
                        }
                        rep_str = rep_str + 'n'+ Long.toString(timestamp);
                        //System.out.println("the string is"+rep_str);
                        byte[] rb = rep_str.getBytes();
                        packet = new DatagramPacket(rb, rb.length, clientAddress, port);
                        serverSocket.send(packet);
                        continue;
                    }
                }

                /* read format: get sdfsname localname */
                if(recved.split("\\s+")[0].equals("get")){
                    recved = recved.replace("\0", "");
                    String sdfsname = recved.split("\\s+")[1];
                    String localname = recved.split("\\s+")[2];
                    System.out.println("Reading file: " + sdfsname);
                    String rep_str = "";
                    ArrayList<Integer> rep_list = file_info.get(sdfsname);
                    if (rep_list == null){
                        rep_str = "-1";
                        System.out.println("File doesn't exist.");
                    }
                    else{
                        for(int i = 0; i < rep_list.size(); i++){
                            rep_str += Integer.toString(rep_list.get(i));
                        }
                    }
                    byte[] rb = rep_str.getBytes();
                    packet = new DatagramPacket(rb, rb.length, clientAddress, port);
                    serverSocket.send(packet);
                    continue;
                }

                /* in case of receiving file info list */
                if(recved.substring(0,2).equals("fi")){
                    unpack_file_info(recved);
                    continue;
                }

                /* delete format: delete sdfsname */
                if(recved.split("\\s+")[0].equals("delete")){
                    recved = recved.replace("\0", "");
                    String sdfsname = recved.split("\\s+")[1];
                    System.out.println("Deleting file: " + sdfsname);
                    ArrayList<Integer> rep_list = file_info.get(sdfsname);
                    if(rep_list == null){
                        System.out.println("File doesn't exist");
                        continue;
                    }
                    /* tell all the nodes storing the corresponding replicas to remove the replica */
                    String tosend = "del-master " + sdfsname;
                    System.out.println(rep_list + tosend);
                    byte[] sending_msg = tosend.getBytes(StandardCharsets.UTF_8);
                    for(int i = 0; i < rep_list.size(); i++){
                        System.out.println("sending delete request to " + ip_array.get(rep_list.get(i)).substring(1));
                        DatagramPacket sending = new DatagramPacket(sending_msg, sending_msg.length, InetAddress.getByName(ip_array.get(rep_list.get(i)).substring(1)), 3490);
                        serverSocket.send(sending);
                    }
                    /* update the fileinfo list on every node */
                    file_info.remove(sdfsname);
                    tosend = pack_file_info();
                    //System.out.println(tosend);
                    sending_msg = tosend.getBytes(StandardCharsets.UTF_8);
                    synchronized (member_list){
                        for(int i = 0; i < member_list.size(); i++){
                            DatagramPacket sending = new DatagramPacket(sending_msg, sending_msg.length, member_list.get(i), 3490);
                            serverSocket.send(sending);
                        }
                    }
                    continue;
                }
                /* deal with delete request */
                if(recved.split("\\s+")[0].equals("del-master")){        //delete command
                    recved = recved.replace("\0", "");
                    String sdfsname = recved.split("\\s+")[1];
                    /* Delete the stored versions of this file */
                    File del_directory = new File("./SDFS/" + sdfsname + "d");
                    String[] entries = del_directory.list();
                    for(String s: entries){
                        File currentFile = new File(del_directory.getPath(),s);
                        currentFile.delete();
                    }
                    del_directory.delete();
                    File del_file = new File("./SDFS/" + sdfsname);
                    del_file.delete();
                    vdict.remove(sdfsname);
                    System.out.println(sdfsname + " has been deleted.");
                    continue;
                }

                if(recved.split("\\s+")[0].equals("rerep")){            //re rep command, used in failure
                    recved = recved.replace("\0", "");
                    String filename = recved.split("\\s+")[1];
                    Integer target = Integer.parseInt(recved.split("\\s+")[2]);
                    System.out.println("Received rereplica request of file: " + filename + " to " + target);

                    ArrayList<Long> list = vdict.get(filename);
                    System.out.println(list);
                    ArrayList<String> filelist = new ArrayList();
                    int initial = list.size() > 5 ? list.size() - 5 : 0;
                    for(int i = initial; i < list.size(); i++){
                        String local_addr = "./SDFS/" + filename + "d/" + filename + Long.toString(list.get(i));
                        filelist.add(local_addr);
                    }
                    System.out.println(filelist.toString());
                    file_sender fs = new file_sender(InetAddress.getByName(ip_array.get(target).substring(1)), 3500, filelist, clientAddress);
                    fs.flag = 2;
                    fs.start();
                    continue;
                }



                flag = gossiped_list.indexOf(recved);         //find out if message has been gossiped, if so , refuse to gossip
                if(flag != -1 ){
                    continue;
                }
                String  shorts= recved.substring(0,4);
                if (recved.charAt(4) != '\u0000'&&recved.charAt(0)=='g'){        //this message is not an introduce message, it has been gossiped

                    char dig = recved.charAt(2);
                    int index = Character.getNumericValue(dig);     //get our inetaddress from the string

                    if(index < 0 || index > 9)
                        continue;

                    String ip_string = ip_array.get(index).substring(1);
                    String ts = new String(packet.getData(), 3, 13);
                    InetAddress gossip_ip = InetAddress.getByName(ip_string);   //ip decoded from gossip string
                    synchronized (member_list){
                        if (recved.charAt(1) == 'a' &&!member_list.contains(gossip_ip)){   //id gossip message is join

                            member_list.addElement(gossip_ip);
                            ts_list.addElement(ts);
                            synchronized (logfile){
                                logfile.append(gossip_ip + " joined.\n");
                                logfile.flush();
                            }
                        }
                        else if (recved.charAt(1)== 's'  &&member_list.contains(gossip_ip)){   //if gossip message is leave

                            int rmindex = member_list.indexOf(gossip_ip);
                            member_list.remove(rmindex);
                            ts_list.remove(rmindex);
                            synchronized (logfile){
                                logfile.append(gossip_ip + " left.\n");
                                logfile.flush();
                            }
                        }
                    }
                    gossiped_list.addElement(recved);
                    for(int c = 0; c < 5; c++){          //the gossiping it self
                        gossip(recved);
                    }
                    continue;
                }
                if(shorts.equals("join") ||shorts.equals("leav")){   //plain join and leave
                    long timestamp = clk.millis();
                    String ts = Long.toString(timestamp);
                    if(shorts.equals("join")){
                        if(member_list.size() == 0){
                            byte[] sending_msg = "notin".getBytes();
                            DatagramPacket sending = new DatagramPacket(sending_msg, sending_msg.length, clientAddress, port);
                            serverSocket.send(sending);
                            continue;
                        }
                        synchronized (member_list){
                            if(!member_list.contains(clientAddress)){
                                System.out.println("Node at " + clientAddress + " will join.");
                                member_list.add(clientAddress);
                                ts_list.add(timestamp);
                                synchronized (logfile){
                                    logfile.append(clientAddress + " joined.\n");
                                    logfile.flush();
                                }
                                String tosend = new String();                 //replay messages include timestamp string and membership list
                                for(int i = 0; i< member_list.size(); i++){
                                    String temp = member_list.get(i).toString();
                                    int index = ip_array.indexOf(temp);
                                    tosend += Integer.toString(index);
                                }
                                tosend += "t";
                                for(int i = 0; i< ts_list.size(); i++){
                                    String temp = ts_list.get(i).toString();
                                    tosend += temp;
                                }
                                tosend = tosend+ "l" + ts;              //add the ts string to sent message

                                byte[] sending_msg = tosend.getBytes();
                                DatagramPacket sending = new DatagramPacket(sending_msg, sending_msg.length, clientAddress, port);
                                serverSocket.send(sending);
                                /* In case of rejoin */
                                if(!file_info.isEmpty()){
                                    tosend = pack_file_info();
                                    //System.out.println(tosend);
                                    sending_msg = tosend.getBytes(StandardCharsets.UTF_8);
                                    for(int i = 0; i < member_list.size(); i++){
                                        sending = new DatagramPacket(sending_msg, sending_msg.length, member_list.get(i), 3490);
                                        serverSocket.send(sending);
                                    }
                                }
                            }
                        }
                    }
                    if(shorts.equals("leav")){
                        synchronized (member_list){
                            if(member_list.contains(clientAddress)){       //if leav gossip, we do not need to send back
                                System.out.println("Node at " + clientAddress + " will leave.");
                                int index = member_list.indexOf(clientAddress);
                                member_list.remove(index);
                                ts_list.remove(index);
                                synchronized (logfile){
                                    logfile.append(clientAddress + " left.\n");
                                    logfile.flush();
                                }
                            }
                        }
                    }

                    String gossip_message =parseString(recved, clientAddress.toString(), timestamp);  //introduce message
                    gossiped_list.addElement(gossip_message);
                    for(int i=0; i<5; i++) {
                        gossip(gossip_message);

                    }
                }

            }
            catch(SocketTimeoutException s){
                System.out.println("Timeout exception");
                break;
            }catch(IOException e){
                System.out.println("IO exception");
                e.printStackTrace();
                break;
            }
        }
    }

}
