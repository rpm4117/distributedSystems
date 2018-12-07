package com.company;

import java.net.*;
import java.io.*;
import java.util.*;
import java.util.Random;
import java.time.Clock;

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

    /*
    Server_thread(int port, Vector target, Vector target_ts, FileWriter fw)
    the constructor of the server thread
    initialize the member_list from call in main function
    initialize everything
    */

    public Server_thread(int port, Vector target, Vector target_ts, FileWriter fw) throws IOException {
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
                byte[] msg = new byte[128];
                DatagramPacket packet = new DatagramPacket(msg, msg.length);
                serverSocket.receive(packet);
                InetAddress clientAddress = packet.getAddress();         //get the address of the sender
                int port = packet.getPort();
                int flag = 0;
                String recved = new String(packet.getData(), 0, 25);
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
                        try{
                            Thread.sleep(5);        //sleep a bit to reduce the bandwidth
                        }
                        catch(InterruptedException e){}
                    }
                    continue;
                }
                if(shorts.equals("join") ||shorts.equals("leav")){   //plain join and leave
                    long timestamp = clk.millis();
                    String ts = Long.toString(timestamp);
                    if(shorts.equals("join")){
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
                        try{
                            Thread.sleep(5);                        //sleep to decrease bandwidth
                        }
                        catch(InterruptedException e){}
                    }
                }
                if(shorts.equals("ping")){                                //if ping, reply with pingack
                    String str = "pingack";
                    byte[] sending_msg = str.getBytes();
                    DatagramPacket sending = new DatagramPacket(sending_msg, sending_msg.length, clientAddress, port);
                    serverSocket.send(sending);

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
