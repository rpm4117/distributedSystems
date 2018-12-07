package com.company;

import java.net.*;
import java.io.*;
import java.time.Clock;
import java.util.Vector;

/*
this is the client thread
this waits for user input and send messages to other machines
it detects the membership list when client
 */

public class Client_thread extends Thread{
    private Vector<InetAddress> member_list; //membership list
    private Vector<Long> ts_list;            //timestamp list
    private DatagramSocket client;
    private InetAddress serverAddr;
    private Clock clk;
    public String[] ip_array;
    private long timestamp;

/*
Client_thread(Vector target, Vector target_ts)
this is the client thread contructor
initializes the introducer IP address
 */
    public Client_thread(Vector target, Vector target_ts) throws IOException{
        clk = Clock.systemDefaultZone();
        member_list = target;
        ts_list = target_ts;
        timestamp = ts_list.size() > 0 ? ts_list.get(0) : 0;
        int port = 3491;
        client = new DatagramSocket(port);
        ip_array = new String[]{"172.22.156.75", "172.22.158.75", "172.22.154.76", "172.22.156.76", "172.22.158.76",
                "172.22.154.77", "172.22.156.77", "172.22.158.77", "172.22.154.78", "172.22.156.78"};
        try{
            serverAddr = InetAddress.getByName("172.22.156.75"); //vm1 ip address
        }
        catch (UnknownHostException ex) {
            System.out.print("unknown exception");
        }
    }

    /*
    run()
    run function of client thread
    detects user command line input
    sends the raw join/leave messages to other vms
     */

    public void run(){
        while(true){
            try{

                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                String kbInput = br.readLine();

                synchronized (member_list){            //list command, print out membership list
                    if(kbInput.equals("list")){
                        if(member_list.size()!=0){
                            System.out.println("List: ");
                            for(int i = 0; i < member_list.size(); i++){
                                System.out.println("IP: " + member_list.get(i) + ", timestamp: " + ts_list.get(i));
                            }
                            continue;
                        }
                        else{
                            System.out.println("Not in group");
                            continue;
                        }
                    }
                }

                if(kbInput.equals("id")){               //command line for showing id
                    if(member_list.size()!=0){
                        try(final DatagramSocket socket = new DatagramSocket()){
                            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
                            String ip = socket.getLocalAddress().getHostAddress();
                            System.out.println("IP address: " + ip);
                        }
                        System.out.println("Time stamp: " + timestamp);
                        continue;
                    }
                    else{
                        System.out.println("Not in group");
                        continue;
                    }
                }


                String ip = null;                      //get your own ip address
                try(final DatagramSocket socket = new DatagramSocket()){
                    socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
                    ip = socket.getLocalAddress().getHostAddress();
                }
                catch(SocketException e){}
                catch(UnknownHostException e){}

                if(kbInput.equals("join")){
                    serverAddr = InetAddress.getByName("172.22.156.75");
                }

                if(kbInput.equals("leav") && member_list.size()==2){                //when list is 2, use the address that is not your own ip
                    String list0 = member_list.get(0).getHostAddress();
                    if(!list0.equals(ip)){
                        serverAddr =member_list.get(0);
                    }
                    else{
                        serverAddr =member_list.get(1);
                    }
                }
                if(kbInput.equals("leav") && member_list.size()>2){                 //tell leave from what machine when list is bigger than 2
                    String list0 = member_list.get(0).getHostAddress();              //avoid failing to leave when introducer is down
                    String list1 = member_list.get(1).getHostAddress();
                    if(!list0.equals(ip) && !list0.equals("172.22.156.75")){
                        serverAddr = member_list.get(0);
                    }

                    else if(!list1.equals(ip) && !list1.equals("172.22.156.75")){
                        serverAddr = member_list.get(1);
                    }
                    else{
                        serverAddr = member_list.get(2);
                    }
                }

                //code just changed
                byte[] msg = kbInput.getBytes();
                System.out.println("sending to " +serverAddr + " port number " + 3490);
                DatagramPacket packet = new DatagramPacket(msg, msg.length, serverAddr, 3490);

                client.send(packet);

                if(kbInput.equals("leav")){    //type in leave, flush your membership list
                    member_list.clear();
                    ts_list.clear();
                }

                client.setSoTimeout(1000);
                /* Start receiving response from client */
                synchronized (member_list){
                    if(kbInput.equals("join") && member_list.size()==0){         //only receives stuff in case of joining
                        byte[] reply = new byte[161];                                       //check if size is 0 to avoid bad request
                        packet = new DatagramPacket(reply, reply.length);
                        client.receive(packet);
                        String recved = new String(packet.getData(), 0,160);
//                        System.out.println(recved);
                        int lindex = recved.indexOf('l');
                        int tindex = recved.indexOf('t');
                        for(int i = 0; i <tindex; i++){
                            char dig = recved.charAt(i);
                            if(Character.isDigit(dig)) {                           //tell if is digit
                                int index = Character.getNumericValue(recved.charAt(i));
                                String ip_string = ip_array[index];
                                member_list.addElement(InetAddress.getByName(ip_string));      //add the element
                            }
                            else{
                                break;
                            }
                        }
                        for(int i = tindex + 1; i < lindex; i += 13){               //get the timestamp list elements
                            String ts = new String();
                            for(int j = i; j < i + 13; j++){
                                char dig = recved.charAt(j);
                                if(Character.isDigit(dig)){
                                    ts += dig;
                                }
                                else{
                                    break;
                                }
                            }
                            ts_list.addElement(Long.valueOf(ts));
//
                        }
                        String longs = "";       //get your own timestamp string from the server
                        for(int i = lindex+1; i<recved.length(); i++){
                            char dig = recved.charAt(i);
                            if(Character.isDigit(dig)){
                                longs+=dig;
                            }
                            else{
                                break;
                            }
                        }

                        timestamp = Long.valueOf(longs);  //give timestamp value

                    }
                }
            }
            catch(IOException e){
                System.out.println("No response");
            }
        }
    }
}
