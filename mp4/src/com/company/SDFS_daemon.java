package com.company;

import java.io.*;
import java.net.*;
import java.time.Clock;
import java.util.*;

/* SDFS_daemon
 * Functionality: This thread is created to receive
 * SDFS command from server threads, without input
 * from keyboard
 */
public class SDFS_daemon extends Thread {
    private ServerSocket sock;
    private DatagramSocket client;
    private Vector<InetAddress> member_list; //membership list
    private Hashtable<String, ArrayList<Long>> vdict;
    public String[] ip_array;

    public SDFS_daemon(Vector target, Hashtable version_dict) throws IOException {
        member_list = target;
        client = new DatagramSocket(7000);
        sock = new ServerSocket(6000);
        ip_array = new String[]{"172.22.156.75", "172.22.158.75", "172.22.154.76", "172.22.156.76", "172.22.158.76",
                                "172.22.154.77", "172.22.156.77", "172.22.158.77", "172.22.154.78", "172.22.156.78"};
        vdict = version_dict;
    }

    public void run(){
        while(true){
            try{
                Socket requestsocket = sock.accept();
                InputStream is = requestsocket.getInputStream();
                DataInputStream clientData = new DataInputStream(is);
                String cmd = clientData.readUTF();
                String op = cmd.split("\\s+")[0];
                if(op.equals("put") || op.equals("get")){
                    InetAddress serverAddr = member_list.get(0);        //route message to master(first on the list)
                    System.out.println("sending file op to " + serverAddr + " port number " + 3490);
                    byte[] msg = cmd.getBytes();
                    DatagramPacket packet = new DatagramPacket(msg, msg.length, serverAddr, 3490);
                    client.send(packet);
                    if(op.equals("put")){                          //put command of filesystem
                        Clock clock = Clock.systemDefaultZone();
                        long start = clock.millis();
                        byte[] reply = new byte[25];
                        packet = new DatagramPacket(reply, reply.length);
                        client.receive(packet);
                        String rep = new String(packet.getData());
                        rep = rep.replace("\0", "");

                        String localname = cmd.split("\\s+")[1];
                        String sdfsname = cmd.split("\\s+")[2];

                        String tss = rep.substring(rep.indexOf('n')+1);
                        long ts = Long.parseLong(tss);
                        System.out.println("rep: "+ rep);

                        for(int i = 0; i < rep.indexOf('n'); i++){
                            //System.out.println("Line" + i + " has " + rep.charAt(i));
                            InetAddress target = InetAddress.getByName(ip_array[Character.getNumericValue(rep.charAt(i))]);
                            file_sender fs = new file_sender(target, localname, sdfsname, 3495,ts);
                            fs.sendts();
                        }
                        long end = clock.millis();
                        System.out.println("writing done: " + (end - start) + " ms");
                    }
                    if(op.equals("get")){                                          //get command of filesystem
                        Clock clock = Clock.systemDefaultZone();
                        long start = clock.millis();
                        byte[] reply = new byte[10];
                        packet = new DatagramPacket(reply, reply.length);
                        client.receive(packet);
                        String rep = new String(packet.getData());
                        rep = rep.replace("\0", "");

                        String sdfsname = cmd.split("\\s+")[1];
                        String localname = cmd.split("\\s+")[2];
                        int num_rep = member_list.size() >= 4 ? 4 : member_list.size();
                        //System.out.println(rep);
                        if(rep.equals("-1")){
                            System.out.println("File doesn't exist.");
                            continue;
                        }
                        receiver_thread recv = new receiver_thread(localname, 3494, vdict, member_list);
                        //for(int i = 0; i < rep.length(); i++){
                        //System.out.println("Receiving from " + rep.charAt(i));
                        InetAddress target = InetAddress.getByName(ip_array[Character.getNumericValue(rep.charAt(0))]);
                        String sending = "get_rep " + sdfsname;
                        msg = sending.getBytes();
                        packet = new DatagramPacket(msg, msg.length, target, 3490);
                        client.send(packet);
                        recv.read_receive();
                        //System.out.println("Received from " + rep.charAt(i));
                        //}
                        recv.close();
                        long end = clock.millis();
                        System.out.println("reading done: " + (end - start) + " ms");
                    }
                    continue;
                }
                requestsocket.close();
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }
    }
}
