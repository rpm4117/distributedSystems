package com.company;

import java.net.*;
import java.io.*;
import java.util.*;

/* A SWIM-style failure detector with only direct pinging. */
public class Failure_detector_thread extends Thread{
    private Vector<InetAddress> member_list;
    private Vector ts_list;
    private DatagramSocket fd_socket;
    private FileWriter logfile;
    /* Failure detector constructor */
    public Failure_detector_thread(int T, Vector target, Vector target_ts, FileWriter fw) throws IOException{
        member_list = target;
        ts_list = target_ts;
        fd_socket = new DatagramSocket(3493);
        logfile = fw;
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
        DatagramPacket packet = new DatagramPacket(msg, msg.length, target, 3490);
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
                    packet = new DatagramPacket(msg, msg.length, target, 3490);
                    fd_socket.send(packet);
                    fd_socket.receive(packet);
                }
                catch(SocketTimeoutException ex){
                    synchronized (member_list){
                        System.out.println("Failure detected at: " + target);
                        ts_list.remove(member_list.indexOf(target));
                        member_list.remove(target);
                        System.out.println("Updated list: ");
                        for(int i = 0; i < member_list.size(); i++){
                            System.out.println("IP" + member_list.get(i)+ ", timestamp: " + ts_list.get(i));
                        }
                    }
                    synchronized (logfile){
                        logfile.append(target + " failed.\n");
                        logfile.flush();
                    }
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
    run()
    run function of failure detector thread. After every 200 ms, this thread
    choose a member randomly from list and ping it
    */
    public void run(){
        System.out.println("Initiating failure detector");
        Random rand = new Random();
        String ip = null;
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ip = socket.getLocalAddress().getHostAddress();
        }
        catch(SocketException e){}
        catch(UnknownHostException e){}
        /* Choose a random member to check for failure */
        while(true){
            if(member_list.size() == 0)
                continue;
            int index = rand.nextInt(member_list.size());
            InetAddress target = member_list.get(index);
//            System.out.println(index + " " +  member_list.size());
//            System.out.println(target.toString().equals("/" + ip));
            if(target != null && !target.toString().equals("/" + ip)){
                ping_target(target);
            }
            try{
                Thread.sleep(200);
            }
            catch(InterruptedException e){}
        }
    }
}
