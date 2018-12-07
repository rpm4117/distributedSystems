package com.company;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.Vector;

/*
main function
handles the two threads
offers them empty mebership list to the constructor
 */

public class Main {
    public static void main(String[] args) throws IOException{

        //System.out.println(serverAddr);
        Vector member_list = new Vector();
        Vector ts_list = new Vector();
        String filename = "vm" + Integer.toString(Integer.parseInt(args[0]) + 1) + ".log";
        File f = new File(filename);
        FileWriter fw = new FileWriter(f);
        Clock clk = Clock.systemDefaultZone();
        //in_group[0] = args[0].equals("1") ? true : false;
        try {
            if (args[0].charAt(0) == '0') {
                member_list.addElement(InetAddress.getByName("172.22.156.75"));
                ts_list.add(clk.millis());
            }
        }catch(UnknownHostException u){}
        try{
            Server_thread s1 = new Server_thread(3490, member_list, ts_list, fw);
            Client_thread c1 = new Client_thread(member_list, ts_list);
            Failure_detector_thread fd = new Failure_detector_thread(200, member_list, ts_list, fw);
            s1.start();
            c1.start();
            fd.start();
        }
        catch(IOException e){}
    }
}
