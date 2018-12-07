package com.company;

import java.io.*;
import java.net.*;

/* Spout
 * Functionality: This thread is created as spout
 * that reads input file and sends tuples to other
 * bolts based on the topology
 */
public class Spout extends Thread{
    private File source_file;
    private Socket send_sock;

    public Spout(String file, InetAddress ip, int port){
        int flag = 0;
        while(flag == 0){
            try{
                send_sock = new Socket(ip, port);
                flag = 1;
            }
            catch (IOException e){flag = 0;}
        }
        System.out.println("connecting to: " + ip + " at: " + port);
        System.out.println("Opening source file: " + file + ".ds");
        source_file = new File(file + ".ds");

    }

    public void run(){
        try{
            System.out.println("Start sending " + source_file.getName());
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(source_file)));
            String line = reader.readLine();
            OutputStream os = send_sock.getOutputStream();
            //send out tuple one at a time
            while(line != null){
                try{
                    Thread.sleep(1);
                }
                catch (InterruptedException ie){
                    System.out.println("Ending spout");
                    send_sock.close();
                    return;
                }
//                if(Thread.interrupted()){
//                    System.out.println("Ending spout");
//                    send_sock.close();
//                    return;
//                }
                System.out.println(line);
                line += "\n";
                try{
                    os.write(line.getBytes());
                    os.flush();
                }
                catch (IOException e){
                    System.out.println("Ending spout");
                    send_sock.close();
                    return;
                }
                line = reader.readLine();
            }
            // if reached end of file, send out a string indicating end of stream
            String eos = "@END";
            os.write(eos.getBytes());
            System.out.println("Finished sending " + source_file.getName());
            os.close();
            send_sock.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}
