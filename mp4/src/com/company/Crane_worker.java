package com.company;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/* Crane_worker
 * Functionality: crane worker receives command from
 * crane master and create bolts that meets the
 * requirements.
 */
public class Crane_worker extends Thread {
    private ServerSocket socket;
    private Socket ClientSocket;
    private ArrayList<Thread> bolt_list;
    private InetAddress master;

    public Crane_worker() throws IOException {
        System.out.println("Worker online");
        socket = new ServerSocket(5000);
        bolt_list = new ArrayList<>();
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

    public void run() {
        while (true) {
            try {
                ClientSocket = socket.accept();
                InputStream in = ClientSocket.getInputStream();
                DataInputStream clientData = new DataInputStream(in);
                String bolt_info = clientData.readUTF();

                if(ClientSocket.getInetAddress().equals(InetAddress.getByName(get_own_ip()))){
                    System.out.println("Failed node detected");
                    if(bolt_info.equals("Fail")){
                        for(Thread t: bolt_list){
                            System.out.println("Killing " + t);
                            while(t.isAlive()){
                                t.interrupt();
                            }
                        }
                        System.out.println("Bolts cleaned");
                        bolt_list.clear();
                    }
                    ClientSocket.close();
                    continue;
                }

                master = ClientSocket.getInetAddress();
                System.out.println("master is: " + master);
                String bolt_name = bolt_info.split("\\s+")[0];
                OutputStream output = new FileOutputStream(bolt_name + ".cpp");
                BufferedOutputStream out = new BufferedOutputStream(output);
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
                out.flush();
                // Closing the FileOutputStream handle
                out.close();
                in.close();
                clientData.close();
                output.close();
                ClientSocket.close();
                System.out.println("Received bolt:" + bolt_name);


                /* Clean bolt list */
                for (int i = bolt_list.size() - 1; i >= 0; i--) {
                    if(!bolt_list.get(i).isAlive()){
                        System.out.println("Deleting" + bolt_list.get(i));
                        bolt_list.remove(i);
                    }
                }
                System.out.println("Worker state: " + bolt_list);

                /* In case of reading db, get db file */
                if(bolt_info.split("\\s+").length >= 4){
                    String dbname = "";
                    if(bolt_info.split("\\s+")[2].contains(".db"))
                        dbname = bolt_info.split("\\s+")[2];
                    else if(bolt_info.split("\\s+")[3].contains(".db"))
                        dbname = bolt_info.split("\\s+")[3];
                    else if(bolt_info.split("\\s+").length == 5){
                        if(bolt_info.split("\\s+")[4].contains(".db"))
                            dbname = bolt_info.split("\\s+")[4];
                    }
                    if(!dbname.isEmpty()){
                        Socket sdfs_sock = new Socket(InetAddress.getByName(get_own_ip()), 6000);
                        OutputStream os = sdfs_sock.getOutputStream();
                        DataOutputStream dos = new DataOutputStream(os);
                        String cmd = "get " + dbname + " " + dbname;
                        dos.writeUTF(cmd);
                        os.close();
                        sdfs_sock.close();
                    }
                }
                /* spawn a bolt */
                Bolt b1 = new Bolt(bolt_info, master);
                bolt_list.add(b1);
                b1.start();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }
}
