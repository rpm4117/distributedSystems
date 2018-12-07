package com.company;

import java.net.*;
import java.io.*;
import java.util.ArrayList;


/*
file sender class
takes care of all the file sending in the file system program
the file sender has multiple constructors and send functions to deal with all scenarios
 */
public class file_sender extends Thread {
    private Socket socket;
    private File tosend;
    private  String fn;
    private Long ts;
    public int flag = 0;
    public InetAddress master;
    ArrayList<String> filelist;
    DatagramSocket ack_socket;


    /*
    public file_sender(InetAddress b,  int port, ArrayList<String> as, InetAddress replier)
    the file sender with a replier InetAddress and an arraylist of strings
    used during get-versions
    the arraylist of string is the arraylist of filename to send out
     */
    public file_sender(InetAddress b,  int port, ArrayList<String> as, InetAddress replier) throws IOException{
        socket = new Socket(b, port);
        filelist = as;
        master = replier;
    }
/*
public file_sender(InetAddress b, String localname, String newname, int port)
this constructor has the localname and the newname in it
with the localname standing for the filesystem filename
and newname standing for the receiver filename
 */

    public file_sender(InetAddress b, String localname, String newname, int port) throws IOException{
        socket = new Socket(b, port);
        fn = newname;
        System.out.println("sending file "+localname + " to " + b);
        tosend = new File(localname);

    }
/*
public file_sender(InetAddress b, String localname, String newname, int port, Long timestamp)
this constructor includes a filename, a newname and a timestamp
with the localname standing for the filesystem filename
and newname standing for the receiver filename
the timestamp is the long we need to send
 */
    public file_sender(InetAddress b, String localname, String newname, int port, Long timestamp) throws IOException{
        socket = new Socket(b, port);
        fn = newname;
        System.out.println("sending file "+localname + " to " + b);
        tosend = new File(localname);
        ts = timestamp;
    }

    /*
    public void sendfiles(ArrayList<String> list)
    send a list of files
    used in get-versions command
    does not close the socket between each file write
     */

    public void sendfiles(ArrayList<String> list)throws IOException{
        for(int i = 0; i<list.size();i++){
            tosend = new File(list.get(i));
            OutputStream os = socket.getOutputStream();
            BufferedOutputStream bos = new BufferedOutputStream(os);

            byte[] mybytearray = new byte[5000];

            FileInputStream fis = new FileInputStream(tosend);
            BufferedInputStream bis = new BufferedInputStream(fis);

            String delimiter = "Version " + Integer.toString(i + 1) + ":\n";
            bos.write(delimiter.getBytes());
            int count;
            while((count = bis.read(mybytearray)) > 0){
                bos.write(mybytearray, 0, count);
            }
            bos.flush();
            os.flush();
            System.out.println("File sent");
        }
        socket.close();
        System.out.println("All Done");

    }


    /*
     public void sendts()
     sned long during the process
     used when sending timestamp is needed
     */
    public void sendts() throws IOException{
        //System.out.println("sending file " + tosend.getName());
        //fn+='\n';
        OutputStream os = socket.getOutputStream();
        DataOutputStream dos = new DataOutputStream(os);
        dos.writeUTF(fn);
        dos.writeLong(ts);
        dos.flush();

        byte[] mybytearray = new byte[5000];

        FileInputStream fis = new FileInputStream(tosend);
        BufferedInputStream bis = new BufferedInputStream(fis);
        //bis.read(mybytearray, 0, mybytearray.length);
        int count;
        while((count = bis.read(mybytearray)) > 0){
            os.write(mybytearray, 0, count);
        }

        os.flush();

        fis.close();
        bis.close();
        os.close();
        dos.close();
        socket.close();
        System.out.println("File sent");
    }

    /*
     public void sendrep(ArrayList<String> list)
    send the replicas
    send multiple files
    get a reply message and send ack back
     */
    public void sendrep(ArrayList<String> list) throws IOException{
        InetAddress addr = socket.getInetAddress();
        int port = socket.getPort();
        //System.out.println("addr: " + addr + ", port: " + port);
        OutputStream os = socket.getOutputStream();
        DataOutputStream dos = new DataOutputStream(os);
        dos.writeInt(list.size());
        for(int i = 0; i<list.size();i++){
            tosend = new File(list.get(i));
            byte[] mybytearray = new byte[5000];
            FileInputStream fis = new FileInputStream(tosend);
            BufferedInputStream bis = new BufferedInputStream(fis);
            os = socket.getOutputStream();
            BufferedOutputStream bos = new BufferedOutputStream(os);
            dos.writeUTF(list.get(i));
            dos.writeLong(tosend.length());

            int count;
            while((count = bis.read(mybytearray)) > 0){
                os.write(mybytearray, 0, count);
            }
            bos.flush();
            os.flush();
            System.out.println("Rep file sent");
        }
        DatagramSocket ack_socket = new DatagramSocket(3700);
        String reply = "reped";
        byte[] reply_b = reply.getBytes();
        DatagramPacket reply_p = new DatagramPacket(reply_b, reply_b.length, master, 3493);
        ack_socket.send(reply_p);
        ack_socket.close();
        socket.close();
        System.out.println("Rep all Done");
    }

/*
public void send()
basic send function
write long as 0 and writeUTF
used when we do not need to write a meaningfu long
 */

    public void send() throws IOException{
        //System.out.println("sending file " + tosend.getName());
        //fn+='\n';
        byte[] mybytearray = new byte[5000];

        FileInputStream fis = new FileInputStream(tosend);
        BufferedInputStream bis = new BufferedInputStream(fis);
        //bis.read(mybytearray, 0, mybytearray.length);


        OutputStream os = socket.getOutputStream();
        DataOutputStream dos = new DataOutputStream(os);

        dos.writeUTF(fn);
        dos.writeLong(0);
        dos.flush();

        int count;
        while((count = bis.read(mybytearray)) > 0){
            os.write(mybytearray, 0, count);
        }
        os.flush();

        fis.close();
        bis.close();
        os.close();
        dos.close();
        socket.close();
        System.out.println("File sent");
    }
/*
 public void run()
 run function
 uses threading to call the send functions
 */
    public void run() {
        if (flag == 0) {
            System.out.println("sending one file");
            try {
                send();
            } catch (IOException e) {}
        }
        else if (flag == 1) {
            try {
                sendfiles(filelist);
            } catch (IOException e) {}
        }
        else{
            try {
                System.out.println("sending multiple files");
                sendrep(filelist);
            } catch (IOException e) {}
        }
    }
}