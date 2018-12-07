package com.company;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;


/*
receiver thread
handles all the receiving of the program
have receive functions for multiple scenarios
 */
public class receiver_thread extends Thread {
    private ServerSocket socket;
    private Socket ClientSocket;
    private String localname;
    private Vector member_list;
    public int flag = 0;
    Hashtable<String, ArrayList> vdict;

    /*
    public receiver_thread(String input, int port, Hashtable version_dict, Vector mem_list)
    constructor of the thread
    initializes the dictionaries and the socket
     */

    public receiver_thread(String input, int port, Hashtable version_dict, Vector mem_list)throws IOException {
        vdict = version_dict;
        socket = new ServerSocket(port);
        localname = input;
        member_list = mem_list;
        //System.out.println("Receiver initializing...");
    }

    /*
    public void simple_receive()
    simple receive function
    that receives multiple files and nothing else
    used in get-versions
     */

    public void simple_receive() throws IOException{
        ClientSocket = socket.accept();
        int bytesRead;
        InputStream in = ClientSocket.getInputStream();
        DataInputStream clientData = new DataInputStream(in);

        System.out.println("Receiving file: " + localname);
        //fileName = fileName.substring(0,fileName.indexOf('.'))+"_v0"+fileName.substring(fileName.indexOf('.'), fileName.length()-1);
        OutputStream output = new FileOutputStream(localname);
        byte[] buffer = new byte[1024];
        while ((bytesRead = in.read(buffer)) != -1) {
            output.write(buffer, 0, bytesRead);
        }
        System.out.println("Receive done.");
        // Closing the FileOutputStream handle
        in.close();
        clientData.close();
        output.close();
        ClientSocket.close();
    }

    // public void read_receive()
    //read_receive function
    //used in get command of the filesystem
    /* used for reading */
    public void read_receive() throws IOException{
        ClientSocket = socket.accept();
        int bytesRead;
        InputStream in = ClientSocket.getInputStream();
        DataInputStream clientData = new DataInputStream(in);
        String fileName = clientData.readUTF();
        Long ts = clientData.readLong();
        System.out.println("Receiving reading file: " + localname);
        //fileName = fileName.substring(0,fileName.indexOf('.'))+"_v0"+fileName.substring(fileName.indexOf('.'), fileName.length()-1);
        OutputStream output = new FileOutputStream(localname);
        byte[] buffer = new byte[1024];
        while ((bytesRead = in.read(buffer)) != -1) {
            output.write(buffer, 0, bytesRead);
        }
        // Closing the FileOutputStream handle
        System.out.println("Receive done.");
        in.close();
        clientData.close();
        output.close();
        ClientSocket.close();
    }

    public void close() throws IOException{
        socket.close();            //close the socket
    }

    // public void run()
    /* used for writing */
    //the run function for the thread
    //handle basic file receiving during put functions
    //handles all the replica receiving

    public void run(){
        try {
            if(flag == 0){
                while(true){
                    ClientSocket = socket.accept();
                    int bytesRead;
                    //System.out.println("got connection");
                    InputStream in = ClientSocket.getInputStream();
                    DataInputStream clientData = new DataInputStream(in);
                    String fileName = clientData.readUTF();
                    Long ts = clientData.readLong();
                    fileName = fileName.replace("\0", "");
                    System.out.println("Receiving writing file: " + fileName);
                    String fileNameClean = fileName;


                    fileName = "./SDFS/" + fileName;
                    //fileName = "./SDFS/" + fileName.substring(0,fileName.indexOf('.'))+"_v0"+fileName.substring(fileName.indexOf('.'), fileName.length()-1);
                    OutputStream output = new FileOutputStream(fileName);
                    BufferedOutputStream out = new BufferedOutputStream(output);
                    byte[] buffer = new byte[1024];
                    while ((bytesRead = in.read(buffer)) != -1) {
                        out.write(buffer, 0, bytesRead);
                        //System.out.println("getting sth");
                    }
                    out.flush();
                    // Closing the FileOutputStream handle
                    out.close();
                    in.close();
                    clientData.close();
                    output.close();
                    ClientSocket.close();
                    //System.out.println("plain receive");
                    if (vdict.containsKey(fileNameClean)){
                        ArrayList oldlist =  vdict.get(fileNameClean);
                        oldlist.add(ts);
                    }
                    else{
                        File directory = new File("./SDFS/"+fileNameClean+'d');
                        directory.mkdir();
                        //System.out.println(cmdstr);
                        ArrayList list = new ArrayList();
                        list.add(ts);
                        vdict.put(fileNameClean, list);
                    }
                    String old_path = "./SDFS/"+fileNameClean;
                    String new_path = "./SDFS/"+fileNameClean+'d'+'/'+fileNameClean+ts.toString();
                    File old = new File(old_path);
                    File newf = new File(new_path);
                    //System.out.println(new_path);
                    Files.copy(old.toPath(), newf.toPath());
                }
            }
            if(flag == 1){
                while(true){
                    ClientSocket = socket.accept();
                    int bytesRead;
                    //System.out.println("got connection");
                    InputStream in = ClientSocket.getInputStream();
                    DataInputStream clientData = new DataInputStream(in);
                    int file_num = clientData.readInt();
                    for(int i = 0 ;i < file_num; i++){
                        String filePath = clientData.readUTF();
                        Long size = clientData.readLong();
                        filePath = filePath.replace("\0", "");
                        String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
                        String dirName = "./SDFS/" + fileName.substring(0, fileName.length() - 13) + "d";
                        File dir = new File(dirName);
                        if(!dir.exists())
                            dir.mkdir();
                        System.out.println("Receiving replica file: " + dirName + "/" + fileName);
                        OutputStream output = new FileOutputStream(dirName + "/" + fileName);
                        BufferedOutputStream out = new BufferedOutputStream(output);
                        byte[] buffer = new byte[1024];
                        if(i == file_num - 1){
                            System.out.println("writing to get file: " + "./SDFS/" + fileName.substring(0, fileName.length() - 13));
                            /* upon getting the latest version, also update the file in SDFS */
                            BufferedOutputStream get_fis = new BufferedOutputStream(new FileOutputStream("./SDFS/" + fileName.substring(0, fileName.length() - 13)));
                            while (size > 0 && (bytesRead = in.read(buffer, 0, (int)Math.min(buffer.length, size))) != -1) {
                                out.write(buffer, 0, bytesRead);
                                get_fis.write(buffer, 0, bytesRead);
                                size -= bytesRead;
                                //System.out.println("getting sth");
                            }
                            get_fis.close();
                        }
                        else{
                            while (size > 0 && (bytesRead = in.read(buffer, 0, (int)Math.min(buffer.length, size))) != -1) {
                                out.write(buffer, 0, bytesRead);
                                size -= bytesRead;
                                //System.out.println("getting sth");
                            }
                        }
                        System.out.println("got replica " + fileName);
                        // Closing the FileOutputStream handle
                        out.close();
                        output.close();
                        String fileNameClean = fileName.substring(0, fileName.length() - 13);
                        if (vdict.containsKey(fileNameClean)){
                            ArrayList oldlist =  vdict.get(fileNameClean);
                            oldlist.add(Long.parseLong(fileName.substring(fileName.length() - 13)));
                        }
                        else{
                            //System.out.println(cmdstr);
                            ArrayList list = new ArrayList();
                            list.add(Long.parseLong(fileName.substring(fileName.length() - 13)));
                            vdict.put(fileNameClean, list);
                        }
                    }

                    in.close();
                    clientData.close();
                    ClientSocket.close();
                }
            }
        }
        catch(Exception ex){ex.printStackTrace();}
    }

}
