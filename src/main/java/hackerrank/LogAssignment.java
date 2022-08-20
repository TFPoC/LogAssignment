package hackerrank;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LogAssignment {

    private static Map events = new ConcurrentHashMap<String,Event>();
    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>(10);;

    private static final Logger log = Logger.getLogger("LogAssignment");

    private volatile boolean flag = true;


    public static void main(String args[]) {

        String fileName = System.getProperty("user.dir")+File.separator+"logfile.txt";
        LogAssignment l = new LogAssignment();
        JDBCUtils.createTable();
        if("Serial".equals(args[0])) {
            l.processSequence(fileName);
        } else if ("Parallel".equals(args[0])){

            Thread producer = new Thread(() -> {
                try {
                    l.produce(fileName);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            Thread consumer = new Thread(() -> {
                try {
                    l.consume();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            producer.start();
            consumer.start();
        }

    }

    public static Event toObject(String json){

        ObjectMapper mapper = new ObjectMapper();
        try {

            Event e = mapper.readValue(json, Event.class);
            return e;
        } catch (JsonProcessingException e) {
            log.log(Level.SEVERE,"Error while converting json to object",e);
        }
        return null;
    }

    public void processSequence(String fileName){

        try (Scanner input = new Scanner(new File(fileName));) {

            while(input.hasNextLine()){

                Event e = LogAssignment.toObject(input.nextLine());
                if(events.get(e.getId()) == null){
                    events.put(e.getId(),e);
                } else {
                    // check is long event
                    Event e1 = (Event)events.get(e.getId());
                    if(e.isLongEvent(e1)){
                        log.info("Event is more than 4ms "+e.getId());
                        try {
                            JDBCUtils.insertRecord(e.getId(),"true",e.getType(),e.getHost(), (int) Math.abs(e1.getTimestamp() - e.getTimestamp()));
                        } catch (SQLException e2) {
                            e2.printStackTrace();
                        }
                    }
                }

            }

        } catch (IOException e) {
            log.log(Level.SEVERE,"Error while reading file",e);
        }

        /*try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {

            String line;
            while ((line = br.readLine()) != null) {

                Event e = LogAssignment.toObject(line);
                if(events.get(e.getId()) == null){
                    events.put(e.getId(),e);
                } else {
                    // check is long event
                    Event e1 = (Event)events.get(e.getId());
                    if(e.isLongEvent(e1)){
                        log.info("Event is more than 4ms "+e.getId());
                        try {
                            JDBCUtils.insertRecord(e.getId(),"true",e.getType(),e.getHost(), (int) Math.abs(e1.getTimestamp() - e.getTimestamp()));
                        } catch (SQLException e2) {
                            e2.printStackTrace();
                        }
                    }
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }*/

    }

    public void produce(String fileName) throws InterruptedException {

        try (Scanner input = new Scanner(new File(fileName));) {

            String line;
            while(input.hasNextLine()){

                Event e = LogAssignment.toObject(input.nextLine());
                queue.put(e);
            }
            flag = false;

        } catch (IOException e) {
            log.log(Level.SEVERE,"Error while reading file",e);
        }
    }

    public void consume() throws InterruptedException {

        while(flag || !queue.isEmpty()){

            Event e = queue.take();
            log.info("[Consumer] Take : " + e);
            if(events.get(e.getId()) == null){
                events.put(e.getId(),e);
            } else {
                // check is long event
                Event e1 = (Event)events.get(e.getId());
                if(e.isLongEvent(e1)){
                    log.info("Event is more than 4ms "+e.getId());
                    try {
                        JDBCUtils.insertRecord(e.getId(),"true",e.getType(),e.getHost(), (int) Math.abs(e1.getTimestamp() - e.getTimestamp()));
                    } catch (SQLException e2) {
                        log.log(Level.SEVERE,"Error ehile inserting record",e2);
                    }
                }
            }

        }
    }

}
