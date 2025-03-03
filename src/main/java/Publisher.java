/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.io.*;
import java.util.LinkedList;

/**
 * @author xieyu
 * Uses a Future based API to MQTT.
 * Create shell file to execute save command in Docker.
 * Send tar of Docker images in 10MB size per frame to reduce Broker workload.
 */
class Publisher {

    public static void main(String []args) throws Exception {

        // Set shell path
        String path="/Users/xieyu/Downloads/save.sh";
        File command = new File(path);
        FileOutputStream fos = new FileOutputStream(command);
        BufferedOutputStream bs=new BufferedOutputStream(fos);

        // Write content of shell
        String top = "#!/bin/bash\n";
        String s = top + "docker save -o /Users/xieyu/Downloads/registry.tar registry";
        bs.write(s.getBytes());
        bs.close();
        fos.close();

        // Execute shell file
        Process ps = Runtime.getRuntime().exec("chmod 777 " + path);
        ps.waitFor();
        ps = Runtime.getRuntime().exec(path);
        ps.waitFor();

        // Set mqtt
        String user = env("ACTIVEMQ_USER", "admin");
        String password = env("ACTIVEMQ_PASSWORD", "password");
        // Set Activemq IP address and port.
        String host = env("ACTIVEMQ_HOST", "10.109.17.251");
        int port = Integer.parseInt(env("ACTIVEMQ_PORT", "1883"));
        final String destination = arg(args, 0, "docker");

        // Choose file to send
        // The file need to be consistent with file in shell.
        File file= new File("/Users/xieyu/Downloads/registry.tar");
        FileInputStream is= new FileInputStream(file);
        BufferedInputStream in=null;
        ByteArrayOutputStream bos=null;

        bos=new ByteArrayOutputStream((int)file.length());
        in=new BufferedInputStream(is);
        final int buf_size=1024*1024;
        byte[] buffer=new byte[buf_size];

        MQTT mqtt = new MQTT();
        mqtt.setHost(host, port);
        mqtt.setUserName(user);
        mqtt.setPassword(password);

        FutureConnection connection = mqtt.futureConnection();
        connection.connect().await();

        final LinkedList<Future<Void>> queue = new LinkedList<Future<Void>>();
        UTF8Buffer topic = new UTF8Buffer(destination);

        int len=0;
        int total=0;
        int i=1;

        while(-1 != (len=in.read(buffer,0,buf_size))){
            bos.write(buffer,0,len);
            total+=len;

            if(total>buf_size*10 || len!=buf_size){

                Buffer msg = new Buffer(bos.toByteArray());
                // Send the publish without waiting for it to complete. This allows us
                // to send multiple message without blocking..
                queue.add(connection.publish(topic, msg, QoS.EXACTLY_ONCE, false));

                // Eventually we start waiting for old publish futures to complete
                // so that we don't create a large in memory buffer of outgoing message.s

                queue.removeFirst().await();

                System.out.println(String.format("Sent %d messages.", i));
                i++;
                total=0;
                bos.reset();
            }
        }

        // Send finish sign
        byte[] shutdown = "SHUTDOWN".getBytes();
        queue.add(connection.publish(topic, new AsciiBuffer(shutdown), QoS.EXACTLY_ONCE, false));
        while( !queue.isEmpty() ) {
            queue.removeFirst().await();
        }

        connection.disconnect().await();

        in.close();
        bos.close();
        System.exit(0);

    }

    private static String env(String key, String defaultValue) {
        String rc = System.getenv(key);
        if( rc== null )
            return defaultValue;
        return rc;
    }

    private static String arg(String []args, int index, String defaultValue) {
        if( index < args.length )
            return args[index];
        else
            return defaultValue;
    }

}