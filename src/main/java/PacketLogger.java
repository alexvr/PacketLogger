import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.time.LocalTime;
import java.util.Arrays;

/**
 * Created by Alexander en Jens on 20/12/16.
 */
public class PacketLogger implements MqttCallback {

    private static int SESSION_IN_SECONDS    = 5;
    private static int packetCounter         = 0;
    private static int sessionCounter        = 0;
    private static long[] totalFrequencies   = new long[32];
    private static long[] averageFrequencies = new long[32];
    private static LocalTime sessionStart    = LocalTime.now();

    public static void main(String[] args) {

        String topic        = "matrixInfo";
        int qos             = 0;
        String broker       = "tcp://localhost:1883";
        String clientId     = "Subscriber";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttAsyncClient sampleClient = new MqttAsyncClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            sampleClient.setCallback(new PacketLogger());
            System.out.println("Connecting to broker: " + broker + "...");

            sampleClient.connect(connOpts);
            System.out.println("Connected!");

            Thread.sleep(1000);
            sampleClient.subscribe(topic, qos);
            System.out.println("Subscribed and ready to receive!");

        } catch (Exception me) {
            if (me instanceof MqttException) {
                System.out.println("reason " + ((MqttException) me).getReasonCode());
            }
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }

    public void connectionLost(Throwable arg0) {
        System.err.println("connection lost");

    }

    public void deliveryComplete(IMqttDeliveryToken arg0) {
        System.err.println("delivery complete");
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {
        if ((LocalTime.now().getSecond() - sessionStart.getSecond()) < SESSION_IN_SECONDS) {
            ++packetCounter;
            byte[] byteArray = message.getPayload();
            for (int i = 0; i < byteArray.length; i++) {
                totalFrequencies[i] += byteArray[i];
                averageFrequencies[i] = Math.round(totalFrequencies[i] / packetCounter);
            }
        } else {
            ++sessionCounter;
            System.out.println("Average frequencies of session " + sessionCounter + "\n  " + Arrays.toString(averageFrequencies));

            packetCounter = 0;
            sessionStart = LocalTime.now();
            totalFrequencies = new long[32];
            averageFrequencies = new long[32];

            ++packetCounter;
            byte[] byteArray = message.getPayload();
            for (int i = 0; i < byteArray.length; i++) {
                totalFrequencies[i] += byteArray[i];
                averageFrequencies[i] = Math.round(totalFrequencies[i] / packetCounter);
            }
        }

        //System.out.println(Arrays.toString(message.getPayload()));
        //System.out.println(Arrays.toString(averageFrequencies) + "\n");
    }

}