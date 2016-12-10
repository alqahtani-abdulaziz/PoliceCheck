import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.*;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashMap;

public class PoliceCheck {
    QueryApp queryApp ;
    Configuration config;
    ServiceBusContract service;
    HashMap<String , CameraEntity> camerasbyid;

    public static final String storageConnectionString =
            "DefaultEndpointsProtocol=http;" +
                    "AccountName=nosql;" +
                    "AccountKey=2d7s25YGjHn5GxcW0qW1DYsyMwcp0cxT9/z0jGpvoDZJTa9Q2R0pP7hizwEN9jSzoAil5KzrRzH8RTAQEP4rpw==";
    public PoliceCheck() throws URISyntaxException, InvalidKeyException, StorageException, ServiceException {

        config = ServiceBusConfiguration.configureWithSASAuthentication(
                "smarcamera",
                "RootManageSharedAccessKey",
                "h9zP+sPjaennM/CI3rHJzoy+ymsSGJkcJM0/csNp7Vw=",
                ".servicebus.windows.net"

        );

        createTable();
        //Create a connection to the service
        service = ServiceBusService.create(config);
        queryApp = new QueryApp();
        camerasbyid = new HashMap<String, CameraEntity>();
        ArrayList<CameraEntity> cameras = queryApp.getAllCameras();
        for (CameraEntity cameraEntity : cameras){
            camerasbyid.put(cameraEntity.getUid(),cameraEntity);
        }

        try{
        // Create a "HighMessages" filtered subscription
        SubscriptionInfo subInfo = new SubscriptionInfo("HighMessages");
        CreateSubscriptionResult result = service.createSubscription("vehiclemsgs", subInfo);
        RuleInfo ruleInfo = new RuleInfo("myRuleGT3");
        ruleInfo = ruleInfo.withSqlExpressionFilter("isOffender = true");
        CreateRuleResult ruleResult = service.createRule("vehiclemsgs", "HighMessages", ruleInfo);
// Delete the default rule, otherwise the new rule won't be invoked.
        service.deleteRule("vehiclemsgs", "HighMessages", "$Default");}
        catch (Exception e){
            System.out.println("Already Subscribed");
        }

        recieveSubscribtionMessage();

    }
    public void recieveSubscribtionMessage(){

        try
        {
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            opts.setReceiveMode(ReceiveMode.PEEK_LOCK);

            while(true)  {
                ReceiveSubscriptionMessageResult  resultSubMsg =
                        service.receiveSubscriptionMessage("vehiclemsgs", "HighMessages", opts);
                BrokeredMessage message = resultSubMsg.getValue();
                if (message != null && message.getMessageId() != null)
                {
                    byte[] b = new byte[200];
                    String s = null;
                    int numRead = message.getBody().read(b);
                    ArrayList<String> out = new ArrayList<String>();
                    while (-1 != numRead)
                    {
                        s = new String(b);
                        s = s.trim();
                        out.add(s);
                        numRead = message.getBody().read(b);
                    }
                    String my = new String();
                    for(String x : out){
                        my += x;
                    }
                    JsonObject object = new JsonParser().parse(my).getAsJsonObject();

                    CameraEntity currentCam = camerasbyid.get(object.get("camera").getAsString());
                    int tenPercent = (int) (currentCam.getSpeedLimit() * 0.1);
                    if(object.get("speed").getAsInt() > tenPercent+currentCam.getSpeedLimit()){
                        System.out.println("PRIORITY : " + object.toString());
                    }else{
                        System.out.println(object.toString());
                    }
                    writeTable(object);

                }
                else
                {
                    System.out.println("Finishing up - no more messages.");
                    break;
                    // Added to handle no more messages.
                    // Could instead wait for more messages to be added.
                }
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        catch (Exception e) {
            System.out.print("Generic exception encountered: ");
            System.out.println(e.getMessage());
        }

    }

    public void writeTable(JsonObject object){
        VehicleEntity entity = new VehicleEntity(object.get("registration").getAsString(),object.get("isOffender").getAsString());
        entity.setRegistration(object.get("registration").getAsString());
        entity.setType(object.get("type").getAsString());
        entity.setSpeed(object.get("speed").getAsInt());
        entity.setCameraID(object.get("camera").getAsString());
        entity.setOffender(object.get("isOffender").getAsBoolean());



        try
        {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference("SpeedingVehicles");



            // Create an operation to add the new customer to the people table.
            TableOperation insertCustomer1 = TableOperation.insertOrReplace(entity);

            // Submit the operation to the table service.
            cloudTable.execute(insertCustomer1);
        }
        catch (Exception e)
        {
            // Output the stack trace.
            e.printStackTrace();
        }

    }
 public void createTable(){
     try
     {
         // Retrieve storage account from connection-string.
         CloudStorageAccount storageAccount =
                 CloudStorageAccount.parse(storageConnectionString);

         // Create the table client.
         CloudTableClient tableClient = storageAccount.createCloudTableClient();

         // Create the table if it doesn't exist.
         CloudTable vehiclesTable = tableClient.getTableReference("SpeedingVehicles");
         vehiclesTable.createIfNotExists();

     }
     catch (Exception e)
     {

         System.out.println("SpeedingVehicles Table Already Exists");
     }

 }
}
