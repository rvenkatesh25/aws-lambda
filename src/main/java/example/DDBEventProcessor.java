package example;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.util.Base64;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Builder;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DDBEventProcessor implements
        RequestHandler<DynamodbEvent, String> {
    private Bigquery bigquery;

    public PrivateKey getPrivateKey() throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {
        String pk = "private key";

        StringBuilder pkcs8Lines = new StringBuilder();
        BufferedReader rdr = new BufferedReader(new StringReader(pk));
        String line;
        while ((line = rdr.readLine()) != null) {
            pkcs8Lines.append(line);
        }

        String pkcs8Pem = pkcs8Lines.toString();
        pkcs8Pem = pkcs8Pem.replace("-----BEGIN PRIVATE KEY-----", "");
        pkcs8Pem = pkcs8Pem.replace("-----END PRIVATE KEY-----", "");
        pkcs8Pem = pkcs8Pem.replaceAll("\\s+","");

        byte [] pkcs8EncodedBytes = Base64.decode(pkcs8Pem);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8EncodedBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(keySpec);
    }

    public DDBEventProcessor() throws IOException, GeneralSecurityException {
        List<String> scopes = Lists.newArrayList();
        scopes.add(BigqueryScopes.BIGQUERY);

        NetHttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
        JacksonFactory jsonFactory = JacksonFactory.getDefaultInstance();
        // TODO: credentials should be managed more securely
        Credential credential = new GoogleCredential.Builder()
                .setServiceAccountId("lambda-bq@tt-dp-prod.iam.gserviceaccount.com")
                .setServiceAccountPrivateKeyId("private-key-id")
                .setServiceAccountScopes(scopes)
                .setServiceAccountPrivateKey(getPrivateKey())
                .setJsonFactory(jsonFactory)
                .setTransport(transport)
                .build();

        bigquery = new Builder(transport, jsonFactory, credential).build();
    }

    public String handleRequest(DynamodbEvent ddbEvent, Context context) {
        LinkedList<Rows> rowsList = new LinkedList<>();
        for (DynamodbStreamRecord record : ddbEvent.getRecords()){
            // get full record from ddb
            Map<String, AttributeValue> recordData = record.getDynamodb().getNewImage();
            // instantiate new bq record
            HashMap<String, Object> bqRecord = new HashMap<>();
            // populate bq record
            for (String key: recordData.keySet()) {
                bqRecord.put(key, recordData.get(key));
            }
            // create a new bq row
            TableDataInsertAllRequest.Rows rows = new TableDataInsertAllRequest.Rows();
            rows.setJson(bqRecord);
            // batch rows for a single request
            rowsList.add(rows);
        }
        TableDataInsertAllRequest insertRequest = new TableDataInsertAllRequest();
        insertRequest.setRows(rowsList);

        String tableName = "lambdabq";
        String datasetName = "sandbox";
        String projectName = "tt-dp-prod";

        try {
            TableDataInsertAllResponse response = bigquery.tabledata().insertAll(projectName, datasetName, tableName, insertRequest).execute();
        } catch (IOException e) {
            return "Failed: " + e.getMessage();
        }
        return "Successfully processed " + ddbEvent.getRecords().size() + " records.";
    }
}