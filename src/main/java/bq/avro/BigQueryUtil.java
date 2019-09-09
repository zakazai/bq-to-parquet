package bq.avro;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;

import java.io.IOException;

public class BigQueryUtil {

    private static TableSchema getBigQuerySchema(String project, String dataset, String table) throws IOException {
        GoogleCredential credential = GoogleCredential.getApplicationDefault(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory()).createScoped(BigqueryScopes.all());
        Bigquery bigQueryClient = new Bigquery.Builder(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(), credential).setApplicationName("bq-client").build();
        Bigquery.Tables tableService = bigQueryClient.tables();
        Table bqTable = tableService.get(project, dataset, table).execute();
        return bqTable.getSchema();
    }

    public static Schema getAvroSchema(String project, String dataset, String table) throws IOException {
        TableSchema schema = getBigQuerySchema(project, dataset, table);
        return BigQueryAvroUtils.toGenericAvroSchema(table, schema.getFields());
    }
}