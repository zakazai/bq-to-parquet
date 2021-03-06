/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package bq.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.testng.annotations.Test;

import java.io.IOException;

public class BigQueryToParquetTest {
    private String PROJECT = "<your-project-name>";
    private String TABLE = "<your-table>";
    private String DATASET = "test";
    private String dateStr = "<YYYY-MM-DD>";

    @Test
    public void testExport() throws IOException {
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(PROJECT);
        options.setStagingLocation(String.format("gs://%s/test_export/staging", options.getProject()));
        options.setTempLocation(String.format("gs://%s/test_export/temp", options.getProject()));

        Pipeline pipeline = TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);

        Schema avroSchema = BigQueryUtil.getAvroSchema(PROJECT, DATASET, TABLE);
        String fullyQualifiedTableId = PROJECT + ":" + DATASET + "." + TABLE;

        PCollection<GenericRecord> tableData = pipeline.apply("read " + fullyQualifiedTableId,
                BigQueryIO.read(new GetGenericRecord())
                        .from(fullyQualifiedTableId)
                        .withCoder(AvroCoder.of(avroSchema))
        );

        String fullBucketPath = String.format("result", options.getProject());
        tableData.apply("write " + dateStr.replace("-", ""), FileIO.<GenericRecord>write().via(
                ParquetIO.sink(avroSchema)).to(fullBucketPath).withNumShards(1)
        );

        pipeline.run().waitUntilFinish();

    }
}

class GetGenericRecord implements SerializableFunction<SchemaAndRecord, GenericRecord> {

    @Override
    public GenericRecord apply(SchemaAndRecord input) {
        return input.getRecord();
    }
}