package demo.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.ParDo;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {

    public static void main(String[] args) {
        log.info("Starting Pipeline...");
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        Write write = SpannerIO
                .write()
                .withEmulatorHost("localhost:9010")
                .withProjectId("demo-project")
                .withInstanceId("demo-instance")
                .withDatabaseId("demo-database");
        pipeline
                .apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(Ship.parse()))
                .apply(ParDo.of(Ship.toMutation("ship")))
                .apply("WriteOutput", write);

        pipeline.run().waitUntilFinish();
    }

    public interface Options extends GcpOptions {
        @Description("Path of the file to read from")
        @Default.String("input/input_normal_0.csv")
        String getInputFile();

        void setInputFile(String value);
    }
}
