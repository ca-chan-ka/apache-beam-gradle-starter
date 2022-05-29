package main;

import java.util.logging.Logger;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;

public class Main {
	private static final Logger log = Logger.getLogger(Main.class.getName());

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory
				.fromArgs(args)
				.withValidation()
				.as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		log.info("Hello");
		pipeline
				.apply("ReadLines", TextIO.read().from(options.getInputFile()))
				.apply(ParDo.of(new RepeatWords()))
				.apply("WriteOutput", TextIO.write().to("output/out"));
		pipeline.run().waitUntilFinish();
	}

	public interface Options extends PipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("sample.txt")
		String getInputFile();

		void setInputFile(String value);

		@Description("Path of the file to write to")
		@Required
		@Default.String("counts")
		String getOutput();

		void setOutput(String value);
	}

	public static class RepeatWords extends DoFn<String, String> {
		@ProcessElement
		public void processElement(ProcessContext context) {
			log.info("processElement");
			context.output(context.element().repeat(2));
		}
	}
}
