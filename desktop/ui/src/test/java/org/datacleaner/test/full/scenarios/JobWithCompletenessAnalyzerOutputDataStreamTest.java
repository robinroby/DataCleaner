package org.datacleaner.test.full.scenarios;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.datacleaner.api.OutputDataStream;
import org.datacleaner.beans.CompletenessAnalyzer;
import org.datacleaner.beans.CompletenessAnalyzer.Condition;
import org.datacleaner.beans.StringAnalyzer;
import org.datacleaner.components.convert.ConvertToNumberTransformer;
import org.datacleaner.configuration.DataCleanerConfiguration;
import org.datacleaner.configuration.DataCleanerConfigurationImpl;
import org.datacleaner.configuration.DataCleanerEnvironment;
import org.datacleaner.configuration.DataCleanerEnvironmentImpl;
import org.datacleaner.connection.Datastore;
import org.datacleaner.data.MetaModelInputColumn;
import org.datacleaner.extension.output.CreateExcelSpreadsheetAnalyzer;
import org.datacleaner.job.AnalysisJob;
import org.datacleaner.job.AnalyzerJob;
import org.datacleaner.job.OutputDataStreamJob;
import org.datacleaner.job.TransformerJob;
import org.datacleaner.job.builder.AnalysisJobBuilder;
import org.datacleaner.job.builder.AnalyzerComponentBuilder;
import org.datacleaner.job.builder.TransformerComponentBuilder;
import org.datacleaner.job.runner.AnalysisResultFuture;
import org.datacleaner.job.runner.AnalysisRunnerImpl;
import org.datacleaner.test.TestHelper;
import org.junit.Test;

public class JobWithCompletenessAnalyzerOutputDataStreamTest {

    private final Datastore datastore = TestHelper.createSampleDatabaseDatastore("orderdb");
    private DataCleanerEnvironment environment = new DataCleanerEnvironmentImpl();
    private final DataCleanerConfiguration configuration = new DataCleanerConfigurationImpl().withDatastores(datastore)
            .withEnvironment(environment);

    @Test
    public void test() throws Throwable {

        final AnalysisJob job;
        try (final AnalysisJobBuilder ajb = new AnalysisJobBuilder(configuration)) {
            ajb.setDatastore(datastore);
            ajb.addSourceColumns("customers.contactfirstname");
            ajb.addSourceColumns("customers.contactlastname");
            ajb.addSourceColumns("customers.city");

            final AnalyzerComponentBuilder<CompletenessAnalyzer> completenessAnalyzer = ajb
                    .addAnalyzer(CompletenessAnalyzer.class);

            assertEquals(0, completenessAnalyzer.getOutputDataStreams().size());

            final List<MetaModelInputColumn> sourceColumns = ajb.getSourceColumns();
            completenessAnalyzer.setName("completeness analyzer");
            completenessAnalyzer.addInputColumns(sourceColumns);

            Condition[] conditions = { CompletenessAnalyzer.Condition.NOT_BLANK_OR_NULL, CompletenessAnalyzer.Condition.NOT_BLANK_OR_NULL, CompletenessAnalyzer.Condition.NOT_BLANK_OR_NULL };
            completenessAnalyzer.setConfiguredProperty(CompletenessAnalyzer.PROPERTY_CONDITIONS, conditions);
            completenessAnalyzer.setConfiguredProperty(CompletenessAnalyzer.PROPERTY_CONDITIONS, conditions);

            final List<OutputDataStream> dataStreams = completenessAnalyzer.getOutputDataStreams();

            assertEquals(2, dataStreams.size());

            assertEquals("Complete rows", dataStreams.get(0).getName());
            assertEquals("Incomplete rows", dataStreams.get(1).getName());

            final AnalysisJobBuilder completeRowsOutputDataStreamJobBuilder = completenessAnalyzer
                    .getOutputDataStreamJobBuilder(dataStreams.get(0));
            final List<MetaModelInputColumn> outputStreamCompleteRowsSourceColumns = completeRowsOutputDataStreamJobBuilder
                    .getSourceColumns();
            assertEquals("MetaModelInputColumn[Complete rows.CONTACTFIRSTNAME]", outputStreamCompleteRowsSourceColumns
                    .get(0).toString());
            assertEquals("MetaModelInputColumn[Complete rows.CONTACTLASTNAME]", outputStreamCompleteRowsSourceColumns
                    .get(1).toString());
            assertEquals("MetaModelInputColumn[Complete rows.CITY]", outputStreamCompleteRowsSourceColumns.get(2)
                    .toString());

            final AnalyzerComponentBuilder<CreateExcelSpreadsheetAnalyzer> excelAnalyzer = completeRowsOutputDataStreamJobBuilder
                    .addAnalyzer(CreateExcelSpreadsheetAnalyzer.class);

            excelAnalyzer.addInputColumns(outputStreamCompleteRowsSourceColumns);
            excelAnalyzer.setConfiguredProperty(CreateExcelSpreadsheetAnalyzer.PROPERTY_SHEET_NAME, "test");
            excelAnalyzer
                    .setConfiguredProperty(CreateExcelSpreadsheetAnalyzer.PROPERTY_OVERWRITE_SHEET_IF_EXISTS, true);
            excelAnalyzer.setName("write to excel");

            final AnalysisJobBuilder incompleRowsOutputDataStreamJobBuilder = completenessAnalyzer
                    .getOutputDataStreamJobBuilder(dataStreams.get(1));
            final List<MetaModelInputColumn> incompleteOutputStreamSourceColumns = incompleRowsOutputDataStreamJobBuilder
                    .getSourceColumns();

            assertEquals("MetaModelInputColumn[Incomplete rows.CONTACTFIRSTNAME]", incompleteOutputStreamSourceColumns
                    .get(0).toString());
            assertEquals("MetaModelInputColumn[Incomplete rows.CONTACTLASTNAME]", incompleteOutputStreamSourceColumns
                    .get(1).toString());
            assertEquals("MetaModelInputColumn[Incomplete rows.CITY]", incompleteOutputStreamSourceColumns.get(2)
                    .toString());

            final AnalyzerComponentBuilder<StringAnalyzer> stringAnalyser = incompleRowsOutputDataStreamJobBuilder
                    .addAnalyzer(StringAnalyzer.class);
            stringAnalyser.addInputColumns(incompleteOutputStreamSourceColumns);
            stringAnalyser.setName("string analyzer");

            final TransformerComponentBuilder<ConvertToNumberTransformer> convertToNumberTransformer = ajb
                    .addTransformer(ConvertToNumberTransformer.class);
            convertToNumberTransformer.addInputColumns(sourceColumns);
            convertToNumberTransformer.setName("number converter");

            job = ajb.toAnalysisJob();

            final List<AnalyzerJob> analyzerJobs = job.getAnalyzerJobs();

            assertEquals(1, analyzerJobs.size());
            final AnalyzerJob completeAnalyzersJob = analyzerJobs.get(0);
            assertEquals(completenessAnalyzer.getName(), completeAnalyzersJob.getName());
            final OutputDataStreamJob[] outputDataStreamJobs = completeAnalyzersJob.getOutputDataStreamJobs();
            assertEquals(2, outputDataStreamJobs.length);
            assertEquals(excelAnalyzer.getName(), outputDataStreamJobs[0].getJob().getAnalyzerJobs().get(0).getName());
            assertEquals(stringAnalyser.getName(), outputDataStreamJobs[1].getJob().getAnalyzerJobs().get(0).getName());
            final List<TransformerJob> transformerJobs = job.getTransformerJobs();
            assertEquals(1, transformerJobs.size());
            assertEquals(convertToNumberTransformer.getName(), transformerJobs.get(0).getName());

            final AnalysisRunnerImpl runner = new AnalysisRunnerImpl(configuration);
            final AnalysisResultFuture result = runner.run(job);

            result.await();
            
            if (result.isErrornous()) {
                throw result.getErrors().get(0);
            }
        }
    }

}
