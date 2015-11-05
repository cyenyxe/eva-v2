/*
 * Copyright 2015 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.pipeline.listeners.JobParametersListener;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.core.env.Environment;

@Configuration
@EnableBatchProcessing
public class VariantConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(VariantConfiguration.class);
    public static final String jobName = "variantJob";
    public static final String SKIP_STATS_CREATE = "skipStatsCreate";
    public static final String SKIP_LOAD = "skipLoad";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobParametersListener listener;
    @Autowired
    JobLauncher jobLauncher;
    @Autowired
    Environment environment;

    @Bean
    public JobParametersListener jobParametersListener() {
        return new JobParametersListener();
    }

    @Bean
    public Job variantJob() {
        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer())
                .listener(listener);

        return jobBuilder
                .start(transform())
                .next(load())
                .next(statsCreate())
//                .next(statsLoad())
//                .next(annotation(stepBuilderFactory));
                .build();
    }

    public Step transform() {
        StepBuilder step1 = stepBuilderFactory.get("transform");
        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                JobParameters parameters = chunkContext.getStepContext().getStepExecution().getJobParameters();
                ObjectMap variantOptions = listener.getVariantOptions();

                URI outdirUri = createUri(parameters.getString("outputDir"));
                URI nextFileUri = createUri(parameters.getString("input"));
                URI pedigreeUri = parameters.getString("pedigree") != null ? createUri(parameters.getString("pedigree")) : null;

                logger.info("transform file " + parameters.getString("input") + " to " + parameters.getString("outputDir"));

                logger.info("Extract variants '{}'", nextFileUri);
                VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
                variantStorageManager.extract(nextFileUri, outdirUri, variantOptions);

                logger.info("PreTransform variants '{}'", nextFileUri);
                variantStorageManager.preTransform(nextFileUri, variantOptions);
                logger.info("Transform variants '{}'", nextFileUri);
                variantStorageManager.transform(nextFileUri, pedigreeUri, outdirUri, variantOptions);
                logger.info("PostTransform variants '{}'", nextFileUri);
                variantStorageManager.postTransform(nextFileUri, variantOptions);
                return RepeatStatus.FINISHED;
            }
        });

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(false);

        return tasklet.build();
    }

    public Step load() {
        StepBuilder step1 = stepBuilderFactory.get("load");
        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                JobParameters parameters = chunkContext.getStepContext().getStepExecution().getJobParameters();
                ObjectMap variantOptions = listener.getVariantOptions();

                if (Boolean.parseBoolean(parameters.getString(SKIP_LOAD, "false"))) {
                    logger.info("skipping load step, requested " + SKIP_LOAD + "=" + parameters.getString(SKIP_LOAD));
                } else {
                    VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();// TODO add mongo
                    URI outdirUri = createUri(parameters.getString("outputDir"));
                    URI nextFileUri = createUri(parameters.getString("input"));
                    URI pedigreeUri = parameters.getString("pedigree") != null ? createUri(parameters.getString("pedigree")) : null;
                    Path output = Paths.get(outdirUri.getPath());
                    Path input = Paths.get(nextFileUri.getPath());
                    Path outputVariantJsonFile = output.resolve(input.getFileName().toString() + ".variants.json" + parameters.getString("compressExtension"));
//                outputFileJsonFile = output.resolve(input.getFileName().toString() + ".file.json" + config.compressExtension);
                    URI transformedVariantsUri = outdirUri.resolve(outputVariantJsonFile.getFileName().toString());


                    logger.info("-- PreLoad variants -- {}", nextFileUri);
                    variantStorageManager.preLoad(transformedVariantsUri, outdirUri, variantOptions);
                    logger.info("-- Load variants -- {}", nextFileUri);
                    variantStorageManager.load(transformedVariantsUri, variantOptions);
//                logger.info("-- PostLoad variants -- {}", nextFileUri);
//                variantStorageManager.postLoad(transformedVariantsUri, outdirUri, variantOptions);
                }

                return RepeatStatus.FINISHED;
            }
        });

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(false);
        return tasklet.build();
    }

    public Step statsCreate() {
        StepBuilder step1 = stepBuilderFactory.get("statsCreate");
        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
//                HashMap<String, Set<String>> samples = new HashMap<>(); // TODO fill properly. if this is null overwrite will take on
//                samples.put("SOME", new HashSet<>(Arrays.asList("HG00096", "HG00097")));
                JobParameters parameters = chunkContext.getStepContext().getStepExecution().getJobParameters();

                if (Boolean.parseBoolean(parameters.getString(SKIP_STATS_CREATE, "false"))) {
                    logger.info("skipping stats creation step, requested " + SKIP_STATS_CREATE + "=" + parameters.getString(SKIP_STATS_CREATE));
                } else {
                    ObjectMap variantOptions = listener.getVariantOptions();
                    VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
                    VariantSource variantSource = variantOptions.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
                    VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(variantOptions.getString("dbName"), variantOptions);
                    URI outdirUri = createUri(parameters.getString("outputDir"));
                    URI statsOutputUri = outdirUri.resolve(VariantStorageManager.buildFilename(variantSource));

                    VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
                    QueryOptions statsOptions = new QueryOptions(variantOptions);

                    // actual stats creation
                    variantStatisticsManager.createStats(dbAdaptor, statsOutputUri, null, statsOptions);
                }

                return RepeatStatus.FINISHED;
            }
        });

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(false);
        return tasklet.build();
    }

//    public Step statsLoad() {
//        StepBuilder step1 = stepBuilderFactory.get("statsLoad");
//        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
//            @Override
//            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
//
//                if (config.calculateStats) {
//                    // obtaining resources. this should be minimum, in order to skip this step if it is completed
//                    VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
//                    QueryOptions statsOptions = new QueryOptions(variantOptions);
//                    VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(config.dbName, variantOptions);
//
//                    // actual stats load
//                    variantStatisticsManager.loadStats(dbAdaptor, statsOutputUri, statsOptions);
//                } else {
//                    logger.info("skipping stats loading");
//                }
//
//                return RepeatStatus.FINISHED;
//            }
//        });
//
//        // true: every job execution will do this step, even if this step is already COMPLETED
//        // false: if the job was aborted and is relaunched, this step will NOT be done again
//        tasklet.allowStartIfComplete(false);
//        return tasklet.build();
//    }

    public static URI createUri(String input) throws URISyntaxException {
        URI sourceUri = new URI(null, input, null);
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = Paths.get(input).toUri();
        }
        return sourceUri;
    }

}
