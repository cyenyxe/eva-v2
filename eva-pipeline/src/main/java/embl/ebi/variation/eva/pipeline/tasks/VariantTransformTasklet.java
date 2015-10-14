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
package embl.ebi.variation.eva.pipeline.tasks;

import java.net.URI;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class VariantTransformTasklet implements Tasklet {

	private VariantStorageManager variantStorageManager;
    private ObjectMap variantOptions;
    private URI outdirUri;
    private URI nextFileUri;
    private URI pedigreeUri;

	private static final Logger logger = LoggerFactory.getLogger(VariantTransformTasklet.class);
	    
	public VariantTransformTasklet(VariantStorageManager variantStorageManager, ObjectMap variantOptions, 
			URI outdirUri, URI nextFileUri, URI pedigreeUri) {
		this.variantStorageManager = variantStorageManager;
		this.variantOptions = variantOptions;
		this.outdirUri = outdirUri;
		this.nextFileUri = nextFileUri;
		this.pedigreeUri = pedigreeUri;
	}

	@Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        logger.info("Transform file " + nextFileUri + " to " + outdirUri);

        logger.info("Extract variants '{}'", nextFileUri);
        variantStorageManager.extract(nextFileUri, outdirUri, variantOptions);

        logger.info("PreTransform variants '{}'", nextFileUri);
        variantStorageManager.preTransform(nextFileUri, variantOptions);
        logger.info("Transform variants '{}'", nextFileUri);
        variantStorageManager.transform(nextFileUri, pedigreeUri, outdirUri, variantOptions);
        logger.info("PostTransform variants '{}'", nextFileUri);
        variantStorageManager.postTransform(nextFileUri, variantOptions);
        return RepeatStatus.FINISHED;
    }
	
}
