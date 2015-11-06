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
package embl.ebi.variation.eva.vcfDump;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantVcfFactory;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by jmmut on 2015-10-29.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VariantExporterTest {
    private static final String DB_NAME = "VariantExporterTest";

    private static final Logger logger = LoggerFactory.getLogger(VariantExporterTest.class);

    @Test
    public void testVcfHtsExport() throws Exception {

        Config.setOpenCGAHome(System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga");

        QueryOptions query = new QueryOptions();
//        List<String> files = Arrays.asList("5");
        List<String> files = Arrays.asList("5", "6");
        List<String> studies = Collections.singletonList("7");
        query.put(VariantDBAdaptor.FILES, files);
        query.put(VariantDBAdaptor.STUDIES, studies);
        String outputDir = "./";


        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(DB_NAME, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(query);
        VariantSourceDBAdaptor variantSourceDBAdaptor = variantDBAdaptor.getVariantSourceDBAdaptor();

        List<String> outputFiles = VariantExporter.VcfHtsExport(iterator, outputDir, variantSourceDBAdaptor, query);

        ////// checks 
        assertEquals(studies.size(), outputFiles.size());
        assertEquals(0, VariantExporter.getFailedVariants());   // test file should not have failed variants

        iterator = variantDBAdaptor.iterator(query);
        assertEqualLinesFilesAndDB(outputFiles, iterator);

        for (String outputFile : outputFiles) {
            boolean delete = new File(outputFile).delete();
            assertTrue(delete);
        }
    }
    
    @Test
    public void testVcfHtsExportSeveralStudies() throws Exception {

        Config.setOpenCGAHome(System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga");

        QueryOptions query = new QueryOptions();
//        List<String> files = Arrays.asList("5");
        List<String> files = Arrays.asList("5", "6");
        List<String> studies = Arrays.asList("7", "8");
        query.put(VariantDBAdaptor.FILES, files);
        query.put(VariantDBAdaptor.STUDIES, studies);
        String outputDir = "./";


        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(DB_NAME, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(query);
        VariantSourceDBAdaptor variantSourceDBAdaptor = variantDBAdaptor.getVariantSourceDBAdaptor();

        List<String> outputFiles = VariantExporter.VcfHtsExport(iterator, outputDir, variantSourceDBAdaptor, query);

        ////////// checks

        assertEquals(studies.size(), outputFiles.size());
        assertEquals(0, VariantExporter.getFailedVariants());
        
        for (String outputFile : outputFiles) {
            boolean delete = new File(outputFile).delete();
            assertTrue(delete);
        }
    }

    
    @Test
    public void testFilter() throws Exception {

        Config.setOpenCGAHome(System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga");

        QueryOptions query = new QueryOptions();
//        List<String> files = Arrays.asList("5");
        List<String> files = Arrays.asList("5");
        List<String> studies = Arrays.asList("7");
        String outputDir = "./";

        // tell all variables to filter with
        query.put(VariantDBAdaptor.FILES, files);
        query.put(VariantDBAdaptor.STUDIES, studies);
        query.put(VariantDBAdaptor.REGION, "20:61000-69000");
        query.put(VariantDBAdaptor.REFERENCE, "A");
//        query.put(VariantDBAdaptor., "A");

        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(DB_NAME, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(query);
        VariantSourceDBAdaptor variantSourceDBAdaptor = variantDBAdaptor.getVariantSourceDBAdaptor();

        List<String> outputFiles = VariantExporter.VcfHtsExport(iterator, outputDir, variantSourceDBAdaptor, query);
        
        ////////// checks

        assertEquals(studies.size(), outputFiles.size());
        assertEquals(0, VariantExporter.getFailedVariants());   // test file should not have failed variants

        iterator = variantDBAdaptor.iterator(query);
        assertEqualLinesFilesAndDB(outputFiles, iterator);

        for (String outputFile : outputFiles) {
            boolean delete = new File(outputFile).delete();
            assertTrue(delete);
        }
    }

    private void assertEqualLinesFilesAndDB(List<String> fileNames, VariantDBIterator iterator) throws IOException {

        long lines = 0;
        for (String fileName : fileNames) {
            // counting lines (without comments)
            BufferedReader file = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileName))));
            String line;
            while ((line = file.readLine()) != null) {
                if (line.charAt(0) != '#') {
                    lines++;
                }
            }
            file.close();
        }

        // counting variants in the DB
        int variantRows = 0;
        while(iterator.hasNext()) {
            iterator.next();
            variantRows++;
        }

        assertEquals(variantRows, lines);
    }

    @Test
    public void testGetVariantContextFromVariant() throws Exception {
        final VariantSource variantSource = new VariantSource("name", "fileId", "studyId", "studyName");
        List<String> samples = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            samples.add("s"+i);
        }
        variantSource.setSamples(samples);
        VariantFactory factory = new VariantVcfFactory();
        Map<String, VariantSource> sources = Collections.singletonMap(variantSource.getStudyId(), variantSource);
        QueryOptions options = new QueryOptions("fileId", "fileId");
        String studyId = "studyId";
        options.add("studyId", studyId);
        List<Variant> variants;
        Map<String, VariantContext> variantContext;
        List<String> alleles;

        // test multiallelic
        String multiallelicLine = "1\t1000\tid\tC\tA,T\t100\tPASS\t.\tGT\t0|0\t0|0\t0|1\t1|1\t1|2\t0|1";
        variants = factory.create(variantSource, multiallelicLine);
        assertEquals(2, variants.size());

        variantContext = VariantExporter.convertBiodataVariantToVariantContext(variants.get(0), sources);
        
        alleles = Arrays.asList("C", "A", ".");
        assertEqualGenotypes(variants.get(0), variantContext.get(studyId), alleles);

        variantContext = VariantExporter.convertBiodataVariantToVariantContext(variants.get(1), sources);
        alleles = Arrays.asList("C", "T", ".");
        assertEqualGenotypes(variants.get(1), variantContext.get(studyId), alleles);


        // test indel
        String indelLine = "1\t1000\tid\tC\tCA\t100\tPASS\t.\tGT\t0|0\t0|0\t0|1\t1|1\t1|0\t0|1";
        variants = factory.create(variantSource, indelLine);

        variantContext = VariantExporter.convertBiodataVariantToVariantContext(variants.get(0), sources);
        alleles = Arrays.asList("C", "CA");
        assertEqualGenotypes(variants.get(0), variantContext.get(studyId), alleles);


        // test multiallelic + indel
        String multiallelicIndelLine = "1\t1000\tid\tC\tCA,T\t100\tPASS\t.\tGT\t0|0\t0|0\t0|1\t1|1\t1|2\t0|1";
        variants = factory.create(variantSource, multiallelicIndelLine);
        assertEquals(2, variants.size());

        variantContext = VariantExporter.convertBiodataVariantToVariantContext(variants.get(0), sources);
        alleles = Arrays.asList("C", "CA", ".");
        assertEqualGenotypes(variants.get(0), variantContext.get(studyId), alleles);

        variantContext = VariantExporter.convertBiodataVariantToVariantContext(variants.get(1), sources);
        alleles = Arrays.asList("C", "T", ".");
        assertEqualGenotypes(variants.get(1), variantContext.get(studyId), alleles);

    }

    private void assertEqualGenotypes(Variant variant, VariantContext variantContext, List<String> alleles) {
        for (Map.Entry<String, Map<String, String>> data : variant.getSourceEntries().values().iterator().next().getSamplesData().entrySet()) {
            Genotype genotype = variantContext.getGenotype(data.getKey());
            String gt = data.getValue().get("GT");
            org.opencb.biodata.models.feature.Genotype biodataGenotype = new org.opencb.biodata.models.feature.Genotype(gt, alleles.get(0), alleles.get(1));
            assertEquals(Allele.create(alleles.get(biodataGenotype.getAllele(0)), biodataGenotype.isAlleleRef(0)),
                    genotype.getAllele(0));
            assertEquals(Allele.create(alleles.get(biodataGenotype.getAllele(1)), biodataGenotype.isAlleleRef(1)),
                    genotype.getAllele(1));
        }
    }

    @BeforeClass
    public static void beforeTests() throws IOException, InterruptedException {
        cleanDBs();
        fillDB();
    }

    @AfterClass
    public static void afterTests() throws UnknownHostException {
        cleanDBs();
    }

    private static void cleanDBs() throws UnknownHostException {
        // Delete Mongo collection
        MongoClient mongoClient = new MongoClient("localhost");
        List<String> dbs = Arrays.asList(DB_NAME);
        for (String dbName : dbs) {
            DB db = mongoClient.getDB(dbName);
            db.dropDatabase();
        }
        mongoClient.close();
    }

    public static void fillDB() throws IOException, InterruptedException {
        String dump = VariantExporterTest.class.getResource("/dump/").getFile();
        logger.info("restoring DB from " + dump);
        Process exec = Runtime.getRuntime().exec("mongorestore " + dump);
        exec.waitFor();
        String line;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
        while ((line = bufferedReader.readLine()) != null) {
            logger.info("mongorestore output:" + line);
        }
        bufferedReader.close();
        bufferedReader = new BufferedReader(new InputStreamReader(exec.getErrorStream()));
        while ((line = bufferedReader.readLine()) != null) {
            logger.info("mongorestore errorOutput:" + line);
        }
        bufferedReader.close();

        logger.info("mongorestore exit value: " + exec.exitValue());
    }

    public class MockVariantSourceDBAdaptor implements VariantSourceDBAdaptor {
        private VariantSource variantSource;

        public MockVariantSourceDBAdaptor(VariantSource variantSource) {
            this.variantSource = variantSource;
        }

        @Override
        public QueryResult countSources() {
            return null;
        }

        @Override
        public QueryResult getAllSources(QueryOptions options) {
            QueryResult<VariantSource> queryResult = new QueryResult<>();
            queryResult.setResult(Collections.singletonList(variantSource));
            return queryResult;
        }

        @Override
        public QueryResult getAllSourcesByStudyId(String studyId, QueryOptions options) {
            return null;
        }

        @Override
        public QueryResult getAllSourcesByStudyIds(List<String> studyIds, QueryOptions options) {
            return null;
        }

        @Override
        public QueryResult getSamplesBySource(String fileId, QueryOptions options) {
            return null;
        }

        @Override
        public QueryResult getSamplesBySources(List<String> fileIds, QueryOptions options) {
            return null;
        }

        @Override
        public QueryResult getSourceDownloadUrlByName(String filename) {
            return null;
        }

        @Override
        public List<QueryResult> getSourceDownloadUrlByName(List<String> filenames) {
            return null;
        }

        @Override
        public QueryResult getSourceDownloadUrlById(String fileId, String studyId) {
            return null;
        }

        @Override
        public QueryResult updateSourceStats(VariantSourceStats variantSourceStats, QueryOptions queryOptions) {
            return null;
        }

        @Override
        public boolean close() {
            return false;
        }
    }
}
