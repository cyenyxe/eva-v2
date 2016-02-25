java -jar eva-pipeline/target/eva-pipeline-0.1.jar \
 --spring.batch.job.names=variantJob \
 input=data/small.vcf \
 fileId=5 \
 aggregated=NONE \
 studyType=COLLECTION \
 studyName=studyName \
 studyId=7 \
 outputDir= \
 pedigree= \
 dbName=batch \
 samples=sample1,sample2,sample3,sample4 \
 storageEngine=mongodb \
 overwriteStats=true \
 compressGenotypes=true \
 compressExtension=.gz \
 includeSrc=FIRST_8_COLUMNS \
 skipLoad=false \
 opencga.app.home=/opt/opencga/

