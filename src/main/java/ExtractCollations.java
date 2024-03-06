import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.UUID;

public class ExtractCollations {
    public static AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;
    public static int numberOfInstances = 3;
    public static int appId = UUID.randomUUID().hashCode();

    public static void main(String[]args){
        if (args.length != 3) {
            System.out.println("Usage: ExtractCollations <minPmi> <relMinPmi>");
            System.exit(1);
        }

        String minPmi = args[1];
        String relMinPmi = args[2];

        String stopWordsEngPath = "s3://collocation-extraction-bucket/stop-words/eng-stopwords.txt";
        String inputEngPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data";

        String stopWordsHebPath = "s3://collocation-extraction-bucket/stop-words/heb-stopwords.txt";
        String inputHebPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";

        String inputTest = "s3://collocation-extraction-bucket/inputs/english_bigrams.txt";

        initAWS();

        // Step 1
        HadoopJarStepConfig StepOne = new HadoopJarStepConfig()
                .withJar("s3://collocation-extraction-bucket/jars/StepOneTest.jar") // TODO: StepOne.jar
                .withArgs(stopWordsEngPath, inputTest, String.format("s3://collocation-extraction-bucket/outputs/%d/step-one", appId)) // TODO: inputEngPath
                .withMainClass("StepOne");
        StepConfig StepOneConfig = new StepConfig()
                .withName("StepOne")
                .withHadoopJarStep(StepOne)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 2
        HadoopJarStepConfig StepTwo = new HadoopJarStepConfig()
                .withJar("s3://collocation-extraction-bucket/jars/StepTwo.jar")
                .withArgs(String.format("s3://collocation-extraction-bucket/outputs/%d/step-one", appId), String.format("s3://collocation-extraction-bucket/outputs/%d/step-two", appId))
                .withMainClass("StepTwo");
        StepConfig StepTwoConfig = new StepConfig()
                .withName("StepTwo")
                .withHadoopJarStep(StepTwo)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 3
        HadoopJarStepConfig StepThree = new HadoopJarStepConfig()
                .withJar("s3://collocation-extraction-bucket/jars/StepThree.jar")
                .withArgs(String.format("s3://collocation-extraction-bucket/outputs/%d/step-two", appId), String.format("s3://collocation-extraction-bucket/outputs/%d/step-three", appId), minPmi, relMinPmi)
                .withMainClass("StepThree");
        StepConfig StepThreeConfig = new StepConfig()
                .withName("StepThree")
                .withHadoopJarStep(StepThree)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("CollocationExtraction")
                .withInstances(instances)
                .withSteps(StepOneConfig, StepTwoConfig, StepThreeConfig)
                .withLogUri("s3://collocation-extraction-bucket/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static void initAWS() {
        System.out.println("[INFO] Connecting to aws");
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        System.out.println( "list cluster");
        System.out.println( emr.listClusters());
    }

}
