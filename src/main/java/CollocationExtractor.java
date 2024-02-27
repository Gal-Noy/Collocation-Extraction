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

public class CollocationExtractor {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;
    public static int numberOfInstances = 1;

    public static void main(String[]args){
        if (args.length != 2) {
            System.out.println("Usage: CollocationExtractor <minPmi> <relMinPmi>");
            System.exit(1);
        }

        double minPmi = Double.parseDouble(args[0]);
        double relMinPmi = Double.parseDouble(args[1]);

        credentialsProvider = new ProfileCredentialsProvider();
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
//        System.out.println( "list cluster");
//        System.out.println( emr.listClusters());

        // Step 1
        HadoopJarStepConfig StepOne = new HadoopJarStepConfig()
                .withJar("s3://collocation-extraction-bucket/jars/StepOne.jar")
                .withMainClass("StepOne");

        StepConfig StepOneConfig = new StepConfig()
                .withName("StepOne")
                .withHadoopJarStep(StepOne)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

//        // Step 2
//        HadoopJarStepConfig StepTwo = new HadoopJarStepConfig()
//                .withJar("s3://collocation-extraction-bucket/jars/StepTwo.jar")
//                .withMainClass("StepTwo");
//
//        StepConfig StepTwoConfig = new StepConfig()
//                .withName("StepTwo")
//                .withHadoopJarStep(StepTwo)
//                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Job flow
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
                .withSteps(StepOneConfig)
                .withLogUri("s3://collocation-extraction-bucket/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
