package edu.njit.an395;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import java.util.Properties;

public class OozieCaller {
    // get a OozieClient for local Oozie
    OozieClient wc = new OozieClient("http://bar:11000/oozie");

    // create a workflow job configuration and set the workflow application path
    Properties conf = wc.createConfiguration();
    conf.setProperty(OozieClient.APP_PATH, "hdfs://foo:8020/home/ec2-user/airplane");

    // setting workflow parameters
    conf.setProperty("jobTracker", "foo:8021");
    conf.setProperty("inputDir", "/home/ec2-user/inputdir");
    conf.setProperty("outputDir", "/home/ec2-user/outputdir");
    
    // submit and start the workflow job
    String jobId = wc.run(conf);
    System.out.println("Workflow job submitted");

    // wait until the workflow job finishes printing the status every 10 seconds
    while (wc.getJobInfo(jobId).getStatus() == Workflow.Status.RUNNING) {
        System.out.println("Workflow job running ...");
        Thread.sleep(10 * 1000);
    }

    // print the final status of the workflow job
    System.out.println("Workflow job completed ...");
    System.out.println(wf.getJobInfo(jobId));
}
