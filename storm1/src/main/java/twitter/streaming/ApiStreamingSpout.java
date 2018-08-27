package twitter.streaming;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class ApiStreamingSpout extends BaseRichSpout {

    private final static String STREAMING_API_URL="https://stream.twitter.com/1/statuses/filter.json?track=";
    private String track;
    private String user;
    private String password;
    private DefaultHttpClient client;
    private SpoutOutputCollector collector;
    private UsernamePasswordCredentials credentials;
    private BasicCredentialsProvider credentialProvider;

    LinkedBlockingDeque<String> tweets = new LinkedBlockingDeque<String>();

    static Logger LOG = Logger.getLogger(ApiStreamingSpout.class);
    static JSONParser jsonParser = new JSONParser();


    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        int spoutsSize = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        int myIdx = topologyContext.getThisTaskId();
        String [] tracks = conf.get("track").toString().split(",");
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < tracks.length; i++) {
            if( i % spoutsSize == myIdx){
                stringBuffer.append(",");
                stringBuffer.append(tracks[i]);
            }
        }

        if(stringBuffer.length() == 0){
            throw new RuntimeException("No track found for spout" +
                    " [spoutsSize:"+spoutsSize+", tracks:"+tracks.length+"] the amount" +
                    " of tracks must be more then the spout paralellism");
        }

        this.track = stringBuffer.substring(1);

        user = (String) conf.get("user");
        password = (String) conf.get("password");

        credentials = new UsernamePasswordCredentials(user, password);
        credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(AuthScope.ANY, credentials);
        this.collector = spoutOutputCollector;

    }

    @Override
    public void nextTuple() {

        /*
         * Create the client call
         */
        client = new DefaultHttpClient();
        client.setCredentialsProvider(credentialProvider);
        HttpGet get = new HttpGet(STREAMING_API_URL+track);
        HttpResponse response;

        try {
            //Execute
            response = client.execute(get);
            StatusLine status = response.getStatusLine();
            if(status.getStatusCode() == 200){
                InputStream inputStream = response.getEntity().getContent();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String in;
                //Read line by line
                while((in = reader.readLine())!=null){
                    try{
                        //Parse and emit
                        Object json = jsonParser.parse(in);
                        collector.emit(new Values(track,json));
                    }catch (ParseException e) {
                        LOG.error("Error parsing message from twitter",e);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Error in communication with twitter api ["+get.getURI().toString()+"]");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e1) {
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("criteria","tweet"));
    }
}
