import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TwitterKafkaProducer {
	
    public static void main( String[] args ) throws Exception {
    	String bearerToken = Constant.BEARER_TOKEN;
		if (bearerToken != null) {
			connectTwitterKafka(bearerToken);
		} else {
			System.out.println("Bearer Token issue. Please make sure the Bearer Token is valid.");
		}
    }
    
    private static void connectTwitterKafka(String bearerToken) throws Exception {
		HttpClient httpClient = HttpClients
				.custom()
				.setDefaultRequestConfig(RequestConfig.custom()
				.setCookieSpec(CookieSpecs.STANDARD).build())
				.build();
		URIBuilder uriBuilder = new URIBuilder(Constant.URL);
		HttpGet httpGet = new HttpGet(uriBuilder.build());
		httpGet.setHeader(Constant.HEADER, String.format("Bearer %s", bearerToken));
		HttpResponse response = httpClient.execute(httpGet);
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
			String line = reader.readLine();
			Gson gson = new Gson();
			while (line != null) {
				System.out.println(line);
				Twitter body = gson.fromJson(line, Twitter.class);
				if (body != null ){
					String id = body.getData().getId();
					String text = body.getData().getText();
					Sender.sendTweet(id, text);
				}
				line = reader.readLine();
			}
		}
	}
}