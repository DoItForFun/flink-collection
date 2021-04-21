package com.flyai.recommend.utils;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author lizhe
 */
public class HttpClient {
    public static String doGet(String url) {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet get = new HttpGet(url);
        try {
            HttpResponse response = client.execute(get);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String doPost(String url , Map <String , String> params) {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        try {
            List <NameValuePair> request = new ArrayList <>();
            for(Map.Entry<String, String> entry : params.entrySet()){
                request.add(new BasicNameValuePair(entry.getKey(),  entry.getValue()));
            }
            post.setEntity(new UrlEncodedFormEntity(request, HTTP.UTF_8));
            HttpResponse response = client.execute(post);
            String res = EntityUtils.toString(response.getEntity());

            return res;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
