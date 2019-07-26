package com.serverless.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class S3FileService {

  private final static String BUCKET_NAME = System.getenv("BUCKET_NAME");

  public static S3Object getFile(String fileName) throws IOException {

    S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
    try {
      Regions region = Regions.fromName("us-east-1");

      AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
          .withRegion(region)
          .withCredentials(new ProfileCredentialsProvider())
          .build();

      // Get an object and print its contents.
      System.out.println("Downloading an object");
      fullObject = s3Client.getObject(new GetObjectRequest(BUCKET_NAME, fileName));
      System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
      System.out.println("Content: ");
      displayTextInputStream(fullObject.getObjectContent());

      return fullObject;
     /* // Get a range of bytes from an object and print the bytes.
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(BUCKET_NAME, fileName)
          .withRange(0,9);
      objectPortion = s3Client.getObject(rangeObjectRequest);
      System.out.println("Printing bytes retrieved.");
      displayTextInputStream(objectPortion.getObjectContent());

      // Get an entire object, overriding the specified response headers, and print the object's content.
      ResponseHeaderOverrides headerOverrides = new ResponseHeaderOverrides()
          .withCacheControl("No-cache")
          .withContentDisposition("attachment; filename=example.txt");
      GetObjectRequest getObjectRequestHeaderOverride = new GetObjectRequest(BUCKET_NAME, fileName)
          .withResponseHeaders(headerOverrides);
      headerOverrideObject = s3Client.getObject(getObjectRequestHeaderOverride);
      displayTextInputStream(headerOverrideObject.getObjectContent());*/
    }
    catch(AmazonServiceException e) {
      // The call was transmitted successfully, but Amazon S3 couldn't process
      // it, so it returned an error response.
      e.printStackTrace();
    }
    catch(SdkClientException e) {
      // Amazon S3 couldn't be contacted for a response, or the client
      // couldn't parse the response from Amazon S3.
      e.printStackTrace();
    }
    finally {
      // To ensure that the network connection doesn't remain open, close any open input streams.
      if(fullObject != null) {
        fullObject.close();
      }
      if(objectPortion != null) {
        objectPortion.close();
      }
      if(headerOverrideObject != null) {
        headerOverrideObject.close();
      }
    }
    return null;
  }

  private static void displayTextInputStream(InputStream input) throws IOException {
    // Read the text input stream one line at a time and display each line.
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    String line;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
    }
    System.out.println();
  }
}
