package com.serverless;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.model.S3Object;
import com.serverless.service.S3FileService;
import com.serverless.service.TsvParserService;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Handler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

  private static final Logger LOG = Logger.getLogger(Handler.class);

  @Override
  public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
    LOG.info("received: " + input);
    String fileName = (String) ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>[]) input
        .get("Records"))[0]
        .get("s3"))
        .get("object"))
        .get("key");
    try (S3Object file = S3FileService.getFile(fileName)) {
      if (Objects.nonNull(file)) {
        System.out.println(file.getBucketName());

        List<String[]> strings = TsvParserService.parseCards(file.getObjectContent());
        strings.forEach(Arrays::toString);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", input);
    return ApiGatewayResponse.builder()
        .setStatusCode(200)
        .setObjectBody(responseBody)
        .setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless"))
        .build();
  }
}
