package com.getindata.connectors.http.app;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpStubApp {

  private static final String URL = "/client";

  private static WireMockServer wireMockServer;

  public static void main(String[] args) {
    wireMockServer =
        new WireMockServer(
            WireMockConfiguration.wireMockConfig().port(8080).extensions(JsonTransform.class));
    wireMockServer.start();

    wireMockServer.addStubMapping(setupServerStub(200));
  }

  private static StubMapping setupServerStub(int status) {
    return wireMockServer.stubFor(
        get(urlPathEqualTo(URL))
            .willReturn(
                aResponse()
                    .withTransformers(JsonTransform.NAME)));
  }
}
