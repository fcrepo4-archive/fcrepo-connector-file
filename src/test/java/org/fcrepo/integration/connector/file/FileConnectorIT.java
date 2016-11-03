/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.integration.connector.file;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.parseInt;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;
import static org.apache.jena.graph.Node.ANY;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.vocabulary.RDF.type;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.fcrepo.kernel.modeshape.RdfJcrLexicon.MODE_NAMESPACE;
import static org.fcrepo.http.commons.test.util.TestHelpers.parseTriples;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.http.HttpResponse;
import org.apache.http.annotation.NotThreadSafe;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.graph.Node;

import org.fcrepo.http.commons.test.util.CloseableDataset;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests around using the fcrepo-connector-file
 *
 * @author awoods
 * @author ajs6f
 */
public class FileConnectorIT {

    private static final Logger logger = LoggerFactory.getLogger(FileConnectorIT.class);

    private static final int SERVER_PORT = parseInt(System.getProperty("fcrepo.dynamic.test.port", "8080"));

    private static final String HOSTNAME = "localhost";

    private static final String PROTOCOL = "http";

    private static final String serverAddress = PROTOCOL + "://" + HOSTNAME + ":" + SERVER_PORT + "/fcrepo/rest/";

    private static CloseableHttpClient client = createClient();

    private static CloseableHttpClient createClient() {
        return HttpClientBuilder.create().setMaxConnPerRoute(MAX_VALUE).setMaxConnTotal(MAX_VALUE).build();
    }

    private static SimpleDateFormat headerFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

    /**
     * I should be able to link to content on a federated filesystem.
     *
     * @throws IOException thrown during this function
     **/
    @Test
    public void testFederatedDatastream() throws IOException {
        final String federationAddress = serverAddress + "files/FileSystem1/ds1";
        final String linkingAddress;
        try (final CloseableHttpResponse response = client.execute(new HttpPost(serverAddress))) {
            EntityUtils.consume(response.getEntity());
            linkingAddress = response.getFirstHeader("Location").getValue();
        }

        // link from the object to the content of the file on the federated filesystem
        final HttpPatch patch = new HttpPatch(linkingAddress);
        patch.addHeader("Content-Type", "application/sparql-update");
        patch.setEntity(new ByteArrayEntity(
                ("INSERT DATA { <> <http://some-vocabulary#hasExternalContent> " + "<" + federationAddress + "> . }")
                .getBytes()));
        assertEquals("Couldn't link to external datastream!", NO_CONTENT.getStatusCode(), getStatus(patch));
    }

    /**
     * Given a directory at: test-FileSystem1/ /ds1 /ds2 /TestSubdir/ and a projection of test-objects as
     * fedora:/files, then I should be able to retrieve an object from fedora:/files/FileSystem1 that lists a child
     * object at fedora:/files/FileSystem1/TestSubdir and lists datastreams ds and ds2
     *
     * @throws IOException thrown during this function
     */
    @Test
    public void testGetProjectedNode() throws IOException {
        final HttpGet method = new HttpGet(serverAddress + "files/FileSystem1");
        try (final CloseableDataset dataset = getDataset(client, method)) {
            final DatasetGraph graph = dataset.asDatasetGraph();
            final Node subjectURI = createURI(serverAddress + "files/FileSystem1");
            assertTrue("Didn't find the first datastream! ", graph.contains(ANY,
                    subjectURI, ANY, createURI(subjectURI + "/ds1")));
            assertTrue("Didn't find the second datastream! ", graph.contains(ANY,
                    subjectURI, ANY, createURI(subjectURI + "/ds2")));
            assertTrue("Didn't find the first object! ", graph.contains(ANY,
                    subjectURI, ANY, createURI(subjectURI + "/TestSubdir")));
        }
    }

    /**
     * When I make changes to a resource in a federated filesystem, the parent folder's Last-Modified header should be
     * updated.
     *
     * @throws IOException thrown during this function
     **/
    @Test
    public void testLastModifiedUpdatedAfterUpdates() throws IOException {
        // create directory containing a file in filesystem
        final File fed = new File("target/test-classes/test-objects");
        final String id = randomUUID().toString();
        final File dir = new File(fed, id);
        final File child = new File(dir, "child");
        final long timestamp1 = currentTimeMillis();
        dir.mkdir();
        child.mkdir();
        // TODO this seems really brittle
        try {
            sleep(2000);
        } catch (final InterruptedException e) {
        }

        // check Last-Modified header is current
        final long lastmod1;
        try (final CloseableHttpResponse resp1 = client.execute(new HttpHead(serverAddress + "files/" + id))) {
            assertEquals(OK.getStatusCode(), getStatus(resp1));
            lastmod1 = headerFormat.parse(resp1.getFirstHeader("Last-Modified").getValue()).getTime();
            assertTrue((timestamp1 - lastmod1) < 1000); // because rounding

            // remove the file and wait for the TTL to expire
            final long timestamp2 = currentTimeMillis();
            child.delete();
            try {
                sleep(2000);
            } catch (final InterruptedException e) {
            }

            // check Last-Modified header is updated
            try (final CloseableHttpResponse resp2 = client.execute(new HttpHead(serverAddress + "files/" + id))) {
                assertEquals(OK.getStatusCode(), getStatus(resp2));
                final long lastmod2 = headerFormat.parse(resp2.getFirstHeader("Last-Modified").getValue()).getTime();
                assertTrue((timestamp2 - lastmod2) < 1000); // because rounding
                assertFalse("Last-Modified headers should have changed", lastmod1 == lastmod2);
            } catch (final ParseException e) {
                fail();
            }
        } catch (final ParseException e) {
            fail();
        }
    }

    /**
     * I should be able to copy objects from a federated filesystem to the repository.
     **/
    @Test
    public void testCopyFromProjection() {
        final String destination = serverAddress + "copy-" + randomUUID().toString() + "-ds1";
        final String source = serverAddress + "files/FileSystem1/ds1";

        // ensure the source is present
        assertEquals(OK.getStatusCode(), getStatus(new HttpGet(source)));

        // copy to repository
        final HttpCopy request = new HttpCopy(source);
        request.addHeader("Destination", destination);
        assertEquals(CREATED.getStatusCode(), getStatus(request));

        // repository copy should now exist
        assertEquals(OK.getStatusCode(), getStatus(new HttpGet(destination)));
        assertEquals(OK.getStatusCode(), getStatus(new HttpGet(source)));
    }

    /**
     * I should be able to copy objects from the repository to a federated filesystem.
     *
     * @throws IOException exception thrown during this function
     **/
    @Ignore("Enabled once the FedoraFileSystemConnector becomes readable/writable")
    public void testCopyToProjection() throws IOException {
        // create object in the repository
        final String pid = randomUUID().toString();
        final HttpPut put = new HttpPut(serverAddress + pid + "/ds1");
        put.setEntity(new StringEntity("abc123"));
        put.setHeader("Content-Type", TEXT_PLAIN);
        client.execute(put);

        // copy to federated filesystem
        final HttpCopy request = new HttpCopy(serverAddress + pid);
        request.addHeader("Destination", serverAddress + "files/copy-" + pid);
        assertEquals(CREATED.getStatusCode(), getStatus(request));

        // federated copy should now exist
        final HttpGet copyGet = new HttpGet(serverAddress + "files/copy-" + pid);
        assertEquals(OK.getStatusCode(), getStatus(copyGet));

        // repository copy should still exist
        final HttpGet originalGet = new HttpGet(serverAddress + pid);
        assertEquals(OK.getStatusCode(), getStatus(originalGet));
    }

    @Test
    public void testGetRepositoryGraph() throws IOException {
        try (final CloseableDataset dataset = getDataset(client, new HttpGet(serverAddress))) {
            final DatasetGraph graph = dataset.asDatasetGraph();
            logger.trace("Retrieved repository graph:\n" + graph);
            assertFalse("Should not find the root type", graph.contains(ANY,
                    ANY, type.asNode(), createURI(MODE_NAMESPACE + "root")));
        }
    }

    /**
     * I should be able to create two subdirectories of a non-existent parent directory.
     *
     * @throws IOException thrown during this function
     **/
    @Ignore("Enabled once the FedoraFileSystemConnector becomes readable/writable")
    // TODO
    public
    void testBreakFederation() throws IOException {
        final String id = randomUUID().toString();
        testGetRepositoryGraph();
        createObjectAndClose("files/a0/" + id + "b0");
        createObjectAndClose("files/a0/" + id + "b1");
        testGetRepositoryGraph();
    }

    /**
     * I should be able to upload a file to a read/write federated filesystem.
     *
     * @throws IOException thrown during this function
     **/
    @Ignore("Enabled once the FedoraFileSystemConnector becomes readable/writable")
    // TODO
    public
    void testUploadToProjection() throws IOException {
        // upload file to federated filesystem using rest api
        final String id = randomUUID().toString();
        final String uploadLocation = serverAddress + "files/" + id + "/ds1";
        final String uploadContent = "abc123";
        logger.debug("Uploading to federated filesystem via rest api: " + uploadLocation);
        // final HttpResponse response = createDatastream("files/" + pid, "ds1", uploadContent);
        // final String actualLocation = response.getFirstHeader("Location").getValue();
        // assertEquals("Wrong URI in Location header", uploadLocation, actualLocation);

        // validate content
        try (final CloseableHttpResponse getResponse = client.execute(new HttpGet(uploadLocation))) {
            final String actualContent = EntityUtils.toString(getResponse.getEntity());
            assertEquals(OK.getStatusCode(), getResponse.getStatusLine().getStatusCode());
            assertEquals("Content doesn't match", actualContent, uploadContent);
        }
        // validate object profile
        try (final CloseableHttpResponse objResponse = client.execute(new HttpGet(serverAddress + "files/" + id))) {
            assertEquals(OK.getStatusCode(), objResponse.getStatusLine().getStatusCode());
        }
    }

    /* Many of the following private methods are copied directly from
     * org.fcrepo.integration.http.api.AbstractResourceIT.java. The reason we're not
     * simply inheriting from that class and re-using these methods has to do with the
     * complexities of building a working spring-based container along with the assumptions of
     * certain dependency chains. Basically, it didn't work, and this is a compromise
     * to simply make this integration test work. */

    /**
     * Executes an HTTP request and returns the status code of the response, closing the response.
     *
     * @param req the request to execute
     * @return the HTTP status code of the response
     */
    private static int getStatus(final HttpUriRequest req) {
        try (final CloseableHttpResponse response = client.execute(req)) {
            final int result = getStatus(response);
            if (!(result > 199) || !(result < 400)) {
                logger.warn("Got status {}", result);
                if (response.getEntity() != null) {
                    logger.trace(EntityUtils.toString(response.getEntity()));
                }
            }
            EntityUtils.consume(response.getEntity());
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int getStatus(final HttpResponse response) {
        return response.getStatusLine().getStatusCode();
    }

    private CloseableHttpResponse createObject(final String pid) {
        final HttpPost httpPost = new HttpPost(serverAddress + "/");
        if (pid.length() > 0) {
            httpPost.addHeader("Slug", pid);
        }
        try {
            final CloseableHttpResponse response = client.execute(httpPost);
            assertEquals(CREATED.getStatusCode(), getStatus(response));
            return response;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createObjectAndClose(final String pid) {
        try {
            createObject(pid).close();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CloseableDataset getDataset(final CloseableHttpClient client, final HttpUriRequest req)
            throws IOException {
        if (!req.containsHeader("Accept")) {
            req.addHeader("Accept", "application/n-triples");
        }
        logger.debug("Retrieving RDF using mimeType: {}", req.getFirstHeader("Accept"));

        try (final CloseableHttpResponse response = client.execute(req)) {
            assertEquals(OK.getStatusCode(), response.getStatusLine().getStatusCode());
            final CloseableDataset result = parseTriples(response.getEntity());
            logger.trace("Retrieved RDF: {}", result);
            return result;
        }
    }

    @NotThreadSafe // HttpRequestBase is @NotThreadSafe
    private class HttpCopy extends HttpRequestBase {

        /**
         * @throws IllegalArgumentException if the uri is invalid.
         */
        public HttpCopy(final String uri) {
            super();
            setURI(URI.create(uri));
        }

        @Override
        public String getMethod() {
            return "COPY";
        }
    }
}
