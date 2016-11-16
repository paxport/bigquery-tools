package com.paxport.bigquery;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;

/**
 * Bootstrap our connection to BigQuery by either using the default credential
 * setup for running under app engine (or locally via standard path/sys prop) or
 * a custom credential by providing serviceAccountId and p12 file path
 *
 * You should probably set the bigquery.applicationName
 */
@Component
public class BigQueryFactory implements FactoryBean<Bigquery>, InitializingBean {

    private final Collection<String> SCOPES = scopes();
    private final HttpTransport TRANSPORT = httpTransport();
    private final JsonFactory JSON_FACTORY = jsonFactory();

    @Value("${bigquery.serviceAccountId:''}")
    private String serviceAccountId = "";

    @Value("${bigquery.p12path:''}")
    private String p12path;

    @Value("${bigquery.applicationName:'Cloud Audit'}")
    private String applicationName = "Bigquery Tools";

    private Bigquery instance;

    public Bigquery getBigquery() {
        if (instance == null) {
            instance = buildInstance();
        }
        return instance;
    }

    protected HttpTransport httpTransport(){
        return new NetHttpTransport();
    }

    protected JsonFactory jsonFactory(){
        return new JacksonFactory();
    }

    protected Collection<String> scopes(){
        return BigqueryScopes.all();
    }

    private Bigquery buildInstance() {
        return new Bigquery.Builder(httpTransport(),jsonFactory(),requestInitializer())
                .setApplicationName(applicationName)
                .build();
    }

    protected HttpRequestInitializer requestInitializer() {
        if ( shouldUseDefaultCredential() ){
            return defaultCredential();
        }
        else {
            return customCredential();
        }
    }

    protected boolean shouldUseDefaultCredential() {
        return serviceAccountId.trim().equals("") || !p12path.trim().equals("");
    }

    protected HttpRequestInitializer defaultCredential() {

        try {
            GoogleCredential credential = GoogleCredential.getApplicationDefault(TRANSPORT, JSON_FACTORY);
            // Depending on the environment that provides the default credentials (e.g. Compute Engine, App
            // Engine), the credentials may require us to specify the scopes we need explicitly.
            // Check for this case, and inject the Bigquery scope if required.
            if (credential.createScopedRequired()) {
                credential = credential.createScoped(SCOPES);
            }
            return credential;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create default Google Credential", e);
        }
    }

    protected HttpRequestInitializer customCredential() {
        File p12File = new File(p12path);
        if (!p12File.exists()) {
            throw new RuntimeException("Failed to find bigquery p12 cert file at: " + p12File.getAbsolutePath());
        }
        try {
            GoogleCredential cred = new GoogleCredential.Builder()
                    .setTransport(TRANSPORT)
                    .setJsonFactory(JSON_FACTORY)
                    .setServiceAccountId(serviceAccountId)
                    .setServiceAccountPrivateKeyFromP12File(p12File)
                    .setServiceAccountScopes(SCOPES)
                    .build();

            return cred;
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Failed to create custom Google Credential", e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create custom Google Credential",e);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        getBigquery();
    }

    public String getServiceAccountId() {
        return serviceAccountId;
    }

    public BigQueryFactory setServiceAccountId(String serviceAccountId) {
        this.serviceAccountId = serviceAccountId;
        return this;
    }

    public String getP12path() {
        return p12path;
    }

    public BigQueryFactory setP12path(String p12path) {
        this.p12path = p12path;
        return this;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public BigQueryFactory setApplicationName(String applicationName) {
        this.applicationName = applicationName;
        return this;
    }

    @Override
    public Bigquery getObject() throws Exception {
        return getBigquery();
    }

    @Override
    public Class<?> getObjectType() {
        return Bigquery.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
