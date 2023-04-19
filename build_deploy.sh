mvn clean package
docker build -t us.gcr.io/pharmacydeda/pharmacy-deda .
docker push us.gcr.io/pharmacydeda/pharmacy-deda

    steps:
        - name: Checkout
          uses: actions/checkout@v1

        # Setup gcloud CLI
        - uses: GoogleCloudPlatform/github-actions/setup-gcloud@master
          with:
              version: '275.0.0'
              service_account_email: ${{ secrets.SA_EMAIL }}
              service_account_key: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS}}

            # Configure gcloud CLI
            - name: gcloud Set up
              run: |
                  gcloud config set project $PROJECT_ID
            # Build and push image to Google Container Registry
            - name: Build
              run: |
                  gcloud builds submit -t gcr.io/$PROJECT_ID/$SERVICE_NAME:latest
            # Deploy image to Cloud Run
            - name: Deploy
              run: |
                  gcloud dataflow flex-template build gs://pharamacy-deda/template/run_template_new.json --image gcr.io/$PROJECT_ID/$SERVICE_NAME:latest --sdk-language JAVA
