mvn clean package
docker build -t us.gcr.io/pharmacydeda/pharmacy-deda .
docker push us.gcr.io/pharmacydeda/pharmacy-deda