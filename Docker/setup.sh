docker image build -f Dockerfile.authentication . -t authentication_test:latest
docker image build -f Dockerfile.authorization . -t authorization_test:latest
docker image build -f Dockerfile.content . -t content_test:latest
docker-compose up
