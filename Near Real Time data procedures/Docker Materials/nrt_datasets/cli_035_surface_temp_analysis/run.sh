#Change the NAME variable with the name of your script
NAME=cli_035_most_recent

docker build -t $NAME --build-arg NAME=$NAME .
docker run -v $(pwd)/data:/opt/$NAME/data --name $NAME --env-file .env --rm $NAME python main.py